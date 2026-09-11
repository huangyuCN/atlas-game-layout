// Package actor 提供 game 服务的 actor 装配：PlayerActor（集群懒激活）承载
// 玩家在线态：内存聚合根（cow 写）+ 定时 redis 快照 + 下线 mongo 落库（D9/D10）。
//
// 分发由 protoc-gen-atlas-actor 生成的桩接管（gamev1.NewPlayerActorServer）：
// 业务 actor 只实现 PlayerActorServer 业务接口与可选生命周期（OnStart/OnStop）；
// 本地消息（tickSnapshot）经 WithLocalTell 类型路由注册，无 switch 分发。
package actor

import (
	"fmt"
	"time"

	atlaslog "github.com/huangyuCN/atlas/log"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// tickSnapshot 是定时快照自消息（OnStart 注册 Repeat，经本地类型路由分发）。
type tickSnapshot struct{}

// NewProps 构造 PlayerActor 注册规格（SpawnAuto 懒激活 + 默认中间件链）。
// snapTTL 是快照缓存租期；snapTick 是定时快照周期（≤0 关闭）；
// match 是匹配队列客户端（nil 降级：匹配方法返回内部错误，单测可注入 fake）。
func NewProps(svc biz.PlayerService, store repo.PlayerRepo, match biz.MatchQueueClient, snapTTL, snapTick time.Duration) core.Props {
	return core.Props{
		Type: consts.ActorTypePlayer,
		NewHandler: func(pid types.PID) core.Handler {
			p := &PlayerActor{
				pid: pid, svc: svc, store: store, match: match, snapTTL: snapTTL, snapTick: snapTick,
			}
			return gamev1.NewPlayerActorServer(p, core.WithLocalTell(p.onTickSnapshot))
		},
		SpawnMode:     core.SpawnAuto,
		Tell:          pkgactor.DefaultTellChain(),
		Ask:           pkgactor.DefaultAskChain(),
		DecodeInbound: gamev1.NewPlayerActorDecodeInbound(),
	}
}

// PlayerActor 是玩家在线态 actor：同一玩家全局唯一实例（Locator 注册 player:<id>）。
// 实现 gamev1.PlayerActorServer 业务接口；OnStart/OnStop 为可选生命周期接口
// （生成桩断言转发）；OnAsk/OnTell 分发由生成桩全权接管。
type PlayerActor struct {
	gamev1.UnimplementedPlayerActorServer // 兜底：service 加新 rpc 未实现也能编译
	pid                                   types.PID
	svc                                   biz.PlayerService
	store                                 repo.PlayerRepo
	match                                 biz.MatchQueueClient
	snapTTL                               time.Duration
	snapTick                              time.Duration

	player *models.Player // 内存聚合根（在线缓存；登录后持有）
}

// OnStart 实现 core.Handler 生命周期：注册定时快照 Repeat。
func (p *PlayerActor) OnStart(ctx core.ActorContext) error {
	p.pid = ctx.Self()
	if p.snapTick > 0 {
		ctx.Repeat(p.snapTick, p.snapTick, tickSnapshot{})
	}
	return nil
}

// OnStop 实现 core.Handler 生命周期：先联动取消在队匹配（幂等、失败仅忽略，
// 不阻断下线），再做下线 mongo 落库（若持有聚合根）。
func (p *PlayerActor) OnStop(ctx core.ActorContext, reason types.ExitReason) error {
	_ = reason
	if p.match != nil {
		_, _ = p.match.Cancel(ctx.Context(), p.pid.UID())
	}
	if p.player != nil {
		if err := p.store.SavePlayer(ctx.Context(), p.player); err != nil {
			return fmt.Errorf("actor: 下线落库失败: %w", err)
		}
		p.player = nil
	}
	return nil
}

// ---- 消息前置钩子（示例，可整体删除）----
//
// OnBeforeTell/OnBeforeAsk 实现 core.BeforeTellHook/BeforeAskHook 可选接口：
// proto 消息与本地消息在分发到具体业务方法之前都会经过这里，适合打印、埋点、
// 鉴权、限流等前置操作。返回 nil 继续正常分发；返回非 nil error 中断本次处理
// （该错误即本次投递/Ask 的结果，业务错误经集群 error 通道回传调用方）。
// 不实现这两个方法时生成桩自动跳过，零成本。

// OnBeforeTell Tell 消息分发前置钩子（示例）：打印消息类型。
func (p *PlayerActor) OnBeforeTell(ctx core.ActorContext, msg any) error {
	atlaslog.Debugf("player actor tell 前置: uid=%s msg=%T", ctx.Self().UID(), msg)
	return nil
}

// OnBeforeAsk Ask 请求分发前置钩子（示例）：打印请求类型。
func (p *PlayerActor) OnBeforeAsk(ctx core.ActorContext, req any) error {
	atlaslog.Debugf("player actor ask 前置: uid=%s req=%T", ctx.Self().UID(), req)
	return nil
}

// onTickSnapshot 定时 redis 快照（在线缓存刷盘，本地路由注册项）。
func (p *PlayerActor) onTickSnapshot(ctx core.ActorContext, _ tickSnapshot) error {
	return p.saveSnapshot(ctx)
}

// saveSnapshot 定时 redis 快照（在线缓存刷盘）。
func (p *PlayerActor) saveSnapshot(ctx core.ActorContext) error {
	if p.player == nil || p.snapTTL <= 0 {
		return nil
	}
	return p.store.SaveSnapshot(ctx.Context(), p.player, p.snapTTL)
}
