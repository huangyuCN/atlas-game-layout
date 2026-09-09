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

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/usecase"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// tickSnapshot 是定时快照自消息（OnStart 注册 Repeat，经本地类型路由分发）。
type tickSnapshot struct{}

// NewProps 构造 PlayerActor 注册规格（SpawnAuto 懒激活 + 默认中间件链）。
// snapTTL 是快照缓存租期；snapTick 是定时快照周期（≤0 关闭）。
func NewProps(svc biz.PlayerService, store repo.PlayerRepo, snapTTL, snapTick time.Duration) core.Props {
	return core.Props{
		Type: consts.ActorTypePlayer,
		NewHandler: func(pid types.PID) core.Handler {
			p := &PlayerActor{
				pid: pid, svc: svc, store: store, snapTTL: snapTTL, snapTick: snapTick,
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
	pid      types.PID
	svc      biz.PlayerService
	store    repo.PlayerRepo
	snapTTL  time.Duration
	snapTick time.Duration

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

// OnStop 实现 core.Handler 生命周期：下线 mongo 落库（若持有聚合根）。
func (p *PlayerActor) OnStop(ctx core.ActorContext, reason types.ExitReason) error {
	_ = reason
	if p.player != nil {
		if err := p.store.SavePlayer(ctx.Context(), p.player); err != nil {
			return fmt.Errorf("actor: 下线落库失败: %w", err)
		}
		p.player = nil
	}
	return nil
}

// Register 实现 gamev1.PlayerActorServer（两段式注册，D8）：
// 玩家数据不存在则创建并回执，客户端仍需 Login 建立会话；回执后自停。
func (p *PlayerActor) Register(ctx core.ActorContext, req *gamev1.RegisterActorReq) (*gamev1.RegisterActorReply, error) {
	reply, berr := p.svc.Register(ctx.Context(), req)
	if berr != nil {
		return nil, berr // 错误上抛：集群 error 通道往返（code/reason 保留）
	}
	// 注册是两段式：建号回执后即自停，登录按 player_id 另起实例。
	ctx.Stop(types.ExitNormal())
	return reply, nil
}

// Login 实现 gamev1.PlayerActorServer（D10 会话令牌裁决）：业务裁决成功后
// 加载聚合根到内存（redis→mongo 三级链路）。
func (p *PlayerActor) Login(ctx core.ActorContext, req *gamev1.LoginActorReq) (*gamev1.LoginActorReply, error) {
	reply, berr := p.svc.Login(ctx.Context(), req)
	if berr != nil {
		return nil, berr
	}
	player, err := p.store.LoadPlayer(ctx.Context(), req.GetPlayerId())
	if err != nil {
		return nil, errorv1.ErrInternal("加载玩家失败")
	}
	p.player = player
	return reply, nil
}

// Logout 实现 gamev1.PlayerActorServer（Tell，单向）：令牌校验后停止自身
// （Locator 移除，集群目录归属释放，OnStop 落库）。
func (p *PlayerActor) Logout(ctx core.ActorContext, msg *gamev1.LogoutActorMsg) error {
	_ = p.svc.Logout(ctx.Context(), p.pid.UID(), msg.GetToken())
	ctx.Stop(types.ExitNormal())
	return nil
}

// GrantItem 实现 gamev1.PlayerActorServer：聚合根 undo 写（未登录拒绝）。
func (p *PlayerActor) GrantItem(_ core.ActorContext, req *gamev1.GrantItemActorReq) (*gamev1.GrantItemActorReply, error) {
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("发放道具失败：玩家不在线")
	}
	p.player.GrantItem(req.GetItemId(), req.GetCount())
	return &gamev1.GrantItemActorReply{}, nil
}

// GetBackpack 实现 gamev1.PlayerActorServer：背包查询（聚合根内存快照）。
func (p *PlayerActor) GetBackpack(_ core.ActorContext, _ *gamev1.GetBackpackActorReq) (*gamev1.GetBackpackActorReply, error) {
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("查询背包失败：玩家不在线")
	}
	return &gamev1.GetBackpackActorReply{Items: usecase.BackpackItems(p.player)}, nil
}

// GetPlayer 实现 gamev1.PlayerActorServer：玩家摘要查询（聚合根内存快照）。
func (p *PlayerActor) GetPlayer(_ core.ActorContext, _ *gamev1.GetPlayerActorReq) (*gamev1.GetPlayerActorReply, error) {
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("查询玩家失败：玩家不在线")
	}
	return &gamev1.GetPlayerActorReply{Player: usecase.PlayerSummary(p.player)}, nil
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
