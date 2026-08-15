// Package actor 提供 game 服务的 actor 装配：PlayerActor（集群懒激活）承载
// 玩家在线态：内存聚合根（cow 写）+ 定时 redis 快照 + 下线 mongo 落库（D9/D10）。
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
	"google.golang.org/protobuf/proto"
)

// tickSnapshot 是定时快照自消息（OnStart 注册 Repeat）。
type tickSnapshot struct{}

// NewProps 构造 PlayerActor 注册规格（SpawnAuto 懒激活 + 默认中间件链）。
// snapTTL 是快照缓存租期；snapTick 是定时快照周期（≤0 关闭）。
func NewProps(svc biz.PlayerService, store repo.PlayerRepo, snapTTL, snapTick time.Duration) core.Props {
	return core.Props{
		Type: consts.ActorTypePlayer,
		NewHandler: func(pid types.PID) core.Handler {
			return &PlayerActor{pid: pid, svc: svc, store: store, snapTTL: snapTTL, snapTick: snapTick}
		},
		SpawnMode: core.SpawnAuto,
		Tell:      pkgactor.DefaultTellChain(),
		Ask:       pkgactor.DefaultAskChain(),
	}
}

// PlayerActor 是玩家在线态 actor：同一玩家全局唯一实例（Locator 注册 player:<id>）。
// 跨节点消息约定：payload 为序列化字节，经 PlayerActorMsg 信封还原。
type PlayerActor struct {
	pid      types.PID
	cctx     core.ActorContext
	svc      biz.PlayerService
	store    repo.PlayerRepo
	snapTTL  time.Duration
	snapTick time.Duration

	player *models.Player // 内存聚合根（在线缓存；登录后持有）
}

// OnStart 实现 core.Handler：注册定时快照。
func (p *PlayerActor) OnStart(ctx core.ActorContext) error {
	p.cctx = ctx
	p.pid = ctx.Self()
	if p.snapTick > 0 {
		ctx.Repeat(p.snapTick, p.snapTick, tickSnapshot{})
	}
	return nil
}

// OnStop 实现 core.Handler：下线 mongo 落库（若持有聚合根）。
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

// OnTell 实现 core.Handler：登出退出消息与定时快照自消息。
func (p *PlayerActor) OnTell(ctx core.ActorContext, msg any) error {
	if _, ok := msg.(tickSnapshot); ok {
		return p.saveSnapshot(ctx)
	}
	env, err := decodeEnvelope(msg)
	if err != nil {
		return err
	}
	logout := env.GetLogout()
	if logout == nil {
		return nil
	}
	_ = p.svc.Logout(ctx.Context(), p.pid.UID(), logout.GetToken())
	// 停止自身：Locator 移除（集群目录归属释放），OnStop 落库。
	ctx.Stop(types.ExitNormal())
	return nil
}

// OnAsk 实现 core.Handler：分发注册/登录/背包/查询请求。
// 业务错误经回执 ok=false + error_reason 返回（不进入 error 通道，
// 避免跨节点传输把业务错误当投递故障重试）。
func (p *PlayerActor) OnAsk(ctx core.ActorContext, req any) (any, error) {
	env, err := decodeEnvelope(req)
	if err != nil {
		return nil, err
	}
	switch k := env.GetKind().(type) {
	case *gamev1.PlayerActorMsg_Register:
		reply, berr := p.svc.Register(ctx.Context(), k.Register)
		if berr != nil {
			return &gamev1.RegisterActorReply{Ok: false, ErrorReason: usecase.ReasonOf(berr)}, nil
		}
		// 注册是两段式：建号回执后即自停，登录按 player_id 另起实例。
		ctx.Stop(types.ExitNormal())
		return reply, nil
	case *gamev1.PlayerActorMsg_Login:
		return p.onLogin(ctx, k.Login)
	case *gamev1.PlayerActorMsg_GrantItem:
		return p.onGrantItem(k.GrantItem)
	case *gamev1.PlayerActorMsg_GetBackpack:
		return p.onGetBackpack()
	case *gamev1.PlayerActorMsg_GetPlayer:
		return p.onGetPlayer()
	case *gamev1.PlayerActorMsg_Logout:
		_ = p.svc.Logout(ctx.Context(), p.pid.UID(), k.Logout.GetToken())
		ctx.Stop(types.ExitNormal())
		return &gamev1.LoginActorReply{Ok: true}, nil
	default:
		return nil, errorv1.ErrInvalidParams("未知玩家 actor 消息")
	}
}

// onLogin 登录：业务裁决成功后加载聚合根到内存（redis→mongo 三级链路）。
func (p *PlayerActor) onLogin(ctx core.ActorContext, req *gamev1.LoginActorReq) (any, error) {
	reply, berr := p.svc.Login(ctx.Context(), req)
	if berr != nil {
		return &gamev1.LoginActorReply{Ok: false, ErrorReason: usecase.ReasonOf(berr)}, nil
	}
	player, err := p.store.LoadPlayer(ctx.Context(), req.GetPlayerId())
	if err != nil {
		return &gamev1.LoginActorReply{Ok: false, ErrorReason: usecase.ReasonOf(errorv1.ErrInternal("加载玩家失败"))}, nil
	}
	p.player = player
	return reply, nil
}

// onGrantItem 发放道具：聚合根 undo 写（未登录拒绝）。
func (p *PlayerActor) onGrantItem(req *gamev1.GrantItemActorReq) (any, error) {
	if p.player == nil {
		return &gamev1.GrantItemActorReply{Ok: false, ErrorReason: errorv1.ReasonPlayerNotOnline()}, nil
	}
	p.player.GrantItem(req.GetItemId(), req.GetCount())
	return &gamev1.GrantItemActorReply{Ok: true}, nil
}

// onGetBackpack 背包查询（聚合根内存快照）。
func (p *PlayerActor) onGetBackpack() (any, error) {
	if p.player == nil {
		return &gamev1.GetBackpackActorReply{Ok: false}, nil
	}
	return &gamev1.GetBackpackActorReply{Ok: true, Items: usecase.BackpackItems(p.player)}, nil
}

// onGetPlayer 玩家摘要查询（聚合根内存快照）。
func (p *PlayerActor) onGetPlayer() (any, error) {
	if p.player == nil {
		return &gamev1.GetPlayerActorReply{Ok: false}, nil
	}
	return &gamev1.GetPlayerActorReply{Ok: true, Player: usecase.PlayerSummary(p.player)}, nil
}

// saveSnapshot 定时 redis 快照（在线缓存刷盘）。
func (p *PlayerActor) saveSnapshot(ctx core.ActorContext) error {
	if p.player == nil || p.snapTTL <= 0 {
		return nil
	}
	return p.store.SaveSnapshot(ctx.Context(), p.player, p.snapTTL)
}

// decodeEnvelope 还原消息信封：跨节点 payload 为序列化字节，同节点为对象直传。
func decodeEnvelope(req any) (*gamev1.PlayerActorMsg, error) {
	switch v := req.(type) {
	case *gamev1.PlayerActorMsg:
		return v, nil
	case []byte:
		env := new(gamev1.PlayerActorMsg)
		if err := proto.Unmarshal(v, env); err != nil {
			return nil, fmt.Errorf("actor: 玩家消息解码失败: %w", err)
		}
		return env, nil
	default:
		return nil, fmt.Errorf("actor: 不支持的玩家消息类型 %T", req)
	}
}
