package actor

import (
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// replicaHandler 是懒激活副本的占位 Handler：
// 副本节点不承担实际 spawn（由持有完整 Props 的服务节点经 SpawnRemote 拉起），
// 仅使发送方的懒激活判定（canLazySpawn 需要本地 Props 副本）生效。
type replicaHandler struct{}

func (replicaHandler) OnStart(core.ActorContext) error                  { return nil }
func (replicaHandler) OnStop(core.ActorContext, types.ExitReason) error { return nil }
func (replicaHandler) OnTell(core.ActorContext, any) error              { return nil }
func (replicaHandler) OnAsk(core.ActorContext, any) (any, error)        { return nil, nil }

// RegisterAutoReplica 在运行时注册某 actor 类型的懒激活副本
// （「只发不接」的节点使用；实际拉起由持有完整 Props 的服务节点执行）。
func RegisterAutoReplica(r *Runtime, actorType string) error {
	return r.Register(core.Props{
		Type:       actorType,
		NewHandler: func(types.PID) core.Handler { return replicaHandler{} },
		SpawnMode:  core.SpawnAuto,
	})
}

// RegisterPlayerReplica 注册 PlayerActor 类型懒激活副本（gateway 使用；实际拉起在 game 节点）。
func RegisterPlayerReplica(r *Runtime) error {
	return RegisterAutoReplica(r, consts.ActorTypePlayer)
}

// RegisterBattleReplica 注册战斗 actor 类型懒激活副本（matcher 开局使用；实际拉起在 battle 节点）。
func RegisterBattleReplica(r *Runtime) error {
	return RegisterAutoReplica(r, consts.ActorTypeBattle)
}
