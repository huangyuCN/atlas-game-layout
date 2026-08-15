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

// RegisterPlayerReplica 在运行时注册 PlayerActor 类型的懒激活副本
// （gateway 等「只发不接」的节点使用；实际拉起在 game 节点）。
func RegisterPlayerReplica(r *Runtime) error {
	return r.Register(core.Props{
		Type:       consts.ActorTypePlayer,
		NewHandler: func(types.PID) core.Handler { return replicaHandler{} },
		SpawnMode:  core.SpawnAuto,
	})
}
