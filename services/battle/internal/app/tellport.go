// 连接生命周期消息的投递端口（规格 §9.1）：帧面监听在本节点、注册表里只有本节点的连接，
// 故生命周期事件只对本机战斗实例有意义——端口据此本机投递，不做跨节点寻址。
//
// 为什么要单独一个端口而不是直接 rt.Tell：生命周期消息是**本地 Go 消息**
// （biz.PlayerOnline / biz.PlayerOffline，actor 侧以 core.WithLocalTell 注册类型路由），
// 而集群投递路径只认 wire 载荷（[]byte / proto.Message）。本机没有实例时 Tell 会落到
// 集群路径，本地对象在那里必然被丢弃（unsupported actor payload type），还会顺带把
// 已结算自停的实例重新懒激活——投递端口把这条路径收口在本机。

package app

import (
	"context"

	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// lifecyclePort 是连接生命周期消息到战斗 actor 的本机投递端口（满足 stream.TellFunc）。
type lifecyclePort struct {
	// local 是本节点运行时（只投本机实例，不触发跨节点寻址）。
	local *core.LocalRuntime
}

// newLifecyclePort 以本节点运行时构造生命周期投递端口。
func newLifecyclePort(local *core.LocalRuntime) lifecyclePort {
	return lifecyclePort{local: local}
}

// Tell 实现 stream.TellFunc：本机有该实例即经本地类型路由入队（非阻塞），否则静默丢弃。
//
// 丢弃的语义：本机没有实例说明这份直连事件已无对应 actor——要么实例还没拉起（上线事件由
// 之后的投递/登记自然覆盖），要么已结算自停（掉线计时不再有意义）。此时若照常 Tell，
// 集群路径会按 SpawnAuto 懒激活**重建**刚结算完的实例（空名单重复结算 + 误关本局直连），
// 再在 wire 编码处丢掉本地对象——正是批次 8 损伤验收里的 unsupported actor payload type。
func (p lifecyclePort) Tell(battleID string, msg any) error {
	pid, err := types.NewPID(battlev1actor.BattleServiceActorType, battleID)
	if err != nil {
		return err
	}
	if !p.present(pid) {
		return nil
	}
	if err := p.local.Tell(context.Background(), pid, msg); err != nil {
		if !p.present(pid) {
			return nil // 存在性检查后实例已停：事件无对应 actor，不把竞态报成投递失败
		}
		return err
	}
	return nil
}

// present 返回本机是否存有该实例：只读存在性判定，不触发懒激活、不做跨节点寻址。
func (p lifecyclePort) present(pid types.PID) bool {
	_, ok := p.local.Stats(pid)
	return ok
}
