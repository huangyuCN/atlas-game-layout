// Package biz 定义 battle 服务的业务接口（根包只放接口，
// 实现位于 infra/（结算事件）与 actor/（BattleActor））。
package biz

import (
	"context"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
)

// BattlePusher 是战斗域通知的**直连推送**端口（规格 §1.1）：帧广播与战斗结束经帧引擎
// 推送原语（Push/PushRaw/PushTo）直发客户端，不再绕网关（阶段 3 批次 5 已删 NATS 路）。
// 未登记直连的玩家即跳过（不报错）——玩家可能在帧面尚未登记连接时结算。
type BattlePusher interface {
	// PublishFrame 向指定玩家的直连连接下发一帧（信封 FrameBroadcast）。
	PublishFrame(ctx context.Context, playerID, battleID string, frame *locksteppb.LockstepFrame) error
	// PublishEnd 向指定玩家的直连连接下发战斗结束通知（含胜者）。
	PublishEnd(ctx context.Context, playerID, battleID, winner string) error
	// PublishOut 向指定玩家的直连连接下发出局通知（掉线判负，规格 §9.3）。
	PublishOut(ctx context.Context, playerID, battleID, outPlayerID string, reason battlev1.PlayerOutReason) error
	// CloseBattle 关闭并移除本局全部直连（结算后回收，规格 §9.8）。
	CloseBattle(battleID string)
}

// ConnPresence 是直连在场性查询端口（规格 §9.2 硬约束②）：应用掉线事件前复核注册表，
// 玩家仍有存活连接即忽略这条过期事件（数据报面同一对端键上的新旧事件会交错）。
type ConnPresence interface {
	// Online 返回该玩家当前是否有存活直连（false = 确实不在场）。
	Online(playerID string) bool
}

// SettlePublisher 是战斗结算事件发布接口（nats，game 订阅回写玩家数据）。
type SettlePublisher interface {
	// PublishSettled 发布结算事件。
	PublishSettled(ctx context.Context, ev *battlev1.BattleSettledEvent) error
}
