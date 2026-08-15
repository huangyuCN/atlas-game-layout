// Package biz 定义 battle 服务的业务接口（根包只放接口，
// 实现位于 infra/（nats 下发与结算事件）与 actor/（BattleActor））。
package biz

import (
	"context"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
)

// BattleNotifier 是战斗下行通知接口：
// 战斗帧与结束通知经 nats（atlas.push.<playerID> 信封）由 gateway 转达客户端。
type BattleNotifier interface {
	// PublishFrame 向指定玩家下发一帧（信封 FrameBroadcast）。
	PublishFrame(ctx context.Context, playerID, battleID string, frame *locksteppb.LockstepFrame) error
	// PublishEnd 向指定玩家下发战斗结束通知（信封 BattleEndNotify，含胜者）。
	PublishEnd(ctx context.Context, playerID, battleID, winner string) error
}

// SettlePublisher 是战斗结算事件发布接口（nats，game 订阅回写玩家数据）。
type SettlePublisher interface {
	// PublishSettled 发布结算事件。
	PublishSettled(ctx context.Context, ev *battlev1.BattleSettledEvent) error
}
