// Package infra 提供 battle 服务的外部依赖装配：
// mongo（结算落库）、nats（帧广播 + 结算事件）、actor 集群运行时。
package infra

import (
	"context"
	"fmt"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	natsgo "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
)

// settledTopicSuffix 是结算事件主题后缀（atlas.event.battle.settled）。
const settledTopicSuffix = "battle.settled"

// NewNatsConn 装配 NATS 连接（帧广播 + 结算事件）。
func NewNatsConn(cfg *conf.Bootstrap) (*natsgo.Conn, error) {
	url := ""
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		url = d.GetNats().GetUrl()
	}
	return nats.Connect(nats.Options{URL: url, Name: "battle"})
}

// NatsBattleNotifier 是战斗下行通知实现（nats → gateway → 客户端）。
type NatsBattleNotifier struct {
	nc *natsgo.Conn
}

// NewNatsBattleNotifier 构造下行通知器。
func NewNatsBattleNotifier(nc *natsgo.Conn) *NatsBattleNotifier {
	return &NatsBattleNotifier{nc: nc}
}

// PublishFrame 实现 biz.BattleNotifier：
// 以 atlas.push.<playerID> 信封（type=FrameBroadcast）下发一帧。
func (f *NatsBattleNotifier) PublishFrame(ctx context.Context, playerID, battleID string, frame *locksteppb.LockstepFrame) error {
	payload, err := protojson.Marshal(&gatewayv1.FrameBroadcast{BattleId: battleID, Frame: frame})
	if err != nil {
		return fmt.Errorf("infra: 帧广播编码失败: %w", err)
	}
	return nats.PublishEnvelope(ctx, f.nc, playerID, consts.PushOpFrameBroadcast, payload)
}

// PublishEnd 实现 biz.BattleNotifier：
// 以 atlas.push.<playerID> 信封（type=BattleEndNotify）下发战斗结束通知。
func (f *NatsBattleNotifier) PublishEnd(ctx context.Context, playerID, battleID, winner string) error {
	payload, err := protojson.Marshal(&gatewayv1.BattleEndNotify{BattleId: battleID, WinnerPlayerId: winner})
	if err != nil {
		return fmt.Errorf("infra: 结束通知编码失败: %w", err)
	}
	return nats.PublishEnvelope(ctx, f.nc, playerID, consts.PushOpBattleEnd, payload)
}

// NatsSettlePublisher 是结算事件发布实现。
type NatsSettlePublisher struct {
	nc *natsgo.Conn
}

// NewNatsSettlePublisher 构造结算发布器。
func NewNatsSettlePublisher(nc *natsgo.Conn) *NatsSettlePublisher {
	return &NatsSettlePublisher{nc: nc}
}

// PublishSettled 实现 biz.SettlePublisher（主题 atlas.event.battle.settled）。
func (p *NatsSettlePublisher) PublishSettled(ctx context.Context, ev *battlev1.BattleSettledEvent) error {
	payload, err := protojson.Marshal(ev)
	if err != nil {
		return fmt.Errorf("infra: 结算事件编码失败: %w", err)
	}
	return nats.Publish(ctx, p.nc, consts.EventTopic(settledTopicSuffix), payload)
}

// 静态保证实现接口。
var (
	_ biz.BattleNotifier  = (*NatsBattleNotifier)(nil)
	_ biz.SettlePublisher = (*NatsSettlePublisher)(nil)
)
