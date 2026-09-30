// Package infra 提供 battle 服务的外部依赖装配：
// mongo（结算落库）、nats（结算事件）、actor 集群运行时。
// 战斗域通知（帧广播/战斗结束）不走 nats：经帧引擎直连推送（见 biz.BattlePusher）。
package infra

import (
	"context"
	"fmt"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	natsgo "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
)

// settledTopicSuffix 是结算事件主题后缀（atlas.<ns>.event.battle.settled）。
const settledTopicSuffix = "battle.settled"

// NewNatsConn 装配 NATS 连接（结算事件发布）。
func NewNatsConn(cfg *conf.Bootstrap) (*natsgo.Conn, error) {
	url := ""
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		url = d.GetNats().GetUrl()
	}
	return pkgnats.Connect(pkgnats.Options{URL: url, Name: "battle"})
}

// NatsSettlePublisher 是结算事件发布实现。
type NatsSettlePublisher struct {
	pub *pkgnats.Publisher
}

// NewNatsSettlePublisher 构造结算发布器。
func NewNatsSettlePublisher(pub *pkgnats.Publisher) *NatsSettlePublisher {
	return &NatsSettlePublisher{pub: pub}
}

// PublishSettled 实现 biz.SettlePublisher（主题 atlas.<ns>.event.battle.settled）。
func (p *NatsSettlePublisher) PublishSettled(ctx context.Context, ev *battlev1.BattleSettledEvent) error {
	payload, err := protojson.Marshal(ev)
	if err != nil {
		return fmt.Errorf("infra: 结算事件编码失败: %w", err)
	}
	return p.pub.Publish(ctx, p.pub.Topics().Event(settledTopicSuffix), payload)
}

// 静态保证实现接口。
var _ biz.SettlePublisher = (*NatsSettlePublisher)(nil)
