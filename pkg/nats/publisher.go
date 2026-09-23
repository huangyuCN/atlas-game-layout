package nats

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/nats-io/nats.go"
)

// Publisher 是业务事件的发布入口：持有 NATS 连接与 topic 命名空间。
// 收口成类型的意义有两条——(nc, topics) 不再成对透传（数据泥团），
// 且"发布必须带命名空间"成为编译期约束（漏带就会发到别人的命名空间上）。
type Publisher struct {
	nc     *nats.Conn
	topics consts.Topics
}

// NewPublisher 构造发布器（topics 决定命名空间，须与订阅方同源）。
func NewPublisher(nc *nats.Conn, topics consts.Topics) *Publisher {
	return &Publisher{nc: nc, topics: topics}
}

// Topics 返回命名空间化的 topic 构造器（订阅方取它与发布方对齐）。
func (p *Publisher) Topics() consts.Topics { return p.topics }

// Publish 向指定 subject 发布 payload（subject 由 Topics 构造）。
func (p *Publisher) Publish(ctx context.Context, subject string, payload []byte) error {
	return Publish(ctx, p.nc, subject, payload)
}

// PublishEnvelope 以 {type, payload} 信封向玩家推送主题（atlas.<ns>.push.<playerID>）发布，
// 供 gateway 订阅后按信封透传下发（见 gateway server push 订阅约定）。
func (p *Publisher) PublishEnvelope(ctx context.Context, playerID, operation string, payload []byte) error {
	env, err := json.Marshal(pushEnvelope{Type: operation, Payload: json.RawMessage(payload)})
	if err != nil {
		return fmt.Errorf("nats: 信封编码失败: %w", err)
	}
	return p.Publish(ctx, p.topics.Push(playerID), env)
}
