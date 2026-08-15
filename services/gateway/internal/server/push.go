package server

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/nats-io/nats.go"
)

// pushEnvelope 是 nats 推送事件的消息格式：
// type 为推送 operation（消息 protobuf 完整名），payload 为消息编码字节。
type pushEnvelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// kickNotice 是 gateway 控制通道（atlas.gw.<instanceID>）的挤下线通知。
type kickNotice struct {
	PlayerID string `json:"player_id"`
}

// StartRelay 启动下行推送与控制通道订阅（D11/D13）：
//   - 订阅 atlas.push.> 通配主题，按路由表仅持有连接的实例下发；
//   - 订阅本实例控制通道，处理跨实例挤下线通知。
func (g *Gateway) StartRelay(ctx context.Context) error {
	if g.nc == nil {
		return nil
	}
	if _, err := pkgnats.SubscribeWildcard(g.nc, consts.TopicPush+">", g.onPushEvent); err != nil {
		return err
	}
	_, err := pkgnats.Subscribe(g.nc, consts.GatewayTopic(g.instanceID), g.onKickNotice)
	return err
}

// relayTimeout 是推送/控制回调访问 redis 的兜底超时（防回调 goroutine 悬挂）。
const relayTimeout = 2 * time.Second

// onPushEvent 处理推送事件：解析玩家 ID 与消息信封，仅本实例持有该玩家时下发。
func (g *Gateway) onPushEvent(subject string, data []byte) {
	playerID := strings.TrimPrefix(subject, consts.TopicPush)
	if playerID == "" || playerID == subject {
		return
	}
	var env pushEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), relayTimeout)
	defer cancel()
	route, err := g.sess.Route(ctx, playerID)
	if err != nil || route == nil || route.InstanceID != g.instanceID {
		return
	}
	_ = g.sess.PushRaw(playerID, env.Type, env.Payload)
}

// onKickNotice 处理跨实例挤下线：向本地旧会话推送被挤下线通知并清理本地表。
func (g *Gateway) onKickNotice(_ string, data []byte) {
	var kn kickNotice
	if err := json.Unmarshal(data, &kn); err != nil || kn.PlayerID == "" {
		return
	}
	sess, ok := g.sess.LocalSession(kn.PlayerID)
	if !ok {
		return
	}
	g.pushKicked(sess)
	// 清理本地会话；redis 路由已被新实例覆盖，Unbind 的属主校验会跳过删除。
	ctx, cancel := context.WithTimeout(context.Background(), relayTimeout)
	defer cancel()
	if conn := sess.Biz; conn != nil {
		g.sess.Unbind(ctx, kn.PlayerID, conn.ID)
	}
}

// publishControl 向指定 gateway 实例的控制通道发布挤下线通知。
func publishControl(ctx context.Context, nc *nats.Conn, instanceID string, data []byte) error {
	if nc == nil {
		return nil
	}
	return pkgnats.Publish(ctx, nc, consts.GatewayTopic(instanceID), data)
}

// PublishPush 向玩家推送消息（供 gateway 上层/测试发布 nats 推送事件）。
func PublishPush(ctx context.Context, nc *nats.Conn, playerID, operation string, payload json.RawMessage) error {
	env, err := json.Marshal(pushEnvelope{Type: operation, Payload: payload})
	if err != nil {
		return err
	}
	return pkgnats.Publish(ctx, nc, consts.PushTopic(playerID), env)
}
