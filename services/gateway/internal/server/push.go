package server

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	atlaslog "github.com/huangyuCN/atlas/log"
	"google.golang.org/protobuf/encoding/protojson"
)

// pushEnvelope 是 nats 推送事件的消息格式：
// type 为推送 operation（消息 protobuf 完整名），payload 为消息编码字节。
type pushEnvelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// kickNotice 是 gateway 控制通道（atlas.<ns>.gw.<instanceID>）的挤下线通知。
type kickNotice struct {
	PlayerID string `json:"player_id"`
}

// StartRelay 启动下行推送与控制通道订阅（D11/D13）：
//   - 订阅 atlas.<ns>.push.> 通配主题，按路由表仅持有连接的实例下发；
//   - 订阅成局事件（atlas.<ns>.event.match.started），向参战玩家推送开局通知；
//   - 订阅失败事件（atlas.<ns>.event.match.failed），向玩家推送失败/取消通知；
//   - 订阅本实例控制通道，处理跨实例挤下线通知。
//
// 命名空间由配置（runtime.env）决定：与 matcher/battle 的发布方同源，否则订阅落空。
func (g *Gateway) StartRelay(ctx context.Context) error {
	if g.nc == nil {
		return nil
	}
	if _, err := pkgnats.SubscribeWildcard(g.nc, g.pub.Topics().PushWildcard(), g.onPushEvent); err != nil {
		return err
	}
	if _, err := pkgnats.Subscribe(g.nc, g.pub.Topics().MatchStarted(), g.onMatchStarted); err != nil {
		return err
	}
	if _, err := pkgnats.Subscribe(g.nc, g.pub.Topics().MatchFailed(), g.onMatchFailed); err != nil {
		return err
	}
	if _, err := pkgnats.Subscribe(g.nc, g.pub.Topics().PartyRoster(), g.onPartyRoster); err != nil {
		return err
	}
	_, err := pkgnats.Subscribe(g.nc, g.pub.Topics().GatewayControl(g.instanceID), g.onKickNotice)
	return err
}

// relayEvent 是事件转发的公共骨架：解码 nats 事件 → 构造通知消息 →
// 按参战玩家名单逐一下发。build 返回 (参战玩家名单, 通知 payload, 是否有效)；
// 解码/编码失败静默忽略（事件总线消息允许丢失，恢复路径见查询接口）。
func (g *Gateway) relayEvent(operation string, data []byte,
	build func(ctx context.Context) (players []string, notify []byte, ok bool)) {
	ctx, cancel := context.WithTimeout(context.Background(), relayTimeout)
	defer cancel()
	players, notify, ok := build(ctx)
	if !ok {
		return
	}
	for _, pid := range players {
		_ = g.pub.PublishEnvelope(ctx, pid, operation, notify)
	}
}

// onMatchStarted 处理成局事件：向参战玩家推送开局通知
// （MatchStartedNotify：对局 ID + battle ID + 参战名单 + 实例端点，规格 §7）。
func (g *Gateway) onMatchStarted(_ string, data []byte) {
	g.relayEvent(consts.PushOpMatchStarted, data, func(_ context.Context) ([]string, []byte, bool) {
		var ev matcherv1.MatchStartedEvent
		if err := protojson.Unmarshal(data, &ev); err != nil {
			return nil, nil, false
		}
		payload, err := protojson.Marshal(&gamev1.MatchStartedNotify{
			MatchId:   ev.GetMatchId(),
			BattleId:  ev.GetBattleId(),
			PlayerIds: ev.GetPlayerIds(),
			Endpoint:  ev.GetBattleEndpoint(),
		})
		if err != nil {
			return nil, nil, false
		}
		return ev.GetPlayerIds(), payload, true
	})
}

// onMatchFailed 处理失败事件：向玩家推送失败/取消通知（MatchFailedNotify）。
func (g *Gateway) onMatchFailed(_ string, data []byte) {
	g.relayEvent(consts.PushOpMatchFailed, data, func(_ context.Context) ([]string, []byte, bool) {
		var ev matcherv1.MatchFailedEvent
		if err := protojson.Unmarshal(data, &ev); err != nil {
			return nil, nil, false
		}
		payload, err := protojson.Marshal(&gamev1.MatchFailedNotify{
			TicketId: ev.GetTicketId(),
			Reason:   ev.GetReason(),
		})
		if err != nil {
			return nil, nil, false
		}
		return ev.GetPlayerIds(), payload, true
	})
}

// onPartyRoster 处理名册变更事件：向全队推送名册快照（PartyRosterNotify）。
func (g *Gateway) onPartyRoster(_ string, data []byte) {
	g.relayEvent(consts.PushOpPartyRoster, data, func(_ context.Context) ([]string, []byte, bool) {
		var ev matcherv1.PartyRosterEvent
		if err := protojson.Unmarshal(data, &ev); err != nil {
			atlaslog.Error("gateway: 名册事件解码失败", "err", err, "raw", string(data))
			return nil, nil, false
		}
		payload, err := protojson.Marshal(&gamev1.PartyRosterNotify{
			PartyId:   ev.GetPartyId(),
			LeaderId:  ev.GetLeaderId(),
			PlayerIds: ev.GetPlayerIds(),
			Reason:    ev.GetReason(),
		})
		if err != nil {
			return nil, nil, false
		}
		return ev.GetPlayerIds(), payload, true
	})
}

// relayTimeout 是推送/控制回调访问 redis 的兜底超时（防回调 goroutine 悬挂）。
const relayTimeout = 2 * time.Second

// onPushEvent 处理推送事件：解析玩家 ID 与消息信封，仅本实例持有该玩家时下发。
func (g *Gateway) onPushEvent(subject string, data []byte) {
	playerID := strings.TrimPrefix(subject, g.pub.Topics().PushPrefix())
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
func publishControl(ctx context.Context, pub *pkgnats.Publisher, instanceID string, data []byte) error {
	if pub == nil {
		return nil // 未配置 NATS（测试/裁剪形态）：无控制通道可发
	}
	return pub.Publish(ctx, pub.Topics().GatewayControl(instanceID), data)
}

// PublishPush 向玩家推送消息（供 gateway 上层/测试发布 nats 推送事件）。
func PublishPush(ctx context.Context, pub *pkgnats.Publisher, playerID, operation string, payload json.RawMessage) error {
	return pub.PublishEnvelope(ctx, playerID, operation, payload)
}
