package server

import (
	"bytes"
	"context"
	gamev1opclient "github.com/huangyuCN/atlas-game-layout/api/game/v1/opclient"
	gatewayv1opclient "github.com/huangyuCN/atlas-game-layout/api/gateway/v1/opclient"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	"github.com/huangyuCN/atlas/transport"
	"google.golang.org/protobuf/encoding/protojson"
)

// TestKickCrossInstance 验证跨实例挤下线：实例 A 登录 → 实例 B 登录 →
// 经 nats 控制通道 A 的旧连接收到被挤下线通知并清理本地会话。
func TestKickCrossInstance(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	envA := newGWEnv(t, "gw-a", mr, natsURL)
	envB := newGWEnv(t, "gw-b", mr, natsURL)

	envA.login(t, 1, "p-1")
	envB.login(t, 1, "p-1")

	// 路由已归属 B；A 的旧连接收到被挤下线通知（含原因枚举）。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if !waitFor(3*time.Second, func() bool {
		return hasPush(envA.push.snapshot(), 1, gatewayv1opclient.SessionPushOps.KickedNotify)
	}) {
		t.Fatalf("A 的旧连接未收到挤下线推送: %+v", envA.push.snapshot())
	}
	pushes := envA.push.snapshot()
	for _, p := range pushes {
		if p.op != gatewayv1opclient.SessionPushOps.KickedNotify {
			continue
		}
		var kn gatewayv1.KickedNotify
		if err := protojson.Unmarshal(p.payload, &kn); err != nil {
			t.Fatalf("挤下线通知解码: %v", err)
		}
		if kn.GetReason() != gatewayv1.KickedReason_KICKED_REASON_LOGGED_IN_ELSEWHERE {
			t.Fatalf("挤下线原因不符: %+v", &kn)
		}
	}
	// A 本地会话已清理；路由归 B。
	if got := envA.sess.Count(); got != 0 {
		t.Fatalf("A 本地会话数 = %d, want 0", got)
	}
	r, err := envA.sess.Route(ctx, "p-1")
	if err != nil || r == nil || r.InstanceID != "gw-b" {
		t.Fatalf("路由 = %+v, err = %v, want gw-b", r, err)
	}
	// A 旧连接心跳被拒（本地会话已清理，身份解绑）。
	if _, err := envA.g.Heartbeat(connCtx(transport.KindTCP, gatewayv1.OperationSessionHeartbeatTCP, 1, ""), &gatewayv1.HeartbeatRequest{Ts: 1}); err == nil {
		t.Fatal("A 旧连接心跳应失败")
	}
}

// TestPushOnlyOwnerDelivers 验证双实例推送专项：仅持有连接的实例下发。
func TestPushOnlyOwnerDelivers(t *testing.T) {
	mr, natsURL, pub := newSharedBackends(t)
	envA := newGWEnv(t, "gw-a", mr, natsURL)
	envB := newGWEnv(t, "gw-b", mr, natsURL)

	envA.login(t, 1, "p-1")
	envB.login(t, 1, "p-2")

	// 推送 p-1：仅实例 A 下发。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := PublishPush(ctx, testPublisher(pub), "p-1", gamev1opclient.PlayerServicePushOps.MatchStartedNotify, []byte(`{"match_id":"m-1","battle_id":"b-1"}`)); err != nil {
		t.Fatalf("PublishPush p-1: %v", err)
	}
	if !waitFor(2*time.Second, func() bool {
		return hasPush(envA.push.snapshot(), 1, gamev1opclient.PlayerServicePushOps.MatchStartedNotify)
	}) {
		t.Fatalf("A 未收到 p-1 推送: %+v", envA.push.snapshot())
	}
	time.Sleep(300 * time.Millisecond)
	if hasPush(envB.push.snapshot(), 1, gamev1opclient.PlayerServicePushOps.MatchStartedNotify) {
		t.Fatal("B 不应收到 p-1 推送")
	}

	// 推送 p-2：仅实例 B 下发。
	if err := PublishPush(ctx, testPublisher(pub), "p-2", gamev1opclient.PlayerServicePushOps.MatchStartedNotify, []byte(`{"match_id":"m-2","battle_id":"b-2"}`)); err != nil {
		t.Fatalf("PublishPush p-2: %v", err)
	}
	if !waitFor(2*time.Second, func() bool {
		return hasPush(envB.push.snapshot(), 1, gamev1opclient.PlayerServicePushOps.MatchStartedNotify)
	}) {
		t.Fatalf("B 未收到 p-2 推送: %+v", envB.push.snapshot())
	}
}

// TestKickCrossInstanceKeepsNewRoute 验证跨实例挤下线不会误删新实例路由。
func TestKickCrossInstanceKeepsNewRoute(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	envA := newGWEnv(t, "gw-a", mr, natsURL)
	envB := newGWEnv(t, "gw-b", mr, natsURL)

	envA.login(t, 1, "p-1")
	envB.login(t, 1, "p-1")

	// A 的旧连接收到挤下线通知。
	if !waitFor(3*time.Second, func() bool {
		return hasPush(envA.push.snapshot(), 1, gatewayv1opclient.SessionPushOps.KickedNotify)
	}) {
		t.Fatalf("未收到挤下线通知: %+v", envA.push.snapshot())
	}
	// B 的路由必须完好（A 的控制通道清理不得误删）。
	// 轮询观察一小段时间：若 A 侧误删发生，B 路由会在该窗口内消失。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		r, err := envB.sess.Route(ctx, "p-1")
		if err != nil || r == nil || r.InstanceID != "gw-b" {
			t.Fatalf("B 路由被误删: r=%+v err=%v", r, err)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// B 的本地会话完好（令牌未变，不受 A 控制通道清理影响）。
	if sess, ok := envB.sess.LocalSession("p-1"); !ok || sess.Token == "" {
		t.Fatal("B 本地会话应完好")
	}
}

// TestMatchStartedNotifyRelay 验证成局事件 → 开局通知推送（规格 §7：
// matcher 发布 atlas.event.match.started → gateway 向参战玩家推送 gamev1.MatchStartedNotify）。
func TestMatchStartedNotifyRelay(t *testing.T) {
	mr, natsURL, pub := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	env.login(t, 1, "p-1")

	// 每位参战玩家一张**自己的**票（票即身份凭据：通知必须逐人构造，绝不得群发同一份）。
	key := []byte("0123456789abcdef0123456789abcdef")
	now := time.Now()
	ticketA, err := ticket.Encode(ticket.Ticket{
		Version: ticket.Version1, KID: 1, PlayerID: "p-1", BattleID: "b-1",
		IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}, key)
	if err != nil {
		t.Fatalf("签票 A: %v", err)
	}
	ticketB, err := ticket.Encode(ticket.Ticket{
		Version: ticket.Version1, KID: 1, PlayerID: "p-2", BattleID: "b-1",
		IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}, key)
	if err != nil {
		t.Fatalf("签票 B: %v", err)
	}

	// 发布成局事件（matcher 侧形态：接入层面→地址列表 + 逐人票据名单）。
	payload, err := protojson.Marshal(&matcherv1.MatchStartedEvent{
		MatchId:   "m-1",
		BattleId:  "b-1",
		PlayerIds: []string{"p-1", "p-2"},
		BattleEndpoints: []*battlev1.EdgeEndpoint{
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "127.0.0.1:7100"},
		},
		BattleTickets: []*battlev1.BattleTicketEntry{
			{PlayerId: "p-1", Ticket: ticketA},
			{PlayerId: "p-2", Ticket: ticketB},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := nats.Publish(ctx, pub, testTopics.MatchStarted(), payload); err != nil {
		t.Fatalf("发布成局事件: %v", err)
	}

	// 参战玩家收到开局通知（含对局 ID、battle ID、参战名单、接入层地址与他自己的票）。
	if !waitFor(2*time.Second, func() bool {
		return hasPush(env.push.snapshot(), 1, gamev1opclient.PlayerServicePushOps.MatchStartedNotify)
	}) {
		t.Fatalf("未收到开局通知: %+v", env.push.snapshot())
	}
	pushes := env.push.snapshot()
	raw := pushes[len(pushes)-1].payload
	var n gamev1.MatchStartedNotify
	if err := protojson.Unmarshal(raw, &n); err != nil {
		t.Fatalf("开局通知解码: %v", err)
	}
	if n.GetBattleId() != "b-1" || n.GetMatchId() != "m-1" || len(n.GetPlayerIds()) != 2 {
		t.Fatalf("开局通知不符: %+v", &n)
	}
	if got := n.GetEndpoints(); len(got) != 1 ||
		got[0].GetTransport() != battlev1.EdgeTransport_EDGE_TRANSPORT_WS || got[0].GetAddress() != "127.0.0.1:7100" {
		t.Fatalf("接入层面列表不符: %v", got)
	}
	// 收到的必须是自己那张票（可解出自己的身份），且**全文不得出现别人的票**。
	if !bytes.Equal(n.GetBattleTicket(), ticketA) {
		t.Fatalf("通知未携带本人票据: got %d 字节", len(n.GetBattleTicket()))
	}
	decoded, err := ticket.Decode(n.GetBattleTicket(), key)
	if err != nil || decoded.PlayerID != "p-1" || decoded.BattleID != "b-1" {
		t.Fatalf("本人票据解码不符: %+v err=%v", decoded, err)
	}
	if bytes.Contains(raw, ticketB) {
		t.Fatal("通知里出现了其他玩家的票据（凭据泄露）")
	}
}

// TestMatchFailedNotifyPush 验证失败事件链路：matcher 发布失败事件
// （atlas.event.match.failed）→ gateway 订阅 → 按玩家推送 gamev1.MatchFailedNotify。
func TestMatchFailedNotifyPush(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	env.login(t, 1, "p-1")

	// 模拟 matcher 发布失败事件（与 NatsEventPublisher.PublishFailed 同编解码）。
	pub, err := nats.Connect(nats.Options{URL: natsURL, Name: "publisher"})
	if err != nil {
		t.Fatalf("publisher 连接: %v", err)
	}
	t.Cleanup(pub.Close)
	ev, err := protojson.Marshal(&matcherv1.MatchFailedEvent{
		PlayerIds: []string{"p-1"}, Reason: matcherv1.MatchFailReason_MATCH_FAIL_REASON_TIMEOUT, TicketId: "t-9",
	})
	if err != nil {
		t.Fatalf("事件编码: %v", err)
	}
	if err := pub.Publish(testTopics.MatchFailed(), ev); err != nil {
		t.Fatalf("发布失败事件: %v", err)
	}

	if !waitFor(3*time.Second, func() bool {
		return hasPush(env.push.snapshot(), 1, gamev1opclient.PlayerServicePushOps.MatchFailedNotify)
	}) {
		t.Fatalf("未收到匹配失败通知: %+v", env.push.snapshot())
	}
	pushes := env.push.snapshot()
	var n gamev1.MatchFailedNotify
	if err := protojson.Unmarshal(pushes[len(pushes)-1].payload, &n); err != nil {
		t.Fatalf("失败通知解码: %v", err)
	}
	if n.GetTicketId() != "t-9" || n.GetReason() != matcherv1.MatchFailReason_MATCH_FAIL_REASON_TIMEOUT {
		t.Fatalf("失败通知不符: %+v", &n)
	}
}

// TestSessionSweepNotifiesPlayerActor 验证异常下线联动：心跳过期清扫后
// （未被接管）向 PlayerActor 发 Logout（SESSION_EXPIRED）→ 触发撮合域 OnStop 兜底。
func TestSessionSweepNotifiesPlayerActor(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	env.login(t, 1, "p-1")

	// 模拟心跳超时（把本地会话心跳拨回过期窗口），再手动驱动一次清扫。
	// 真实环境由 Start 的清扫循环周期驱动。
	sess, ok := env.sess.LocalSession("p-1")
	if !ok {
		t.Fatal("本地会话缺失")
	}
	sess.LastHeartbeat = time.Now().Add(-time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if got := env.sess.SweepOnce(ctx); got != 1 {
		t.Fatalf("过期清扫应移除 1 个会话, got %d", got)
	}
	// 路由应已被属主确认删除。
	if r, err := env.sess.Route(ctx, "p-1"); err != nil || r != nil {
		t.Fatalf("清扫后路由残留: r=%+v err=%v", r, err)
	}
	// 清扫联动向 PlayerActor 发 Logout（SESSION_EXPIRED）。
	if !waitFor(3*time.Second, func() bool {
		for _, lg := range env.mock.logoutMsgs {
			if lg.GetReason() == gamev1.LogoutReason_LOGOUT_REASON_SESSION_EXPIRED {
				return true
			}
		}
		return false
	}) {
		t.Fatal("清扫联动未发 Logout(SESSION_EXPIRED)")
	}
}
