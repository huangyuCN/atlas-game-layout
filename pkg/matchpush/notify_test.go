package matchpush

import (
	"bytes"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
)

// testKey 返回 32 字节测试密钥（与 battle 侧同形）。
func testKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

// encodedTicket 用真票据编码器签发一张票（断言必须能解出正确身份，不能用假字节）。
func encodedTicket(t *testing.T, playerID, battleID string) []byte {
	t.Helper()
	issued := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	raw, err := ticket.Encode(ticket.Ticket{
		Version:   ticket.Version1,
		KID:       1,
		PlayerID:  playerID,
		BattleID:  battleID,
		IssuedAt:  issued,
		ExpiresAt: issued.Add(2 * time.Minute),
	}, testKey())
	if err != nil {
		t.Fatalf("签发测试票 %s: %v", playerID, err)
	}
	return raw
}

// testEndpoints 返回接入层的面→地址列表（ws 与 kcp 两面，用于断言逐个照抄、不重组）。
func testEndpoints() []*battlev1.EdgeEndpoint {
	return []*battlev1.EdgeEndpoint{
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.example.com:7100"},
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: "edge.example.com:7101"},
	}
}

// startedEvent 构造双人成局事件（每人一张真票）。
func startedEvent(t *testing.T) *matcherv1.MatchStartedEvent {
	t.Helper()
	return &matcherv1.MatchStartedEvent{
		MatchId:         "m-1",
		BattleId:        "b-1",
		PlayerIds:       []string{"p-1", "p-2"},
		BattleEndpoints: testEndpoints(),
		BattleTickets: []*battlev1.BattleTicketEntry{
			{PlayerId: "p-1", Ticket: encodedTicket(t, "p-1", "b-1")},
			{PlayerId: "p-2", Ticket: encodedTicket(t, "p-2", "b-1")},
		},
	}
}

// TestNotifyForPerPlayerTicket 验证按人构造：两个玩家拿到的票**不同**，
// 且各自能解出自己的 player_id 与本局 battle_id；endpoint/对局字段照抄事件。
func TestNotifyForPerPlayerTicket(t *testing.T) {
	ev := startedEvent(t)

	first, ok := NotifyFor(ev, "p-1")
	if !ok {
		t.Fatal("p-1 应得到通知（名单里有票）")
	}
	second, ok := NotifyFor(ev, "p-2")
	if !ok {
		t.Fatal("p-2 应得到通知（名单里有票）")
	}
	if bytes.Equal(first.GetBattleTicket(), second.GetBattleTicket()) {
		t.Fatal("两个玩家的 battle_ticket 相同——票据是身份凭据，绝不能群发同一份")
	}
	if !sameEndpoints(first.GetEndpoints(), second.GetEndpoints()) {
		t.Fatalf("两个玩家的 endpoints 不同: %v / %v（面列表对所有人相同）",
			first.GetEndpoints(), second.GetEndpoints())
	}

	cases := []struct {
		playerID string
		notify   *gamev1.MatchStartedNotify
	}{
		{"p-1", first},
		{"p-2", second},
	}
	for _, c := range cases {
		tk, err := ticket.Decode(c.notify.GetBattleTicket(), testKey())
		if err != nil {
			t.Fatalf("%s 的票无法解出: %v", c.playerID, err)
		}
		if tk.PlayerID != c.playerID {
			t.Errorf("票内 player_id = %q, 期望 %q（拿到别人的票即身份冒用）", tk.PlayerID, c.playerID)
		}
		if tk.BattleID != "b-1" {
			t.Errorf("票内 battle_id = %q, 期望 b-1", tk.BattleID)
		}
		if got := c.notify.GetEndpoints(); !sameEndpoints(got, ev.GetBattleEndpoints()) {
			t.Errorf("%s 的 endpoints = %v, 期望照抄事件 %v（面列表对所有人相同）",
				c.playerID, got, ev.GetBattleEndpoints())
		}
		if c.notify.GetMatchId() != "m-1" || c.notify.GetBattleId() != "b-1" {
			t.Errorf("%s 的对局字段 = %q/%q, 期望 m-1/b-1", c.playerID, c.notify.GetMatchId(), c.notify.GetBattleId())
		}
		if len(c.notify.GetPlayerIds()) != 2 {
			t.Errorf("%s 的 player_ids = %v, 期望照抄事件的两名参战者", c.playerID, c.notify.GetPlayerIds())
		}
	}
}

// TestNotifyForRejectsMissingTicket 验证「名单里没有该玩家」与「该玩家没有票」
// 都不产出通知（返回 nil），且空票（长度为 0）不算有票。
func TestNotifyForRejectsMissingTicket(t *testing.T) {
	ev := startedEvent(t)
	ev.PlayerIds = append(ev.PlayerIds, "p-3") // 在名单里但没有票
	ev.BattleTickets[1].Ticket = nil           // p-2 的票为空
	ev.BattleTickets = append(ev.BattleTickets, &battlev1.BattleTicketEntry{
		PlayerId: "p-4", // 有票但不在名单里
		Ticket:   encodedTicket(t, "p-4", "b-1"),
	})

	for _, playerID := range []string{"p-2", "p-3", "p-4", "p-none"} {
		notify, ok := NotifyFor(ev, playerID)
		if ok || notify != nil {
			t.Errorf("%s 不应得到通知: ok=%v notify=%v", playerID, ok, notify)
		}
	}
	// 名单里的有票玩家仍正常下发（负例不误伤正例）。
	if _, ok := NotifyFor(ev, "p-1"); !ok {
		t.Error("p-1 应仍得到通知")
	}
}

// TestNotifyForEmptyEvent 验证空事件/空名单/空票一律不产出通知。
func TestNotifyForEmptyEvent(t *testing.T) {
	if notify, ok := NotifyFor(nil, "p-1"); ok || notify != nil {
		t.Errorf("nil 事件不应产出通知: ok=%v notify=%v", ok, notify)
	}
	empty := &matcherv1.MatchStartedEvent{BattleEndpoints: testEndpoints()}
	if notify, ok := NotifyFor(empty, "p-1"); ok || notify != nil {
		t.Errorf("空名单/空票不应产出通知: ok=%v notify=%v", ok, notify)
	}
	if notify, ok := NotifyFor(startedEvent(t), ""); ok || notify != nil {
		t.Errorf("空 player_id 不应产出通知: ok=%v notify=%v", ok, notify)
	}
}

// sameEndpoints 逐项比对两份面列表（面与地址都必须一致；顺序也一致——照抄不重排）。
func sameEndpoints(a, b []*battlev1.EdgeEndpoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetTransport() != b[i].GetTransport() || a[i].GetAddress() != b[i].GetAddress() {
			return false
		}
	}
	return true
}
