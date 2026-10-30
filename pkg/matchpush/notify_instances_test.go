// 多实例逐人地址用例：票条目自带「该玩家被选中实例」的面列表时，通知必须用它
// （而不是事件级列表）；不同玩家可以不同（battle 按 player_id 哈希选实例）。
package matchpush

import (
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
)

// TestNotifyForPlayerInstanceEndpoints 验证逐人实例地址优先：p-1 → 实例 A、p-2 → 实例 B，
// 通知各自拿到自己实例的面列表；票本身仍是各自那张（身份不变、换实例不改票）。
func TestNotifyForPlayerInstanceEndpoints(t *testing.T) {
	ev := startedEvent(t)
	instA := []*battlev1.EdgeEndpoint{{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge-a.example.com:7100"}}
	instB := []*battlev1.EdgeEndpoint{{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge-b.example.com:7100"}}
	ev.BattleTickets[0].Endpoints = instA
	ev.BattleTickets[1].Endpoints = instB

	first, ok := NotifyFor(ev, "p-1")
	if !ok {
		t.Fatal("p-1 应得到通知")
	}
	second, ok := NotifyFor(ev, "p-2")
	if !ok {
		t.Fatal("p-2 应得到通知")
	}
	if !sameEndpoints(first.GetEndpoints(), instA) {
		t.Fatalf("p-1 的 endpoints = %v, 期望自己实例 %v（逐人地址优先于事件级列表）",
			first.GetEndpoints(), instA)
	}
	if !sameEndpoints(second.GetEndpoints(), instB) {
		t.Fatalf("p-2 的 endpoints = %v, 期望自己实例 %v", second.GetEndpoints(), instB)
	}
	if sameEndpoints(first.GetEndpoints(), second.GetEndpoints()) {
		t.Fatal("两个玩家拿到了同一个实例地址——多实例选择没有生效")
	}

	for _, c := range []struct {
		playerID string
		raw      []byte
	}{
		{"p-1", first.GetBattleTicket()},
		{"p-2", second.GetBattleTicket()},
	} {
		tk, err := ticket.Decode(c.raw, testKey())
		if err != nil {
			t.Fatalf("%s 的票无法解出: %v", c.playerID, err)
		}
		if tk.PlayerID != c.playerID {
			t.Fatalf("票内 player_id = %q, 期望 %q（换实例不改票）", tk.PlayerID, c.playerID)
		}
	}
}
