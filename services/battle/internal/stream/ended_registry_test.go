package stream

import (
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas/transport"
)

// endNotifies 返回该端口收到的战斗结束通知（operation + 载荷）。
func endNotifies(port *fakePort) []*battlev1.BattleEndNotify {
	out := make([]*battlev1.BattleEndNotify, 0, len(port.pushed))
	for _, p := range port.pushed {
		if p.operation != battlev1opclient.BattleServicePushOps.BattleEndNotify {
			continue
		}
		if n, ok := p.msg.(*battlev1.BattleEndNotify); ok {
			out = append(out, n)
		}
	}
	return out
}

// TestRegistryCloseBattleRepushesEnd 验证结算关闭前的有界重投：
// 每连接投 EndRetries 次结算通知（数据报面无重传，单次推送易丢），随后只关一次。
func TestRegistryCloseBattleRepushesEnd(t *testing.T) {
	reg, kcpPort, udpPort := newTestRegistry()
	reg.RecordEnded("b-1", "p-a", []string{"p-a", "p-b"})
	a := kcpConn(7, "b-1")
	b := Conn{Kind: transport.KindUDP, Peer: "1.2.3.4:9", BattleID: "b-1"}
	reg.Register("p-a", a)
	reg.Register("p-b", b)

	reg.CloseBattle("b-1")

	for name, port := range map[string]*fakePort{"KCP": kcpPort, "UDP": udpPort} {
		ends := endNotifies(port)
		if len(ends) != EndRetries {
			t.Fatalf("%s 面结算通知重投次数 = %d, 期望 %d", name, len(ends), EndRetries)
		}
		for _, n := range ends {
			if n.GetBattleId() != "b-1" || n.GetWinnerPlayerId() != "p-a" {
				t.Fatalf("%s 面重投载荷不符: %+v", name, n)
			}
		}
		if len(port.closed) != 1 {
			t.Fatalf("%s 面关闭次数 = %d, 期望 1", name, len(port.closed))
		}
	}
	if reg.Count() != 0 || reg.CountBattle("b-1") != 0 {
		t.Fatalf("结算关闭后仍留有登记连接: count=%d battle=%d", reg.Count(), reg.CountBattle("b-1"))
	}
}

// TestRegistryCloseBattleWithoutRecord 验证未留档时只关闭不重投（留档缺失不该发空通知）。
func TestRegistryCloseBattleWithoutRecord(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	reg.Register("p-a", kcpConn(7, "b-1"))

	reg.CloseBattle("b-1")

	if len(kcpPort.pushed) != 0 {
		t.Fatalf("未留档仍推送了 %d 条通知", len(kcpPort.pushed))
	}
	if len(kcpPort.closed) != 1 {
		t.Fatalf("关闭次数 = %d, 期望 1", len(kcpPort.closed))
	}
}

// TestRegistryReplayEndToFreshConn 验证补投：向重连后的新连接补投留档结果，且不再关闭它
// （关闭只发生在结算一次，迟到 op 不得触发再次关闭）。
func TestRegistryReplayEndToFreshConn(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	reg.RecordEnded("b-1", "p-a", []string{"p-a", "p-b"})

	fresh := kcpConn(21, "b-1")
	for i := 0; i < MaxEndReplays; i++ {
		if !reg.ReplayEnd("p-a", fresh) {
			t.Fatalf("第 %d 次补投被拒（上限 %d 之前都应放行）", i+1, MaxEndReplays)
		}
	}
	if got := len(endNotifies(kcpPort)); got != MaxEndReplays {
		t.Fatalf("补投次数 = %d, 期望 %d", got, MaxEndReplays)
	}
	if reg.ReplayEnd("p-a", fresh) {
		t.Fatalf("补投超出上限 %d 仍放行", MaxEndReplays)
	}
	if len(kcpPort.closed) != 0 {
		t.Fatalf("补投触发了连接关闭 %d 次（迟到 op 不得再次关连接）", len(kcpPort.closed))
	}
	for _, n := range endNotifies(kcpPort) {
		if n.GetWinnerPlayerId() != "p-a" {
			t.Fatalf("补投载荷胜者 = %q, 期望 p-a", n.GetWinnerPlayerId())
		}
	}
}

// TestRegistryReplayEndRejectsUnknown 验证未留档/名单外玩家不补投（不给未参战者发结算结果）。
func TestRegistryReplayEndRejectsUnknown(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	fresh := kcpConn(21, "b-1")

	if reg.Ended("b-1") {
		t.Fatal("未留档对局被判为已结束")
	}
	if reg.ReplayEnd("p-a", fresh) {
		t.Fatal("未留档对局发放了补投")
	}
	reg.RecordEnded("b-1", "p-a", []string{"p-a"})
	if !reg.Ended("b-1") {
		t.Fatal("留档后未判为已结束")
	}
	if reg.ReplayEnd("p-x", fresh) {
		t.Fatal("名单外玩家获得了补投")
	}
	if len(kcpPort.pushed) != 0 {
		t.Fatalf("被拒的补投仍推送了 %d 条", len(kcpPort.pushed))
	}
}

// TestRegistryEndedExpiryStopsReplay 验证留档过期后不再拒绝、不再补投（墓碑随 TTL 消失）。
func TestRegistryEndedExpiryStopsReplay(t *testing.T) {
	reg := NewRegistryWithTTL(time.Minute)
	kcpPort := &fakePort{}
	reg.BindPort(transport.KindKCP, kcpPort)
	now := time.Now()
	reg.ended.now = func() time.Time { return now }
	reg.RecordEnded("b-1", "p-a", []string{"p-a"})

	reg.ended.now = func() time.Time { return now.Add(2 * time.Minute) }
	if reg.Ended("b-1") {
		t.Fatal("留档过期后仍判为已结束")
	}
	if reg.ReplayEnd("p-a", kcpConn(21, "b-1")) {
		t.Fatal("留档过期后仍发放补投")
	}
	if len(kcpPort.pushed) != 0 {
		t.Fatalf("过期留档仍推送了 %d 条", len(kcpPort.pushed))
	}
}
