package stream

import (
	"sync"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// recordingTell 记录连接生命周期桥投递给战斗 actor 的本地消息（投递端口的内存实现）。
type recordingTell struct {
	mu      sync.Mutex
	battles []string
	msgs    []any
}

// tell 记录一次投递（实现 TellFunc；返回 nil 表示入队成功）。
func (r *recordingTell) tell(battleID string, msg any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.battles = append(r.battles, battleID)
	r.msgs = append(r.msgs, msg)
	return nil
}

// snapshot 返回已记录的对局与消息（拷贝，便于断言）。
func (r *recordingTell) snapshot() ([]string, []any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.battles...), append([]any(nil), r.msgs...)
}

// udpConn 返回一条 UDP 面的连接句柄（按对端键寻址）。
func udpConn(peer, battleID string) Conn {
	return Conn{Kind: transport.KindUDP, Peer: peer, BattleID: battleID}
}

// TestBridgeRecordNotifiesOnlineOnChange 验证验票登记上报上线：同一连接重复登记（逐帧带票）
// 只上报一次，换连接（重连接管）再上报一次。
func TestBridgeRecordNotifiesOnlineOnChange(t *testing.T) {
	rec := &recordingTell{}
	br := NewBridge(NewRegistry(), rec.tell)

	br.Record("p-a", kcpConn(7, "b-1"))
	br.Record("p-a", kcpConn(7, "b-1")) // 逐帧重复登记：不算上线
	br.Record("p-a", kcpConn(8, "b-1")) // 重连接管：算一次上线

	battles, msgs := rec.snapshot()
	if len(msgs) != 2 {
		t.Fatalf("上线上报次数 = %d, 期望 2（首登 + 接管）", len(msgs))
	}
	for i, m := range msgs {
		on, ok := m.(biz.PlayerOnline)
		if !ok || on.PlayerID != "p-a" {
			t.Fatalf("第 %d 条消息 = %#v, 期望 PlayerOnline{p-a}", i, m)
		}
		if battles[i] != "b-1" {
			t.Fatalf("第 %d 条投递对局 = %q, 期望 b-1", i, battles[i])
		}
	}
}

// TestBridgeOfflineReportsMappedPlayer 验证断开事件按端点反查玩家并上报下线：
// 建立事件与未登记端点都不产生消息（身份未定/匿名连接）。
func TestBridgeOfflineReportsMappedPlayer(t *testing.T) {
	reg := NewRegistry()
	rec := &recordingTell{}
	br := NewBridge(reg, rec.tell)
	br.Record("p-a", kcpConn(7, "b-1"))

	handle := br.ForFace(transport.KindKCP)
	handle(engine.ConnEvent{Kind: engine.ConnEventConnected, ID: "9"})
	handle(engine.ConnEvent{Kind: engine.ConnEventDisconnected, ID: "9"})
	handle(engine.ConnEvent{Kind: engine.ConnEventDisconnected, ID: "7"})

	_, msgs := rec.snapshot()
	if len(msgs) != 2 {
		t.Fatalf("消息数 = %d, 期望 2（上线 + 下线）", len(msgs))
	}
	if off, ok := msgs[1].(biz.PlayerOffline); !ok || off.PlayerID != "p-a" {
		t.Fatalf("第 2 条消息 = %#v, 期望 PlayerOffline{p-a}", msgs[1])
	}
	if reg.Online("p-a") {
		t.Fatal("断开事件后注册表仍报在场")
	}
}

// TestBridgeOfflineAfterTakeoverKeepsNewConn 验证重连接管后旧连接的迟到断开事件仍会上报，
// 但注册表里新连接仍在场（是否过期由 actor 侧复核注册表判定，规格 §9.2 硬约束②）。
func TestBridgeOfflineAfterTakeoverKeepsNewConn(t *testing.T) {
	reg := NewRegistry()
	rec := &recordingTell{}
	br := NewBridge(reg, rec.tell)
	br.Record("p-a", kcpConn(7, "b-1"))
	br.Record("p-a", kcpConn(8, "b-1"))

	br.ForFace(transport.KindKCP)(engine.ConnEvent{Kind: engine.ConnEventDisconnected, ID: "7"})

	_, msgs := rec.snapshot()
	if len(msgs) != 3 {
		t.Fatalf("消息数 = %d, 期望 3（两次上线 + 旧连接下线）", len(msgs))
	}
	if off, ok := msgs[2].(biz.PlayerOffline); !ok || off.PlayerID != "p-a" {
		t.Fatalf("第 3 条消息 = %#v, 期望 PlayerOffline{p-a}", msgs[2])
	}
	if !reg.Online("p-a") {
		t.Fatal("接管后新连接应仍在场（旧连接的迟到断开不得摘掉新连接）")
	}
}

// TestBridgeUDPReconnectAfterEviction 验证数据报面同一对端键的空闲淘汰后重新登记算一次回座
// （UDP 重连可能复用同一个对端键，若不重新上报上线，掉线计时无法取消）。
func TestBridgeUDPReconnectAfterEviction(t *testing.T) {
	reg := NewRegistry()
	rec := &recordingTell{}
	br := NewBridge(reg, rec.tell)
	handle := br.ForFace(transport.KindUDP)

	br.Record("p-a", udpConn("1.2.3.4:9", "b-1"))
	handle(engine.ConnEvent{Kind: engine.ConnEventDisconnected, ID: "1.2.3.4:9"})
	if reg.Online("p-a") {
		t.Fatal("空闲淘汰后仍报在场")
	}
	br.Record("p-a", udpConn("1.2.3.4:9", "b-1")) // 同一对端键重新出现：回座

	_, msgs := rec.snapshot()
	if len(msgs) != 3 {
		t.Fatalf("消息数 = %d, 期望 3（上线/下线/回座）", len(msgs))
	}
	if _, ok := msgs[2].(biz.PlayerOnline); !ok {
		t.Fatalf("第 3 条消息 = %#v, 期望 PlayerOnline", msgs[2])
	}
	if !reg.Online("p-a") {
		t.Fatal("回座后注册表未报在场")
	}
}
