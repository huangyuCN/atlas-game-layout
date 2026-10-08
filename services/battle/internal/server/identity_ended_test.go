package server

import (
	"sync"
	"testing"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/transport"
)

// fakeFrameConn 是帧槽验票直连端口的内存实现：记录登记与补投调用（验票用例的观测点）。
type fakeFrameConn struct {
	mu       sync.Mutex
	ended    map[string]bool
	recorded []stream.Conn
	replayed []stream.Conn
}

// newFakeFrameConn 构造直连端口内存实现。
func newFakeFrameConn() *fakeFrameConn {
	return &fakeFrameConn{ended: make(map[string]bool)}
}

// markEnded 标记该对局已结束（模拟结算留档后的判定）。
func (f *fakeFrameConn) markEnded(battleID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ended[battleID] = true
}

// Record 实现 FrameConn：记录一次登记。
func (f *fakeFrameConn) Record(playerID string, c stream.Conn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = playerID
	f.recorded = append(f.recorded, c)
}

// Ended 实现 FrameConn：按标记判定。
func (f *fakeFrameConn) Ended(battleID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ended[battleID]
}

// ReplayEnd 实现 FrameConn：记录一次补投，留档存在即返回真。
func (f *fakeFrameConn) ReplayEnd(_ string, c stream.Conn) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.ended[c.BattleID] {
		return false
	}
	f.replayed = append(f.replayed, c)
	return true
}

// snapshot 返回登记与补投的计数（避免断言与写入竞态）。
func (f *fakeFrameConn) snapshot() (recorded, replayed []stream.Conn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]stream.Conn(nil), f.recorded...), append([]stream.Conn(nil), f.replayed...)
}

// TestTicketIdentityRejectsEndedBattle 验证已结束对局的迟到 op：拿到稳定 reason（BATTLE_ENDED）、
// **不登记连接**（不投递 ⇒ 不会经 SpawnAuto 懒激活重建），并把留档结果补投到本次连接。
func TestTicketIdentityRejectsEndedBattle(t *testing.T) {
	conn := newFakeFrameConn()
	conn.markEnded("b-1")
	resolver := NewTicketIdentity(testTicketKey(), conn)
	ctx := frameCtx(transport.KindKCP, 7, "", b64(encodeTicket(t, "p-a", "b-1", time.Minute)))

	_, err := resolver.Resolve(ctx, relay.RouteEntry{Operation: "/battle.v1.BattleService/SendFrameInput"})
	if !errorv1.IsBattleEnded(err) {
		t.Fatalf("已结束对局的错误 = %v（reason=%s），期望 BATTLE_ENDED", err, atlaserrors.Reason(err))
	}
	if got := atlaserrors.Reason(err); got != errorv1.ReasonBattleEnded() {
		t.Fatalf("reason = %q，期望 %q（稳定 reason 才可判定，字面量不得散落）", got, errorv1.ReasonBattleEnded())
	}
	recorded, replayed := conn.snapshot()
	if len(recorded) != 0 {
		t.Fatalf("已结束对局仍登记了直连 %d 次（登记即投递 → 懒激活重建）", len(recorded))
	}
	if len(replayed) != 1 || replayed[0].BattleID != "b-1" || replayed[0].ConnID != 7 {
		t.Fatalf("重连后未把结算结果补投到本次连接: %+v", replayed)
	}
}

// TestTicketIdentityEndedWithoutConnHandle 验证取不到连接句柄时照样拒绝：不登记、不补投，
// 但 reason 不变（拒绝语义与能否补投无关）。
func TestTicketIdentityEndedWithoutConnHandle(t *testing.T) {
	conn := newFakeFrameConn()
	conn.markEnded("b-1")
	resolver := NewTicketIdentity(testTicketKey(), conn)
	ctx := frameCtx(transport.KindKCP, 0, "", b64(encodeTicket(t, "p-a", "b-1", time.Minute)))

	if _, err := resolver.Resolve(ctx, relay.RouteEntry{}); !errorv1.IsBattleEnded(err) {
		t.Fatalf("错误 = %v，期望 BATTLE_ENDED", err)
	}
	recorded, replayed := conn.snapshot()
	if len(recorded) != 0 || len(replayed) != 0 {
		t.Fatalf("无连接句柄仍登记/补投: recorded=%d replayed=%d", len(recorded), len(replayed))
	}
}

// TestTicketIdentityRunningUnchanged 验证进行中的对局行为不变：登记一次、不补投、不拒绝。
func TestTicketIdentityRunningUnchanged(t *testing.T) {
	conn := newFakeFrameConn()
	resolver := NewTicketIdentity(testTicketKey(), conn)
	ctx := frameCtx(transport.KindKCP, 7, "", b64(encodeTicket(t, "p-a", "b-1", time.Minute)))

	id, err := resolver.Resolve(ctx, relay.RouteEntry{})
	if err != nil {
		t.Fatalf("进行中对局被拒: %v", err)
	}
	if id.PlayerID != "p-a" || id.BattleID != "b-1" {
		t.Fatalf("身份 = %+v，期望 {p-a b-1}", id)
	}
	recorded, replayed := conn.snapshot()
	if len(recorded) != 1 || len(replayed) != 0 {
		t.Fatalf("进行中对局登记/补投次数 = %d/%d，期望 1/0", len(recorded), len(replayed))
	}
}
