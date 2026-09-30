// 属主变更守卫用例：目录属主变化 → 该局全部流被请求拆除；属主未变（租约续期重写记录）不拆；
// 该局最后一条流结束后监听被回收。
package guard

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/huangyuCN/atlas/contrib/edge"
)

// fakeWatcher 是属主监听假件：记录订阅的 PID，供测试手动触发属主变化。
type fakeWatcher struct {
	mu       sync.Mutex
	pids     []string
	onChange func(owner string)
	canceled int
	err      error
}

// WatchOwner 实现 OwnerWatcher。
func (w *fakeWatcher) WatchOwner(_ context.Context, pid string, onChange func(owner string)) (func(), error) {
	if w.err != nil {
		return nil, w.err
	}
	w.mu.Lock()
	w.pids = append(w.pids, pid)
	w.onChange = onChange
	w.mu.Unlock()
	return func() {
		w.mu.Lock()
		w.canceled++
		w.mu.Unlock()
	}, nil
}

// fire 触发一次属主变化回调。
func (w *fakeWatcher) fire(owner string) {
	w.mu.Lock()
	fn := w.onChange
	w.mu.Unlock()
	if fn != nil {
		fn(owner)
	}
}

// watched 返回已订阅的 PID 列表。
func (w *fakeWatcher) watched() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.pids...)
}

// recorder 记录一次拆流请求。
type recorder struct {
	mu     sync.Mutex
	evicts []edge.TeardownReason
}

// evict 记录拆流请求。
func (r *recorder) evict(reason edge.TeardownReason) {
	r.mu.Lock()
	r.evicts = append(r.evicts, reason)
	r.mu.Unlock()
}

// count 返回拆流请求次数。
func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.evicts)
}

// newGuard 构造带假监听的守卫。
func newGuard(t *testing.T) (*OwnerGuard, *fakeWatcher) {
	t.Helper()
	w := &fakeWatcher{}
	g, err := New(w, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(g.Close)
	return g, w
}

// TestOwnerChangeEvictsStreams 验证属主变化拆除该局全部流（同局多条流共享一条监听）。
func TestOwnerChangeEvictsStreams(t *testing.T) {
	g, w := newGuard(t)
	a, b := &recorder{}, &recorder{}
	unA := g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, a.evict)
	unB := g.Register(edge.StreamInfo{ID: "s-2", BattleID: "b1", Owner: "node-a"}, b.evict)
	defer unA()
	defer unB()
	if pids := w.watched(); len(pids) != 1 || pids[0] != "battle:b1" {
		t.Fatalf("同局应只建一条监听，实际 %v", pids)
	}

	w.fire("node-a") // 租约续期重写记录：属主未变
	if a.count() != 0 || b.count() != 0 {
		t.Fatal("属主未变不应拆流")
	}
	w.fire("node-b")
	if a.count() != 1 || b.count() != 1 {
		t.Fatalf("属主变更应拆除该局全部流，实际 %d/%d", a.count(), b.count())
	}
	if got := a.evicts[0]; got != edge.TeardownOwnerChanged {
		t.Fatalf("拆流原因应为 %s，实际 %s", edge.TeardownOwnerChanged, got)
	}
}

// TestOwnerRecordGoneEvicts 验证属主记录消失（drain 与 activate 之间的窗口）同样拆流：
// 此时旧节点已不持有该局，留着流客户端会一直收不到帧广播。
func TestOwnerRecordGoneEvicts(t *testing.T) {
	g, w := newGuard(t)
	rec := &recorder{}
	defer g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, rec.evict)()
	w.fire("")
	if rec.count() != 1 {
		t.Fatalf("属主记录消失应拆流，实际 %d", rec.count())
	}
}

// TestUnregisterStopsWatch 验证该局最后一条流结束后停止监听（不留后台 watch）。
func TestUnregisterStopsWatch(t *testing.T) {
	g, w := newGuard(t)
	un := g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, (&recorder{}).evict)
	un()
	un() // 幂等
	if w.canceled != 1 {
		t.Fatalf("最后一条流结束后应停止监听一次，实际 %d", w.canceled)
	}
	w.fire("node-b") // 已无订阅者：不应 panic
}

// TestWatchFailureKeepsStream 验证监听启动失败不阻断接入层（只记日志、流照常工作）。
func TestWatchFailureKeepsStream(t *testing.T) {
	w := &fakeWatcher{err: errors.New("etcd 不可用")}
	g, err := New(w, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(g.Close)
	rec := &recorder{}
	un := g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, rec.evict)
	un()
	if rec.count() != 0 {
		t.Fatal("无监听时不应产生拆流请求")
	}
}

// TestNewRequiresWatcher 验证装配期依赖缺失即失败（不静默降级为「永不拆流」）。
func TestNewRequiresWatcher(t *testing.T) {
	if _, err := New(nil, nil); err == nil {
		t.Fatal("缺少属主监听端口时应装配失败")
	}
}

// TestRegisterWithoutBattleIsNoop 验证非战斗流（无 battle_id）不建监听。
func TestRegisterWithoutBattleIsNoop(t *testing.T) {
	g, w := newGuard(t)
	un := g.Register(edge.StreamInfo{ID: "s-1", BattleID: ""}, (&recorder{}).evict)
	un()
	if len(w.watched()) != 0 {
		t.Fatalf("无 battle_id 的流不应建监听，实际 %v", w.watched())
	}
}
