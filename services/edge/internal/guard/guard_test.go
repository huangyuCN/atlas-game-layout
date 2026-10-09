// 属主变更守卫用例：目录属主变化 → 该局全部流被请求拆除；属主未变（租约续期重写记录）不拆；
// 该局最后一条流结束后监听被回收。
//
// 监听链路上不放假替身：守卫注入的是**真适配层**（resolver.LocatorDirectory）＋阻塞型目录
// （testlocator，只有 Stop 才能唤醒 Next）。因此「cancel 有没有真正停掉 watcher」由本用例
// 断言——只 close 停止信号而不 Stop 的实现在这里必然超时（评审 P1-1：每局泄漏 1 watch + 1 goroutine）。
package guard

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/resolver"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/testlocator"
	"github.com/huangyuCN/atlas/contrib/actor/proto/actorv1"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/locator"
)

// battleKey 返回目录事件的键（与适配层按 PID 派生的键同源）。
func battleKey(t *testing.T, pid string) locator.Key {
	t.Helper()
	parsed, err := types.ParsePID(pid)
	if err != nil {
		t.Fatalf("ParsePID(%s): %v", pid, err)
	}
	return parsed.Key()
}

// ownerEvent 构造一次「属主记录被重写为 owner」的目录事件。
func ownerEvent(t *testing.T, pid, owner string) locator.WatchEvent {
	t.Helper()
	return locator.WatchEvent{
		Key: battleKey(t, pid),
		Loc: &types.ActorLocation{Location: &actorv1.Location{OwnerNode: owner}},
	}
}

// goneEvent 构造一次「属主记录被删除」的目录事件（Loc 为 nil）。
func goneEvent(t *testing.T, pid string) locator.WatchEvent {
	t.Helper()
	return locator.WatchEvent{Key: battleKey(t, pid)}
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

// reason 返回第 i 次拆流的原因（越界返回空串）。
func (r *recorder) reason(i int) edge.TeardownReason {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i < 0 || i >= len(r.evicts) {
		return ""
	}
	return r.evicts[i]
}

// awaitEvicts 等到该记录器收到 want 次拆流请求；超时即失败。
func awaitEvicts(t *testing.T, r *recorder, want int) {
	t.Helper()
	deadline := time.Now().Add(testlocator.WaitTimeout)
	for time.Now().Before(deadline) {
		if r.count() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("应收到 %d 次拆流请求，实际 %d", want, r.count())
}

// assertWatchStopped 断言第 i 条监听被真 watcher 停止、且它拿到的 ctx 被取消：
// 前者唤醒阻塞在 Next() 的 goroutine，后者关掉客户端侧 watch 流——缺一即泄漏。
func assertWatchStopped(t *testing.T, loc *testlocator.Locator, i int) {
	t.Helper()
	w := loc.Watcher(i)
	if w == nil {
		t.Fatalf("第 %d 次订阅不存在", i)
	}
	if !w.Stopped(testlocator.WaitTimeout) {
		t.Fatal("真实 watcher 未被 Stop：goroutine 永久阻塞在 Next()（每局泄漏 1 watch + 1 goroutine）")
	}
	ctx := loc.Ctx(i)
	if ctx == nil {
		t.Fatalf("第 %d 次订阅未记录 ctx", i)
	}
	select {
	case <-ctx.Done():
	case <-time.After(testlocator.WaitTimeout):
		t.Fatal("传给 locator.Watch 的 ctx 未被取消：etcd 客户端侧 watch 流不会关闭")
	}
}

// newGuardWith 用给定目录替身装配「真适配层 + 阻塞型目录」的守卫。
func newGuardWith(loc *testlocator.Locator) (*OwnerGuard, *testlocator.Locator, error) {
	dir, err := resolver.NewLocatorDirectory(loc)
	if err != nil {
		return nil, loc, err
	}
	g, err := New(dir, nil)
	return g, loc, err
}

// newGuard 构造守卫并返回目录替身，供断言订阅与停止。
func newGuard(t *testing.T) (*OwnerGuard, *testlocator.Locator) {
	t.Helper()
	g, loc, err := newGuardWith(&testlocator.Locator{})
	if err != nil {
		t.Fatalf("装配守卫: %v", err)
	}
	t.Cleanup(g.Close)
	return g, loc
}

// TestOwnerChangeEvictsStreams 验证属主变化拆除该局全部流（同局多条流共享一条监听）。
func TestOwnerChangeEvictsStreams(t *testing.T) {
	g, loc := newGuard(t)
	a, b := &recorder{}, &recorder{}
	unA := g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, a.evict)
	unB := g.Register(edge.StreamInfo{ID: "s-2", BattleID: "b1", Owner: "node-a"}, b.evict)
	defer unA()
	defer unB()
	if n := loc.Count(); n != 1 {
		t.Fatalf("同局应只建一条监听，实际 %d", n)
	}
	if got := loc.Prefix(0); got != "battle:b1" {
		t.Fatalf("订阅前缀应为 battle:b1，实际 %q", got)
	}
	// 三条事件由同一条监听 goroutine 顺序处理：第三条的回调发生时前两条必已处理完，
	// 故「各流恰好拆一次」可判定地证明「属主未变不拆 + 前缀相同的别局不拆」。
	w := loc.Watcher(0)
	w.Emit(ownerEvent(t, "battle:b1", "node-a"))  // 租约续期重写记录：属主未变
	w.Emit(ownerEvent(t, "battle:b10", "node-b")) // 前缀相同的别局：必须按 key 精确过滤
	w.Emit(ownerEvent(t, "battle:b1", "node-b"))  // 属主变更：应拆该局全部流
	awaitEvicts(t, a, 1)
	awaitEvicts(t, b, 1)
	if got := a.reason(0); got != edge.TeardownOwnerChanged {
		t.Fatalf("拆流原因应为 %s，实际 %s", edge.TeardownOwnerChanged, got)
	}
	if a.count() != 1 || b.count() != 1 {
		t.Fatalf("该局每条流只应拆一次，实际 %d/%d", a.count(), b.count())
	}
}

// TestOwnerRecordGoneEvicts 验证属主记录消失（drain 与 activate 之间的窗口）同样拆流：
// 此时旧节点已不持有该局，留着流客户端会一直收不到帧广播。
func TestOwnerRecordGoneEvicts(t *testing.T) {
	g, loc := newGuard(t)
	rec := &recorder{}
	defer g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, rec.evict)()
	loc.Watcher(0).Emit(goneEvent(t, "battle:b1"))
	awaitEvicts(t, rec, 1)
}

// TestUnregisterStopsWatch 验证该局最后一条流结束后监听被真 watcher 停止（不留后台 watch）。
func TestUnregisterStopsWatch(t *testing.T) {
	g, loc := newGuard(t)
	un := g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, (&recorder{}).evict)
	w := loc.Watcher(0)
	un()
	un() // 幂等：不得二次 Stop（etcd 实现重复 Stop 会 panic）
	assertWatchStopped(t, loc, 0)
	if n := w.Stops(); n != 1 {
		t.Fatalf("监听应恰好停止一次，实际 %d", n)
	}
	w.Emit(ownerEvent(t, "battle:b1", "node-b")) // 已无订阅者：不应 panic
}

// TestCloseStopsWatches 验证停机（Close）停掉全部在监听的局，不等流逐条结束。
func TestCloseStopsWatches(t *testing.T) {
	g, loc := newGuard(t)
	unA := g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, (&recorder{}).evict)
	unB := g.Register(edge.StreamInfo{ID: "s-2", BattleID: "b2", Owner: "node-a"}, (&recorder{}).evict)
	defer unA()
	defer unB()
	g.Close()
	if n := loc.Count(); n != 2 {
		t.Fatalf("两个局应各建一条监听，实际 %d", n)
	}
	for i := 0; i < loc.Count(); i++ {
		assertWatchStopped(t, loc, i)
	}
}

// TestWatchFailureKeepsStream 验证监听启动失败不阻断接入层（只记日志、流照常工作）。
func TestWatchFailureKeepsStream(t *testing.T) {
	g, _, err := newGuardWith(&testlocator.Locator{Err: errors.New("etcd 不可用")})
	if err != nil {
		t.Fatalf("装配守卫: %v", err)
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
	g, loc := newGuard(t)
	un := g.Register(edge.StreamInfo{ID: "s-1", BattleID: ""}, (&recorder{}).evict)
	un()
	if n := loc.Count(); n != 0 {
		t.Fatalf("无 battle_id 的流不应建监听，实际 %d", n)
	}
}
