// 属主变更守卫用例：目录属主变化 → 该局全部流被请求拆除；属主未变（租约续期重写记录）不拆；
// 该局最后一条流结束后订阅被注销（共享监听留给其他局，只在适配层 Close/空闲超时时关闭）。
//
// 监听链路上不放假替身：守卫注入的是**真适配层**（resolver.LocatorDirectory）＋阻塞型目录
// （testlocator，只有 Stop 才能唤醒 Next）。因此「注销有没有真正摘掉消费者、Close 有没有
// 停掉 watcher」由本用例断言——只 close 停止信号而不 Stop 的实现在这里必然超时。
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

// newGuardWith 用给定目录替身装配「真适配层 + 阻塞型目录」的守卫（适配层一并交回）。
func newGuardWith(loc *testlocator.Locator) (*OwnerGuard, *resolver.LocatorDirectory, error) {
	dir, err := resolver.NewLocatorDirectory(loc)
	if err != nil {
		return nil, nil, err
	}
	g, err := New(dir, nil)
	if err != nil {
		dir.Close()
		return nil, nil, err
	}
	return g, dir, nil
}

// newGuardWithDir 构造守卫并把适配层一并交回：生命周期用例需要主动停共享监听。
func newGuardWithDir(t *testing.T) (*OwnerGuard, *resolver.LocatorDirectory, *testlocator.Locator) {
	t.Helper()
	loc := &testlocator.Locator{}
	g, dir, err := newGuardWith(loc)
	if err != nil {
		t.Fatalf("装配守卫: %v", err)
	}
	t.Cleanup(func() {
		g.Close()
		dir.Close()
	})
	return g, dir, loc
}

// newGuard 构造守卫并返回目录替身，供断言订阅与停止。
func newGuard(t *testing.T) (*OwnerGuard, *testlocator.Locator) {
	t.Helper()
	g, _, loc := newGuardWithDir(t)
	return g, loc
}

// requireNoEvicts 断言一段时间内没有新的拆流请求（注销后不得再回调）。
func requireNoEvicts(t *testing.T, r *recorder) {
	t.Helper()
	time.Sleep(20 * time.Millisecond)
	if got := r.count(); got != 0 {
		t.Fatalf("注销后不应再拆流，实际 %d 次", got)
	}
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
	if got := loc.Prefix(0); got != "battle:" {
		t.Fatalf("共享监听应是 PID 类型前缀级（battle:），实际 %q", got)
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

// TestUnregisterDropsSubscription 验证该局最后一条流结束后订阅被注销：不再收到事件、不重复
// 注销；共享监听不按局关闭（其他局可能还在用），由适配层 Close 统一停掉。
func TestUnregisterDropsSubscription(t *testing.T) {
	g, dir, loc := newGuardWithDir(t)
	rec := &recorder{}
	un := g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, rec.evict)
	un()
	un()                                                      // 幂等：不得二次注销/Stop（etcd 实现重复 Stop 会 panic）
	loc.Watcher(0).Emit(ownerEvent(t, "battle:b1", "node-b")) // 已无订阅者：不应回调、不应 panic
	requireNoEvicts(t, rec)
	if n := loc.Watcher(0).Stops(); n != 0 {
		t.Fatalf("单局注销不应停掉共享监听（其他局还在用），实际 Stop %d 次", n)
	}

	dir.Close() // 适配层停机才停共享监听（另一个时机是空闲超时）
	assertWatchStopped(t, loc, 0)
	if n := loc.Watcher(0).Stops(); n != 1 {
		t.Fatalf("共享监听应恰好停止一次，实际 %d", n)
	}
}

// TestCloseDropsSubscriptions 验证守卫停机（Close）注销全部局的订阅：不再回调；共享监听
// 由适配层 Close 停止（接入层停机时装配层按此顺序调用）。
func TestCloseDropsSubscriptions(t *testing.T) {
	g, dir, loc := newGuardWithDir(t)
	a, b := &recorder{}, &recorder{}
	g.Register(edge.StreamInfo{ID: "s-1", BattleID: "b1", Owner: "node-a"}, a.evict)
	g.Register(edge.StreamInfo{ID: "s-2", BattleID: "b2", Owner: "node-a"}, b.evict)
	if n := loc.Count(); n != 1 {
		t.Fatalf("两个局应共用 1 条监听（进程内单 watcher），实际 %d", n)
	}
	g.Close()
	loc.Watcher(0).Emit(ownerEvent(t, "battle:b1", "node-b"), ownerEvent(t, "battle:b2", "node-b"))
	requireNoEvicts(t, a)
	requireNoEvicts(t, b)

	dir.Close()
	assertWatchStopped(t, loc, 0)
}

// TestWatchFailureKeepsStream 验证监听启动失败不阻断接入层（只记日志、流照常工作）。
func TestWatchFailureKeepsStream(t *testing.T) {
	g, dir, err := newGuardWith(&testlocator.Locator{Err: errors.New("etcd 不可用")})
	if err != nil {
		t.Fatalf("装配守卫: %v", err)
	}
	t.Cleanup(func() {
		g.Close()
		dir.Close()
	})
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
