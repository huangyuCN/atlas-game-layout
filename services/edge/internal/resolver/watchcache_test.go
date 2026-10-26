package resolver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/metricstest"
	"github.com/huangyuCN/atlas/contrib/edge"
)

// stubClock 是可控时钟：watch 缓存的「有效期兜底」与空闲回收判定共用它。
type stubClock struct {
	mu sync.Mutex
	t  time.Time
}

// newStubClock 构造从固定时刻起步的可控时钟。
func newStubClock() *stubClock { return &stubClock{t: time.Unix(1700000000, 0)} }

// now 返回当前时刻。
func (c *stubClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// advance 推进时钟。
func (c *stubClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// newTestResolver 构造一个接了本地缓存的 Resolver，并在用例结束时 Close（停掉全部监听）。
func newTestResolver(t *testing.T, dir Directory, frames FrameRegistry, opts Options) *Resolver {
	t.Helper()
	r, err := New(dir, frames, opts)
	if err != nil {
		t.Fatalf("构造 Resolver 失败: %v", err)
	}
	t.Cleanup(r.Close)
	return r
}

// frameInstance 构造一个帧面实例（node 上承载 ws/kcp/udp 三面）。
func frameInstance(id, node, host string) Instance {
	return Instance{
		ID: id, NodeID: node, Host: host,
		Ports: map[string]string{"ws": "9401", "kcp": "9402", "udp": "9403"},
	}
}

// TestResolveReusesLocalCacheForRepeatedBattle 是「解析几乎不产生网络往返」的主用例：
// 同一局连续 1000 次解析只允许各查一次目录与一次帧面实例（旧实现每次解析两跳，共 2000 次）。
func TestResolveReusesLocalCacheForRepeatedBattle(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	frames := &stubFrames{instances: []Instance{frameInstance("f1", "node-1", "10.0.0.7")}}
	r := newTestResolver(t, dir, frames, Options{})

	const rounds = 1000
	for i := 0; i < rounds; i++ {
		backend, err := r.Resolve(context.Background(), resolveReq("b-1", "ws"))
		if err != nil {
			t.Fatalf("第 %d 次解析失败: %v", i, err)
		}
		if backend.Address != "10.0.0.7:9401" || backend.Owner != "node-1" {
			t.Fatalf("第 %d 次解析结果不对: %+v", i, backend)
		}
	}
	if got := dir.callCount(); got != 1 {
		t.Fatalf("同一局 %d 次解析应只查 1 次目录（watch 缓存命中），实际 %d 次", rounds, got)
	}
	if got := frames.callCount(); got != 1 {
		t.Fatalf("同一局 %d 次解析应只查 1 次帧面实例（watch 缓存命中），实际 %d 次", rounds, got)
	}
	t.Logf("%d 次解析的网络往返：目录 %d 次 + 实例 %d 次 = 2 次（旧实现每次两跳 = %d 次）",
		rounds, dir.callCount(), frames.callCount(), 2*rounds)
}

// TestResolveUsesWatchUpdatedOwner 覆盖属主变更语义：目录 watch 报出新属主后，下一次解析必须
// **立刻**用新属主（而不是等 TTL 过期、也不是等目录查询刷新）。
func TestResolveUsesWatchUpdatedOwner(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	frames := &stubFrames{instances: []Instance{
		frameInstance("f1", "node-1", "10.0.0.1"),
		frameInstance("f2", "node-2", "10.0.0.2"),
	}}
	r := newTestResolver(t, dir, frames, Options{})
	if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err != nil {
		t.Fatalf("首次解析失败: %v", err)
	}

	dir.emitOwner("battle:b-1", "node-2")
	backend, err := r.Resolve(context.Background(), resolveReq("b-1", "ws"))
	if err != nil {
		t.Fatalf("属主变更后解析失败: %v", err)
	}
	if backend.Address != "10.0.0.2:9401" || backend.Owner != "node-2" {
		t.Fatalf("watch 报新属主后应立刻用 node-2，实际 %+v", backend)
	}
	if got := dir.callCount(); got != 1 {
		t.Fatalf("属主由 watch 更新后不应再查目录，实际 %d 次", got)
	}
}

// TestResolveFallsBackWhenWatchUnavailable 覆盖保守兜底：目录/注册中心监听建立失败时**不得缓存**，
// 每次解析都退回原来的两跳查询并且成功（缓存是增强能力，不得成为新的失败路径）。
func TestResolveFallsBackWhenWatchUnavailable(t *testing.T) {
	dir := &stubDirectory{
		owners:   map[string]string{"battle:b-1": "node-1"},
		watchErr: errors.New("etcd 不可用"),
	}
	frames := &stubFrames{
		instances: []Instance{frameInstance("f1", "node-1", "10.0.0.7")},
		watchErr:  errors.New("注册中心不可用"),
	}
	r := newTestResolver(t, dir, frames, Options{})

	const rounds = 3
	for i := 0; i < rounds; i++ {
		if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err != nil {
			t.Fatalf("监听不可用时第 %d 次解析失败（必须退回两跳查询且成功）: %v", i, err)
		}
	}
	if got := dir.callCount(); got != rounds {
		t.Fatalf("监听不可用时应每次查目录（不缓存），期望 %d 次，实际 %d 次", rounds, got)
	}
	if got := frames.callCount(); got != rounds {
		t.Fatalf("监听不可用时应每次查帧面实例（不缓存），期望 %d 次，实际 %d 次", rounds, got)
	}
}

// TestResolveRecoversFromFirstDirectoryError 覆盖「监听已建、但首次目录查询失败」的恢复：
// 条目里没有可用属主时不得当成命中（空属主不是答案），下一次解析必须重新查目录并成功。
func TestResolveRecoversFromFirstDirectoryError(t *testing.T) {
	dir := &stubDirectory{
		owners: map[string]string{"battle:b-1": "node-1"},
		err:    errors.New("etcd 抖了一下"),
	}
	frames := &stubFrames{instances: []Instance{frameInstance("f1", "node-1", "10.0.0.7")}}
	r := newTestResolver(t, dir, frames, Options{})

	if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err == nil {
		t.Fatal("目录查询失败时应拒绝，实际成功")
	}
	dir.setErr(nil)
	backend, err := r.Resolve(context.Background(), resolveReq("b-1", "ws"))
	if err != nil {
		t.Fatalf("目录恢复后解析应成功: %v", err)
	}
	if backend.Address != "10.0.0.7:9401" || backend.Owner != "node-1" {
		t.Fatalf("目录恢复后应解析到 node-1，实际 %+v", backend)
	}
}

// TestResolveStaleOwnerFallsBackToDirectory 覆盖有效期兜底：watch 长时间静默（可能已断开）时
// 条目按过期处理、重新查目录，并用目录的**当前**属主（旧实现最多错 TTL，新实现不能无限错）。
func TestResolveStaleOwnerFallsBackToDirectory(t *testing.T) {
	clock := newStubClock()
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	frames := &stubFrames{instances: []Instance{
		frameInstance("f1", "node-1", "10.0.0.1"),
		frameInstance("f2", "node-2", "10.0.0.2"),
	}}
	r := newTestResolver(t, dir, frames, Options{})
	r.now = clock.now
	if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err != nil {
		t.Fatalf("首次解析失败: %v", err)
	}

	// watch 静默且目录已换属主（事件丢失）：超过有效期后必须重查，不能用旧属主。
	clock.advance(DefaultOwnerFreshTTL + time.Second)
	dir.setOwner("battle:b-1", "node-2")
	backend, err := r.Resolve(context.Background(), resolveReq("b-1", "ws"))
	if err != nil {
		t.Fatalf("过期后解析失败: %v", err)
	}
	if backend.Owner != "node-2" {
		t.Fatalf("缓存过期后应重新查目录拿到新属主 node-2，实际 %q", backend.Owner)
	}
	if got := dir.callCount(); got != 2 {
		t.Fatalf("缓存过期后应再查一次目录，实际累计 %d 次", got)
	}
}

// TestResolveOwnerDeletedByWatchDropsEntry 覆盖目录记录消失（对局结束、迁移 drain 与 activate
// 之间的窗口）：watch 报空属主必须丢掉条目，下一次解析重新查目录，而不是继续用旧属主。
func TestResolveOwnerDeletedByWatchDropsEntry(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	frames := &stubFrames{instances: []Instance{frameInstance("f1", "node-1", "10.0.0.1")}}
	r := newTestResolver(t, dir, frames, Options{})
	if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err != nil {
		t.Fatalf("首次解析失败: %v", err)
	}

	dir.emitOwner("battle:b-1", "")
	if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err != nil {
		t.Fatalf("记录删除后解析失败: %v", err)
	}
	if got := dir.callCount(); got != 2 {
		t.Fatalf("记录删除后必须重查目录（条目已作废），实际累计 %d 次", got)
	}
}

// TestResolveUsesWatchUpdatedFrameAddress 覆盖帧面实例上下线：注册中心 watch 推送新地址后，
// 下一次解析立刻用新地址，且不再查询实例列表。
func TestResolveUsesWatchUpdatedFrameAddress(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	frames := &stubFrames{instances: []Instance{frameInstance("f1", "node-1", "10.0.0.1")}}
	r := newTestResolver(t, dir, frames, Options{})
	if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err != nil {
		t.Fatalf("首次解析失败: %v", err)
	}

	frames.emitInstances([]Instance{frameInstance("f1", "node-1", "10.9.9.9")}) // 节点换地址/端口
	backend, err := r.Resolve(context.Background(), resolveReq("b-1", "ws"))
	if err != nil {
		t.Fatalf("实例变化后解析失败: %v", err)
	}
	if backend.Address != "10.9.9.9:9401" {
		t.Fatalf("实例 watch 更新后应用新地址，实际 %s", backend.Address)
	}
	if got := frames.callCount(); got != 1 {
		t.Fatalf("实例由 watch 更新后不应再查询实例列表，实际累计 %d 次", got)
	}
}

// TestResolveRecordsCacheHitMetrics 覆盖命中率口径：冷启动记 miss、命中记 hit、条目过期记 stale。
func TestResolveRecordsCacheHitMetrics(t *testing.T) {
	clock := newStubClock()
	rec := metricstest.New()
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	frames := &stubFrames{instances: []Instance{frameInstance("f1", "node-1", "10.0.0.7")}}
	r := newTestResolver(t, dir, frames, Options{Metrics: rec})
	r.now = clock.now

	record := func(what string) {
		t.Helper()
		if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err != nil {
			t.Fatalf("%s解析失败: %v", what, err)
		}
	}
	record("冷启动") // miss：两层都没条目
	record("命中")  // hit：两层都命中
	clock.advance(DefaultOwnerFreshTTL + time.Second)
	record("过期") // stale：条目在但已过期

	for _, tc := range []struct {
		result string
		want   float64
	}{
		{edge.ResolveCacheHit, 1},
		{edge.ResolveCacheMiss, 1},
		{edge.ResolveCacheStale, 1},
	} {
		if got := rec.CounterValue(edge.MetricResolveCacheTotal, edge.LabelResult, tc.result); got != tc.want {
			t.Fatalf("%s{result=%s} 应为 %v，实际 %v",
				edge.MetricResolveCacheTotal, tc.result, tc.want, got)
		}
	}
}

// TestResolverCloseCancelsWatches 覆盖生命周期：Close 必须取消每局的目录监听与唯一的实例监听，
// 否则每局泄漏一条 etcd watch + 一个监听 goroutine。
func TestResolverCloseCancelsWatches(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{
		"battle:b-1": "node-1",
		"battle:b-2": "node-1",
	}}
	frames := &stubFrames{instances: []Instance{frameInstance("f1", "node-1", "10.0.0.7")}}
	r := newTestResolver(t, dir, frames, Options{})
	for _, battle := range []string{"b-1", "b-2"} {
		if _, err := r.Resolve(context.Background(), resolveReq(battle, "ws")); err != nil {
			t.Fatalf("解析 %s 失败: %v", battle, err)
		}
	}
	if got := dir.watchCount(); got != 2 {
		t.Fatalf("每局应各建一条目录监听，实际 %d 条", got)
	}
	if got := frames.watchCount(); got != 1 {
		t.Fatalf("实例监听应全局唯一（只建一条），实际 %d 条", got)
	}

	r.Close()
	if got := dir.cancelCount("battle:b-1") + dir.cancelCount("battle:b-2"); got != 2 {
		t.Fatalf("Close 应取消两条目录监听，实际 %d 条", got)
	}
	if got := frames.cancelCount(); got != 1 {
		t.Fatalf("Close 应取消实例监听，实际 %d 次", got)
	}
	r.Close() // 幂等：重复 Close 不得 panic、不得重复取消
	if got := frames.cancelCount(); got != 1 {
		t.Fatalf("重复 Close 不应再次取消监听，实际 %d 次", got)
	}
}

// TestResolverNotifiesOwnerChange 覆盖与接入层的联动：登记的变化回调必须收到属主变更的 battle_id
// （接入层据此清掉该局的 TTL 条目，见 edge.ResolveChangeNotifier）。
func TestResolverNotifiesOwnerChange(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	frames := &stubFrames{instances: []Instance{
		frameInstance("f1", "node-1", "10.0.0.1"),
		frameInstance("f2", "node-2", "10.0.0.2"),
	}}
	r := newTestResolver(t, dir, frames, Options{})

	var mu sync.Mutex
	var notified []string
	unregister := r.OnResolveChanged(func(battleID string) {
		mu.Lock()
		notified = append(notified, battleID)
		mu.Unlock()
	})
	if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err != nil {
		t.Fatalf("首次解析失败: %v", err)
	}
	dir.emitOwner("battle:b-1", "node-2") // 属主变更 → 通知接入层
	unregister()                          // 注销后不得再回调

	mu.Lock()
	got := append([]string(nil), notified...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "b-1" {
		t.Fatalf("应恰好通知一次 battle=b-1，实际 %v", got)
	}
}
