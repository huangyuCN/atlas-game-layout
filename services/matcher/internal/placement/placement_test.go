package placement

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector"
	"github.com/huangyuCN/atlas/selector/filter"
	"github.com/huangyuCN/atlas/selector/wrr"
)

// errNoWatch 表示测试桩不支持 Watch（选点只走 GetService，无需监听）。
var errNoWatch = errors.New("placement: 测试桩不支持 Watch")

// fakeDiscovery 是注册发现的测试桩：固定实例列表 + 可注入错误 + 调用计数。
type fakeDiscovery struct {
	mu    sync.Mutex
	ins   []*registry.ServiceInstance
	err   error
	calls int
}

// GetService 返回固定实例列表（计数用于断言候选快照复用）。
func (d *fakeDiscovery) GetService(context.Context, string) ([]*registry.ServiceInstance, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.err != nil {
		return nil, d.err
	}
	return append([]*registry.ServiceInstance(nil), d.ins...), nil
}

// Watch 不支持（选点不做长连监听）。
func (d *fakeDiscovery) Watch(context.Context, string) (registry.Watcher, error) {
	return nil, errNoWatch
}

// callCount 返回 GetService 被调用次数。
func (d *fakeDiscovery) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// setErr 注入/清除发现错误。
func (d *fakeDiscovery) setErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.err = err
}

// battleInstance 构造一条 battle 注册实例：端点故意把 edge 面排在前面，
// 用来钉住「选点取 grpc 面端点」的口径（与拨号侧 WithEndpointScheme(SchemeGRPC) 同源）。
func battleInstance(id, addr string) *registry.ServiceInstance {
	return &registry.ServiceInstance{
		ID:        id,
		Name:      consts.ServiceBattle,
		Version:   "v1",
		Endpoints: []string{"grpc-edge://" + addr + ":1", "grpc://" + addr},
	}
}

// newTestPlanner 构造带本机优先（127.0.0.1）的选点器。
func newTestPlanner(disc registry.Discovery) *Planner {
	return New(disc, Options{Local: filter.PreferLocal("127.0.0.1")})
}

// placeN 连续选 n 次并按地址统计（分布断言用）。
func placeN(t *testing.T, p *Planner, n int) map[string]int {
	t.Helper()
	out := make(map[string]int, 4)
	for i := 0; i < n; i++ {
		addr := p.Place(context.Background())
		if addr == "" {
			t.Fatalf("第 %d 次选点为空（有候选时不得返回空）", i)
		}
		out[addr]++
	}
	return out
}

// assertEven 断言两台候选的分配偏差不超过 10%（100 次 ⇒ 45–55）。
func assertEven(t *testing.T, counts map[string]int, total int) {
	t.Helper()
	if len(counts) != 2 {
		t.Fatalf("应两台都被选中，实际分布 %v", counts)
	}
	lo, hi := total*45/100, total*55/100
	for addr, n := range counts {
		if n < lo || n > hi {
			t.Fatalf("%s 分到 %d 次，超出 [%d,%d]（分布 %v，偏差 >10%%）", addr, n, lo, hi, counts)
		}
	}
}

// TestPlannerSplitsEvenlyAcrossLocalBattles 是本用例组的核心：
// 同主机两台 battle（LOCAL_FIRST 命中多台本机）时必须**均匀分摊**，
// 旧实现没有「每局一次选点」，全靠每 RPC 各自负载均衡 ⇒ 同一局的两跳必然劈到两台
// （预热白做、属主由两跳竞态决定），此处钉住「100 次选点接近 50/50」。
func TestPlannerSplitsEvenlyAcrossLocalBattles(t *testing.T) {
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance("battle-1", "127.0.0.1:9301"),
		battleInstance("battle-b2", "127.0.0.1:9311"),
		battleInstance("battle-remote", "10.0.0.9:9301"),
	}}
	counts := placeN(t, newTestPlanner(disc), 100)
	t.Logf("选 100 次分布（两台本机 + 一台跨机）: %v", counts)
	if counts["10.0.0.9:9301"] != 0 {
		t.Fatalf("本机有候选时不得选跨机节点，分布 %v", counts)
	}
	assertEven(t, counts, 100)
}

// TestPlannerPicksSoleLocalBattle 验证只有一台本机 battle 时**全选它**（不跨机）。
func TestPlannerPicksSoleLocalBattle(t *testing.T) {
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance("battle-remote-a", "10.0.0.8:9301"),
		battleInstance("battle-1", "127.0.0.1:9301"),
		battleInstance("battle-remote-b", "10.0.0.9:9301"),
	}}
	counts := placeN(t, newTestPlanner(disc), 100)
	if counts["127.0.0.1:9301"] != 100 {
		t.Fatalf("唯一本机候选应独占 100 次，分布 %v", counts)
	}
}

// TestPlannerFallsBackToRemoteBattles 验证本机无候选时回落跨机，
// 且**跨机候选之间同样均匀分摊**（兜底不得退化成「恒选第一台」）。
func TestPlannerFallsBackToRemoteBattles(t *testing.T) {
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance("battle-a", "10.0.0.8:9301"),
		battleInstance("battle-b", "10.0.0.9:9301"),
	}}
	assertEven(t, placeN(t, newTestPlanner(disc), 100), 100)
}

// TestPlannerConcurrentPlaceIsThreadSafe 验证并发选点线程安全（配合 -race）：
// 800 个并发选点不得丢次数，且分配仍接近 50/50。
func TestPlannerConcurrentPlaceIsThreadSafe(t *testing.T) {
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance("battle-1", "127.0.0.1:9301"),
		battleInstance("battle-b2", "127.0.0.1:9311"),
	}}
	p := New(disc, Options{Local: filter.PreferLocal("127.0.0.1"), CacheTTL: time.Nanosecond})
	const total = 800
	var mu sync.Mutex
	counts := make(map[string]int, 2)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr := p.Place(context.Background())
			mu.Lock()
			counts[addr]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if got := counts["127.0.0.1:9301"] + counts["127.0.0.1:9311"]; got != total {
		t.Fatalf("并发选点丢次数：%d != %d（分布 %v）", got, total, counts)
	}
	assertEven(t, counts, total)
}

// TestPlannerCachesCandidates 验证候选快照按 TTL 复用：压测下单 tick 可成上百局，
// 逐局查注册中心会把成局速率绑到 etcd 的 QPS 上。
func TestPlannerCachesCandidates(t *testing.T) {
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance("battle-1", "127.0.0.1:9301"),
		battleInstance("battle-b2", "127.0.0.1:9311"),
	}}
	p := newTestPlanner(disc)
	placeN(t, p, 100)
	if n := disc.callCount(); n != 1 {
		t.Fatalf("100 次选点应只查 1 次注册中心（快照复用），实际 %d 次", n)
	}
}

// TestPlannerFailsSoftOnDiscoveryError 验证发现失败**不阻塞成局**：
// 选点返回空串（调用方据此退回既有每 RPC 负载均衡），不返回错误、不 panic。
func TestPlannerFailsSoftOnDiscoveryError(t *testing.T) {
	disc := &fakeDiscovery{err: errors.New("etcd 不可用")}
	if addr := newTestPlanner(disc).Place(context.Background()); addr != "" {
		t.Fatalf("发现失败时应返回空串（不钉选点），实际 %q", addr)
	}
}

// TestPlannerSkipsInstancesWithoutGRPCEndpoint 验证没有 grpc 面端点的实例不参与选点
// （例如只注册了 edge 面/帧面端口的实例），且全部无端点时返回空串。
func TestPlannerSkipsInstancesWithoutGRPCEndpoint(t *testing.T) {
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		{ID: "edge-only", Name: consts.ServiceBattle, Endpoints: []string{"grpc-edge://127.0.0.1:9300"}},
		{ID: "no-endpoint", Name: consts.ServiceBattle},
		battleInstance("battle-1", "127.0.0.1:9301"),
	}}
	counts := placeN(t, newTestPlanner(disc), 10)
	if counts["127.0.0.1:9301"] != 10 {
		t.Fatalf("只有带 grpc 面端点的实例可被选中，分布 %v", counts)
	}
	empty := &fakeDiscovery{ins: []*registry.ServiceInstance{
		{ID: "edge-only", Name: consts.ServiceBattle, Endpoints: []string{"grpc-edge://127.0.0.1:9300"}},
	}}
	if addr := newTestPlanner(empty).Place(context.Background()); addr != "" {
		t.Fatalf("无可用实例时应返回空串，实际 %q", addr)
	}
}

// TestLegacyPerRPCPickSplitsAGameAcrossBattles 记录**旧行为**（取点由每 RPC 的均衡器各自决定）：
// 两台等权本机 battle 下，同一局的两次 battle 调用（预热 Create + 入场 Admit）
// 必然落到**不同**进程。这正是「预热总是白做、属主靠两跳竞态、偏载时全挤一台」的机理，
// 也是本包存在的理由：选点必须是**每局一次决定**，不能是每 RPC 一次。
func TestLegacyPerRPCPickSplitsAGameAcrossBattles(t *testing.T) {
	sel := defaultSelectorFor(t)
	ctx := context.Background()
	split := 0
	for i := 0; i < 100; i++ {
		warm, _, err := sel.Select(ctx)
		if err != nil {
			t.Fatalf("预热选点失败: %v", err)
		}
		admit, _, err := sel.Select(ctx)
		if err != nil {
			t.Fatalf("入场选点失败: %v", err)
		}
		if warm.Address() != admit.Address() {
			split++
		}
	}
	t.Logf("旧行为（每 RPC 各自均衡）：100 局中 %d 局的预热与入场落在不同 battle", split)
	if split != 100 {
		t.Fatalf("旧行为下同一局两跳应 100/100 落到不同 battle（记录偏载机理），实际 %d", split)
	}
}

// defaultSelectorFor 返回框架缺省均衡器（wrr）装了两台等权本机 battle 的选择器。
func defaultSelectorFor(t *testing.T) selector.Selector {
	t.Helper()
	sel := wrr.NewBuilder().Build()
	sel.Apply([]selector.Node{
		selector.NewNode("grpc", "127.0.0.1:9301", &registry.ServiceInstance{ID: "battle-1"}),
		selector.NewNode("grpc", "127.0.0.1:9311", &registry.ServiceInstance{ID: "battle-b2"}),
	})
	return sel
}
