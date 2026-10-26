package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/placement"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector"
	"github.com/huangyuCN/atlas/selector/filter"
)

// placementNode 构造一个 battle 候选节点（地址即 host:port）。
func placementNode(addr string) selector.Node {
	return selector.NewNode("grpc", addr, &registry.ServiceInstance{ID: addr})
}

// decided 返回过滤器作用后剩下的候选地址（判定选点结果）。
func decided(filters []selector.NodeFilter, nodes []selector.Node) []string {
	out := nodes
	for _, f := range filters {
		out = f(context.Background(), out)
	}
	addrs := make([]string, 0, len(out))
	for _, n := range out {
		addrs = append(addrs, n.Address())
	}
	return addrs
}

// TestBattleNodeFiltersPreferLocal 验证本机可用时只剩本机候选：
// 旧实现没有任何选点过滤器，候选里本机与跨机混在一起由负载均衡随机挑，
// 跨节点懒激活因此成为常态（本用例在旧实现下会全量返回 ⇒ 真红）。
func TestBattleNodeFiltersPreferLocal(t *testing.T) {
	cfg := &conf.Bootstrap{Battle: &conf.Battle{
		Placement:  conf.BattlePlacement_BATTLE_PLACEMENT_LOCAL_FIRST,
		LocalHosts: []string{"127.0.0.1"},
	}}
	filters, err := battleNodeFilters(cfg)
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	got := decided(filters, []selector.Node{
		placementNode("10.0.0.2:9000"),
		placementNode("127.0.0.1:9001"),
		placementNode("10.0.0.3:9000"),
	})
	if len(got) != 1 || got[0] != "127.0.0.1:9001" {
		t.Fatalf("本机可用时应只留本机候选，got %v", got)
	}
}

// TestBattleNodeFiltersFallbackToRemote 验证本机没有候选时回落跨机（不把候选集清空）。
func TestBattleNodeFiltersFallbackToRemote(t *testing.T) {
	cfg := &conf.Bootstrap{Battle: &conf.Battle{
		Placement:  conf.BattlePlacement_BATTLE_PLACEMENT_LOCAL_FIRST,
		LocalHosts: []string{"127.0.0.1"},
	}}
	filters, err := battleNodeFilters(cfg)
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	nodes := []selector.Node{placementNode("10.0.0.2:9000"), placementNode("10.0.0.3:9000")}
	if got := decided(filters, nodes); len(got) != len(nodes) {
		t.Fatalf("无本机候选时应原样返回 %d 个候选，got %v", len(nodes), got)
	}
}

// TestBattleNodeFiltersAnyKeepsAll 验证 placement=ANY 时不加过滤器（保持旧行为，可配）。
func TestBattleNodeFiltersAnyKeepsAll(t *testing.T) {
	cfg := &conf.Bootstrap{Battle: &conf.Battle{
		Placement:  conf.BattlePlacement_BATTLE_PLACEMENT_ANY,
		LocalHosts: []string{"127.0.0.1"},
	}}
	filters, err := battleNodeFilters(cfg)
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	if len(filters) != 0 {
		t.Fatalf("ANY 不应加任何过滤器，got %d", len(filters))
	}
	nodes := []selector.Node{placementNode("10.0.0.2:9000"), placementNode("127.0.0.1:9001")}
	if got := decided(filters, nodes); len(got) != 2 {
		t.Fatalf("ANY 应保留全部候选，got %v", got)
	}
}

// TestWarmupEnabledDefaultsOn 验证预热缺省打开（未配置 = true，不因零值静默关闭）。
func TestWarmupEnabledDefaultsOn(t *testing.T) {
	if !warmupEnabled(&conf.Bootstrap{}) {
		t.Fatal("未配置时应缺省打开预热")
	}
	off := false
	if warmupEnabled(&conf.Bootstrap{Battle: &conf.Battle{Warmup: &off}}) {
		t.Fatal("显式 false 应关闭预热")
	}
}

// TestWarmupTimeoutDefault 验证预热预算缺省 2s（与冷启动实测 p50≈2s 同量级）。
func TestWarmupTimeoutDefault(t *testing.T) {
	if got := warmupTimeout(&conf.Bootstrap{}); got != 2*time.Second {
		t.Fatalf("缺省预热预算 = %v，期望 2s", got)
	}
	if got := warmupTimeout(&conf.Bootstrap{Battle: &conf.Battle{WarmupTimeoutMs: 500}}); got != 500*time.Millisecond {
		t.Fatalf("配置预热预算 = %v，期望 500ms", got)
	}
}

// TestBattleNodeFiltersPinWholeGameToOneBattle 是本修复的核心用例（在生产接缝上）：
// 一局的预热与入场必须解析出**同一台且只有一台** battle 候选，100 局按 50/50 分摊。
//
// 旧实现在此直接红：选点完全交给每 RPC 的均衡器，两台等权本机 battle 下两跳必然劈到两台
// （候选集恒为 2 台 ⇒ 「预热总是白做、属主由两跳竞态决定」）。
func TestBattleNodeFiltersPinWholeGameToOneBattle(t *testing.T) {
	cfg := &conf.Bootstrap{Battle: &conf.Battle{
		Placement:  conf.BattlePlacement_BATTLE_PLACEMENT_LOCAL_FIRST,
		LocalHosts: []string{"127.0.0.1"},
	}}
	filters, err := battleNodeFilters(cfg)
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	disc := &appFakeDiscovery{ins: []*registry.ServiceInstance{
		{ID: "battle-1", Name: "battle", Endpoints: []string{"grpc://127.0.0.1:9301"}},
		{ID: "battle-b2", Name: "battle", Endpoints: []string{"grpc://127.0.0.1:9311"}},
	}}
	planner := placement.New(disc, placement.Options{Local: filter.PreferLocal("127.0.0.1")})
	nodes := []selector.Node{placementNode("127.0.0.1:9301"), placementNode("127.0.0.1:9311")}
	counts := make(map[string]int, 2)
	for i := 0; i < 100; i++ {
		ctx := planner.Pin(context.Background())
		warm := decidedCtx(ctx, filters, nodes)
		admit := decidedCtx(ctx, filters, nodes)
		if len(warm) != 1 || len(admit) != 1 {
			t.Fatalf("第 %d 局：两跳的候选数应为 1（已钉选点），实际 warm=%v admit=%v", i, warm, admit)
		}
		if warm[0] != admit[0] {
			t.Fatalf("第 %d 局：预热落到 %s、入场落到 %s（同一局必须同一台）", i, warm[0], admit[0])
		}
		counts[warm[0]]++
	}
	t.Logf("同局两跳同台的 100 局分布: %v", counts)
	if len(counts) != 2 {
		t.Fatalf("两台本机 battle 都应分到对局，实际 %v", counts)
	}
	for addr, n := range counts {
		if n < 45 || n > 55 {
			t.Fatalf("%s 分到 %d 局，超出 [45,55]（分布 %v）", addr, n, counts)
		}
	}
}

// decidedCtx 在给定 ctx 上依次套用过滤器并返回剩余候选地址。
func decidedCtx(ctx context.Context, filters []selector.NodeFilter, nodes []selector.Node) []string {
	out := nodes
	for _, f := range filters {
		out = f(ctx, out)
	}
	addrs := make([]string, 0, len(out))
	for _, n := range out {
		addrs = append(addrs, n.Address())
	}
	return addrs
}

// appFakeDiscovery 是 app 包用例的最小注册发现桩（只实现 GetService）。
type appFakeDiscovery struct{ ins []*registry.ServiceInstance }

// GetService 返回固定实例列表。
func (d *appFakeDiscovery) GetService(context.Context, string) ([]*registry.ServiceInstance, error) {
	return d.ins, nil
}

// Watch 返回一次性快照的监听桩（选点只走 GetService，监听是给 gRPC resolver 用的）。
func (d *appFakeDiscovery) Watch(context.Context, string) (registry.Watcher, error) {
	return &appFakeWatcher{ins: d.ins, stop: make(chan struct{})}, nil
}

// appFakeWatcher 是只推一次快照、之后阻塞到 Stop 的监听桩（resolver 首次解析用）。
type appFakeWatcher struct {
	ins  []*registry.ServiceInstance
	done bool
	stop chan struct{}
	once sync.Once
}

// Next 首次返回实例快照，之后阻塞到 Stop（resolver 不依赖后续变更）。
func (w *appFakeWatcher) Next() ([]*registry.ServiceInstance, error) {
	if !w.done {
		w.done = true
		return w.ins, nil
	}
	<-w.stop
	return nil, context.Canceled
}

// Stop 唤醒阻塞中的 Next（幂等；stop 在构造时创建，避免与 Next 竞争赋值）。
func (w *appFakeWatcher) Stop() error {
	w.once.Do(func() { close(w.stop) })
	return nil
}
