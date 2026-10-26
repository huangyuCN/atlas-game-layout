package app

import (
	"context"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector"
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
