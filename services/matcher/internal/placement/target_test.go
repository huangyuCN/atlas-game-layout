package placement

import (
	"context"
	"testing"

	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector"
	"github.com/huangyuCN/atlas/selector/filter"
)

// testNodes 构造两台本机 battle 候选节点（地址即 host:port）。
func testNodes() []selector.Node {
	return []selector.Node{
		selector.NewNode("grpc", "127.0.0.1:9301", &registry.ServiceInstance{ID: "battle-1"}),
		selector.NewNode("grpc", "127.0.0.1:9311", &registry.ServiceInstance{ID: "battle-b2"}),
	}
}

// addrs 取节点地址列表（断言用）。
func addrs(nodes []selector.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.Address())
	}
	return out
}

// TestTargetFilterPinsSelectedNode 验证已选点（ctx 带目标端点）时候选只剩目标一台：
// 这一条是「每局一次选点」落地的关键——预热与入场带同一个 ctx，必然落到同一台。
func TestTargetFilterPinsSelectedNode(t *testing.T) {
	ctx := WithTarget(context.Background(), "127.0.0.1:9311")
	got := TargetFilter()(ctx, testNodes())
	if len(got) != 1 || got[0].Address() != "127.0.0.1:9311" {
		t.Fatalf("应钉住目标节点 127.0.0.1:9311，实际 %v", addrs(got))
	}
}

// TestTargetFilterFallsBackWhenTargetAbsent 验证目标实例已下线（候选里没有该地址）时
// 原样返回候选：选点失效不得让本局失败，由既有负载均衡兜底。
func TestTargetFilterFallsBackWhenTargetAbsent(t *testing.T) {
	ctx := WithTarget(context.Background(), "10.0.0.9:9301")
	got := TargetFilter()(ctx, testNodes())
	if len(got) != len(testNodes()) {
		t.Fatalf("目标不在候选里时应原样返回 %d 个候选，实际 %v", len(testNodes()), addrs(got))
	}
}

// TestTargetFilterWithoutTargetKeepsAll 验证 ctx 没有目标时过滤器不生效
// （未装配选点/发现失败时保持既有行为）。
func TestTargetFilterWithoutTargetKeepsAll(t *testing.T) {
	if got := TargetFilter()(context.Background(), testNodes()); len(got) != 2 {
		t.Fatalf("无目标时不应过滤，实际 %v", addrs(got))
	}
}

// TestPinKeepsWholeGameOnSameBattle 是本修复的核心行为断言：
// 同一局的预热与入场两跳解析出的候选集必须**完全相同且只有一台**，
// 且 100 局按 50/50 分摊。旧实现没有 Pin（两跳各自负载均衡）⇒ 断言直接红。
func TestPinKeepsWholeGameOnSameBattle(t *testing.T) {
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance("battle-1", "127.0.0.1:9301"),
		battleInstance("battle-b2", "127.0.0.1:9311"),
	}}
	p := newTestPlanner(disc)
	filters := []selector.NodeFilter{filter.PreferLocal("127.0.0.1"), TargetFilter()}
	counts := make(map[string]int, 2)
	for i := 0; i < 100; i++ {
		ctx := p.Pin(context.Background())
		warm := decide(ctx, filters)
		admit := decide(ctx, filters)
		if len(warm) != 1 || len(admit) != 1 {
			t.Fatalf("第 %d 局：两跳的候选数应为 1（已钉选点），实际 warm=%v admit=%v", i, warm, admit)
		}
		if warm[0] != admit[0] {
			t.Fatalf("第 %d 局：预热落到 %s、入场落到 %s（同一局必须同一台）", i, warm[0], admit[0])
		}
		counts[warm[0]]++
	}
	t.Logf("同局两跳钉到同一台后的 100 局分布: %v", counts)
	assertEven(t, counts, 100)
}

// decide 依次套用过滤器并返回剩余候选地址（模拟 gRPC picker 的选点前过滤）。
func decide(ctx context.Context, filters []selector.NodeFilter) []string {
	out := testNodes()
	for _, f := range filters {
		out = f(ctx, out)
	}
	return addrs(out)
}

// TestPinWithoutCandidatesKeepsContext 验证无候选时 Pin 原样返回 ctx（不注入空目标）：
// 空目标会让过滤器误判，选点失败必须等于「不做任何事」。
func TestPinWithoutCandidatesKeepsContext(t *testing.T) {
	p := newTestPlanner(&fakeDiscovery{err: errNoWatch})
	ctx := p.Pin(context.Background())
	if tgt, ok := Target(ctx); ok {
		t.Fatalf("无候选时不应注入目标，实际 %q", tgt)
	}
}
