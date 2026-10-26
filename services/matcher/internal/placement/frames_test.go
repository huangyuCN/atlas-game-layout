package placement

import (
	"context"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector/filter"
)

// 本文件守两条「选点不挑到坏实例」的语义：
//   - 候选必须是**有帧面端点**的实例（没有 battle-frame 端点的实例接得住 Create，
//     却接不住客户端帧连接——选到它就等于这局永远连不上）；
//   - Admit 失败时能换下一个候选重试一次（Retarget），而不是钉死在一台上。

// serviceDiscovery 按服务名返回不同实例列表：选点器同时查 battle 与 battle-frame，
// 单一列表的桩会把「帧面实例」误当成「battle 实例」（旧桩即如此）。
type serviceDiscovery struct {
	byService map[string][]*registry.ServiceInstance
	calls     map[string]int
}

// GetService 返回该服务名的实例列表（并按服务名计数，快照复用断言用）。
func (d *serviceDiscovery) GetService(_ context.Context, service string) ([]*registry.ServiceInstance, error) {
	if d.calls == nil {
		d.calls = make(map[string]int)
	}
	d.calls[service]++
	return append([]*registry.ServiceInstance(nil), d.byService[service]...), nil
}

// Watch 不支持（选点不做长连监听）。
func (d *serviceDiscovery) Watch(context.Context, string) (registry.Watcher, error) {
	return nil, errNoWatch
}

// callsOf 返回某服务名被查询的次数。
func (d *serviceDiscovery) callsOf(service string) int { return d.calls[service] }

// frameInstance 构造一条 battle-frame 注册实例：ID 与 battle 实例同源（runtime.id），
// 端点 scheme 是帧面（ws/kcp/udp）。
func frameInstance(id string, endpoints ...string) *registry.ServiceInstance {
	return &registry.ServiceInstance{ID: id, Name: consts.ServiceBattleFrame, Endpoints: endpoints}
}

// newFrameAwarePlanner 构造带本机优先的选点器（同时查 battle 与 battle-frame）。
func newFrameAwarePlanner(disc *serviceDiscovery) *Planner {
	return New(disc, Options{Local: filter.PreferLocal("127.0.0.1")})
}

// TestPlannerExcludesBattlesWithoutFrameEndpoint 是本修复的核心断言：
// 两台 battle 里只有一台注册了 battle-frame 帧面端点 ⇒ 100 次选点**全选它**。
//
// 旧实现在此红：候选只看「有 grpc:// 端点」，两台严格 50/50——被选中的那一半对局
// 客户端永远连不上帧面（接得住 Create、接不住帧连接）。
func TestPlannerExcludesBattlesWithoutFrameEndpoint(t *testing.T) {
	disc := &serviceDiscovery{byService: map[string][]*registry.ServiceInstance{
		consts.ServiceBattle: {
			battleInstance("battle-1", "127.0.0.1:9301"),
			battleInstance("battle-b2", "127.0.0.1:9311"),
		},
		consts.ServiceBattleFrame: {
			frameInstance("battle-b2", "ws://127.0.0.1:9412", "udp://127.0.0.1:9413"),
		},
	}}
	counts := placeN(t, newFrameAwarePlanner(disc), 100)
	if len(counts) != 1 {
		t.Fatalf("只有 battle-b2 有帧面端点，选点却落到多台: %v", counts)
	}
	if counts["127.0.0.1:9311"] != 100 {
		t.Fatalf("100 次选点应全选有帧面端点的 battle-b2，实际分布 %v", counts)
	}
}

// TestPlannerKeepsCandidatesWhenFrameDiscoveryEmpty 验证帧面发现为空时**不过滤**：
// 选点只是分摊手段，注册中心暂时没有帧面实例（滚动重启、发现抖动）不得把候选缩成空集
// 而让整批对局失去选点（退回每 RPC 均衡仍能工作）。
func TestPlannerKeepsCandidatesWhenFrameDiscoveryEmpty(t *testing.T) {
	disc := &serviceDiscovery{byService: map[string][]*registry.ServiceInstance{
		consts.ServiceBattle: {
			battleInstance("battle-1", "127.0.0.1:9301"),
			battleInstance("battle-b2", "127.0.0.1:9311"),
		},
	}}
	counts := placeN(t, newFrameAwarePlanner(disc), 100)
	assertEven(t, counts, 100)
}

// TestPlannerFallsBackWhenNoFrameEndpointAtAll 验证帧面实例都在、但没有任何帧面端点时退回不过滤：
// 过滤后无候选时宁可退回旧口径（可能选到坏实例），也不能让选点整体失效。
func TestPlannerFallsBackWhenNoFrameEndpointAtAll(t *testing.T) {
	disc := &serviceDiscovery{byService: map[string][]*registry.ServiceInstance{
		consts.ServiceBattle: {
			battleInstance("battle-1", "127.0.0.1:9301"),
			battleInstance("battle-b2", "127.0.0.1:9311"),
		},
		consts.ServiceBattleFrame: {
			frameInstance("battle-1", "grpc://127.0.0.1:9301"),
			frameInstance("battle-b2"),
		},
	}}
	counts := placeN(t, newFrameAwarePlanner(disc), 100)
	assertEven(t, counts, 100)
}

// TestPlannerRetargetPicksAnotherCandidate 验证换点语义：Retarget 必须给出**另一个**候选，
// 且新目标确实挂到了返回的 ctx 上（入场重试据 Target 寻址）。
func TestPlannerRetargetPicksAnotherCandidate(t *testing.T) {
	disc := &serviceDiscovery{byService: map[string][]*registry.ServiceInstance{
		consts.ServiceBattle: {
			battleInstance("battle-1", "127.0.0.1:9301"),
			battleInstance("battle-b2", "127.0.0.1:9311"),
		},
	}}
	p := newFrameAwarePlanner(disc)
	ctx := p.Pin(context.Background())
	first, ok := Target(ctx)
	if !ok {
		t.Fatal("Pin 后 ctx 上应有选点目标")
	}
	retryCtx, ok := p.Retarget(ctx)
	if !ok {
		t.Fatal("两台候选时应能换点")
	}
	second, _ := Target(retryCtx)
	if second == "" || second == first {
		t.Fatalf("换点应给出另一个候选：首次 %q，换点后 %q", first, second)
	}
}

// TestPlannerRetargetWithoutAlternative 验证单台候选时换点不成立（ok=false，ctx 不变）：
// 换到同一台只是原样重试，没有价值——失败按既有路径处置即可。
func TestPlannerRetargetWithoutAlternative(t *testing.T) {
	disc := &serviceDiscovery{byService: map[string][]*registry.ServiceInstance{
		consts.ServiceBattle: {battleInstance("battle-1", "127.0.0.1:9301")},
	}}
	p := newFrameAwarePlanner(disc)
	ctx := p.Pin(context.Background())
	retryCtx, ok := p.Retarget(ctx)
	if ok {
		t.Fatal("只有一台候选时不应换点")
	}
	if retryCtx != ctx {
		t.Fatal("换点失败时应原样返回 ctx")
	}
}

// TestPlannerFrameSnapshotReused 验证帧面发现也在快照窗口内复用：
// 100 次选点对两个服务各只查 1 次注册中心（不能因为多了帧面过滤把成局速率绑到 etcd QPS）。
func TestPlannerFrameSnapshotReused(t *testing.T) {
	disc := &serviceDiscovery{byService: map[string][]*registry.ServiceInstance{
		consts.ServiceBattle:      {battleInstance("battle-1", "127.0.0.1:9301")},
		consts.ServiceBattleFrame: {frameInstance("battle-1", "ws://127.0.0.1:9411")},
	}}
	placeN(t, newFrameAwarePlanner(disc), 100)
	for _, svc := range []string{consts.ServiceBattle, consts.ServiceBattleFrame} {
		if n := disc.callsOf(svc); n != 1 {
			t.Fatalf("100 次选点对 %s 应只查 1 次注册中心（快照复用），实际 %d 次", svc, n)
		}
	}
}
