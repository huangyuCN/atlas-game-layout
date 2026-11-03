package app

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/placement"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector/filter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// echoServer 起一个只回自身 id 的 grpc 服务端（含健康检查，与框架服务端同口径），返回监听地址。
//
// 端口用 0（交给操作系统分配）而非写死的端口：CI runner 与本机可能已有进程占用固定端口，
// 并行跑的用例也会互抢。地址从 listener.Addr() 取回，注册中心里登记的就是实际端口。
// 注意这**不是**本用例历史偶发红的根因——端口本来就是 0，根因见 waitBothReady。
func echoServer(t *testing.T, id string) (addr string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		var in emptypb.Empty
		if err := stream.RecvMsg(&in); err != nil {
			return err
		}
		return stream.SendMsg(wrapperspb.String(id))
	}))
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// TestPinTargetReachesGRPCPicker 是本修复的端到端接缝验证：
// ctx 上的选点目标必须真的被 gRPC 的 picker 认到（只拨目标那一台）。
//
// 为什么必须单独验一次：过滤器是拨号期挂上的、选点在调用期进 ctx，两段的连接点
// （客户端 ctx → picker 的 PickInfo.Ctx → selector 过滤器）全在框架里，单测覆盖不到；
// 一旦这条链断了，「每局一次选点」会静默失效（退回每 RPC 均衡）而测试全绿。
func TestPinTargetReachesGRPCPicker(t *testing.T) {
	addrA := echoServer(t, "A")
	addrB := echoServer(t, "B")
	disc := &appFakeDiscovery{ins: []*registry.ServiceInstance{
		{ID: "a", Name: "battle", Endpoints: []string{"grpc://" + addrA}},
		{ID: "b", Name: "battle", Endpoints: []string{"grpc://" + addrB}},
	}}
	cfg := &conf.Bootstrap{Battle: &conf.Battle{
		Placement:  conf.BattlePlacement_BATTLE_PLACEMENT_LOCAL_FIRST,
		LocalHosts: []string{"127.0.0.1"},
	}}
	filters, err := battleNodeFilters(cfg)
	if err != nil {
		t.Fatalf("构造过滤器失败: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := serverutil.DialDomain(ctx, disc, "battle", serverutil.SchemeGRPC, nil,
		serverutil.WithNodeFilters(filters...))
	if err != nil {
		t.Fatalf("拨号失败（A=%s B=%s）: %v", addrA, addrB, err)
	}
	defer conn.Close()

	byTarget := map[string]string{addrA: "A", addrB: "B"}
	// 先等 picker 的两台候选都就绪，再开始钉选断言（根因与理由见 waitBothReady）。
	waitBothReady(t, conn, byTarget)

	planner := placement.New(disc, placement.Options{Local: filter.PreferLocal("127.0.0.1")})
	hits := assertPinnedCalls(t, conn, planner, byTarget)
	t.Logf("端到端钉选点 100 次分布: %v（A=%s B=%s）", hits, addrA, addrB)
	assertEvenHits(t, hits, addrA, addrB)
}

// waitBothReady 等到 picker 的候选集里两台都就绪。
//
// 为什么必须有这道闸（本用例此前偶发红的真正根因）：ClientConn 进入 READY 只代表
// **第一个** SubConn 就绪，此刻 base balancer 的 picker 可能只装了候选集的一部分
// （实测首帧只见到 A 或只见到 B）。此时把 ctx 钉到另一台，TargetFilter 因为「目标不在
// 候选里」按设计回落到全部候选，于是第 0 次调用就落到别的进程——0.00s 就 FAIL，
// 断言代码本身完全没跑。这是一次「拨号返回 ≠ 候选集就绪」的竞态，与固定端口无关
// （端口本来就是 0）。
//
// 探测用**未钉选点**的调用（不触碰被测链路）：两台都被命中即证明 picker 已装齐候选，
// 之后的 100 次钉选断言仍然是严格断言——闸只保证前提，钉选若真坏了这里不会替它兜底。
func waitBothReady(t *testing.T, conn *grpc.ClientConn, byTarget map[string]string) {
	t.Helper()
	seen := make(map[string]struct{}, len(byTarget))
	err := waitPickable(pinReadyBudget, pinReadyPoll, func(ctx context.Context) error {
		var out wrapperspb.StringValue
		if e := conn.Invoke(ctx, "/probe.Id/Get", &emptypb.Empty{}, &out); e != nil {
			return fmt.Errorf("就绪探测调用失败: %w", e)
		}
		seen[out.Value] = struct{}{}
		if len(seen) < len(byTarget) {
			return fmt.Errorf("picker 候选只见到 %d/%d 台（已见 %v）", len(seen), len(byTarget), keysOf(seen))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("拨号返回后 %s 内候选集未全部就绪（A=%s B=%s）: %v",
			pinReadyBudget, addrOf(byTarget, "A"), addrOf(byTarget, "B"), err)
	}
}

// assertPinnedCalls 连发 100 次调用：每次按 ctx 上的目标校验落点，返回各服务端命中数。
func assertPinnedCalls(t *testing.T, conn *grpc.ClientConn, planner *placement.Planner,
	byTarget map[string]string) map[string]int {
	t.Helper()
	hits := make(map[string]int, 2)
	for i := 0; i < 100; i++ {
		pctx := planner.Pin(context.Background())
		target, ok := placement.Target(pctx)
		if !ok {
			t.Fatalf("第 %d 次：Pin 未把目标挂到 ctx（选点静默失效）", i)
		}
		var out wrapperspb.StringValue
		if err := conn.Invoke(pctx, "/probe.Id/Get", &emptypb.Empty{}, &out); err != nil {
			t.Fatalf("第 %d 次调用失败（目标 %s，A=%s B=%s）: %v",
				i, target, addrOf(byTarget, "A"), addrOf(byTarget, "B"), err)
		}
		if got, want := out.Value, byTarget[target]; got != want {
			t.Fatalf("第 %d 次：钉的是 %s，却落到 %s（A=%s B=%s）——目标未传到 picker",
				i, target, got, addrOf(byTarget, "A"), addrOf(byTarget, "B"))
		}
		hits[out.Value]++
	}
	return hits
}

// assertEvenHits 断言两台都用到且偏差不超过 10%。
func assertEvenHits(t *testing.T, hits map[string]int, addrA, addrB string) {
	t.Helper()
	if len(hits) != 2 {
		t.Fatalf("轮询应两台都用到，实际 %v（A=%s B=%s）", hits, addrA, addrB)
	}
	for id, n := range hits {
		if n < 45 || n > 55 {
			t.Fatalf("%s 落到 %d 次，超出 [45,55]（分布 %v；A=%s B=%s）", id, n, hits, addrA, addrB)
		}
	}
}

// addrOf 按服务端 id 反查其实监听地址（错误信息里要能直接看到真实端口）。
func addrOf(byTarget map[string]string, id string) string {
	for addr, v := range byTarget {
		if v == id {
			return addr
		}
	}
	return "<未注册>"
}

// keysOf 返回集合的排序键（错误信息稳定可读，不受 map 迭代顺序影响）。
func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
