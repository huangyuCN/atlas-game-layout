package app

import (
	"context"
	"net"
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
		t.Fatalf("拨号失败: %v", err)
	}
	defer conn.Close()

	planner := placement.New(disc, placement.Options{Local: filter.PreferLocal("127.0.0.1")})
	hits := assertPinnedCalls(t, conn, planner, map[string]string{addrA: "A", addrB: "B"})
	t.Logf("端到端钉选点 100 次分布: %v（A=%s B=%s）", hits, addrA, addrB)
	assertEvenHits(t, hits)
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
			t.Fatalf("第 %d 次调用失败: %v", i, err)
		}
		if got, want := out.Value, byTarget[target]; got != want {
			t.Fatalf("第 %d 次：钉的是 %s，实际落到 %s（目标未传到 picker）", i, target, got)
		}
		hits[out.Value]++
	}
	return hits
}

// assertEvenHits 断言两台都用到且偏差不超过 10%。
func assertEvenHits(t *testing.T, hits map[string]int) {
	t.Helper()
	if len(hits) != 2 {
		t.Fatalf("轮询应两台都用到，实际 %v", hits)
	}
	for id, n := range hits {
		if n < 45 || n > 55 {
			t.Fatalf("%s 落到 %d 次，超出 [45,55]（分布 %v）", id, n, hits)
		}
	}
}
