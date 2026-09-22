package bootstrap

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	"go.uber.org/fx"
)

// bootHandles 是内嵌 Servers 的测试回捞句柄。
type bootHandles struct {
	fx.In

	Servers
}

// serveGroup 返回把给定服务端放进 servers 值组的 fx 选项。
func serveGroup(servers ...transport.Server) fx.Option {
	opts := make([]fx.Option, 0, len(servers))
	for _, srv := range servers {
		opts = append(opts, fx.Provide(fx.Annotate(
			func() transport.Server { return srv }, fx.ResultTags(`group:"servers"`))))
	}
	return fx.Options(opts...)
}

// TestBootStartsAndStopsGraph 验证进程内启动：返回时服务端已就绪、端点可按 scheme 取；
// 停机后监听关闭（启停由 atlas.App 驱动，且不注册进程信号）。
func TestBootStartsAndStopsGraph(t *testing.T) {
	httpSrv, err := atlashttp.NewServer(atlashttp.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构造 HTTP 服务端失败: %v", err)
	}
	httpSrv.HandleFunc("/health", serverutil.HealthHandler("demo"))
	grpcSrv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构造 gRPC 服务端失败: %v", err)
	}
	cfg := &testBootstrap{Runtime: &configspb.Runtime{Name: "demo", Id: "demo-1"}}

	var h bootHandles
	root, urls, err := Boot(context.Background(), cfg, serveGroup(httpSrv, grpcSrv), &h, "http", "grpc")
	if err != nil {
		t.Fatalf("Boot() 错误 = %v", err)
	}
	if urls["http"] == nil || urls["grpc"] == nil || urls["http"].Port() == "0" {
		t.Fatalf("端点归集不符合预期: %v", urls)
	}
	// 返回时服务端已就绪：HTTP 端点可直接访问。
	resp, err := http.Get("http://" + urls["http"].Host + "/health")
	if err != nil {
		t.Fatalf("Boot 返回后 HTTP 端点应已就绪: %v", err)
	}
	_ = resp.Body.Close()

	if err := root.Stop(context.Background()); err != nil {
		t.Fatalf("root.Stop() 错误 = %v", err)
	}
	if _, err := http.Get("http://" + urls["http"].Host + "/health"); err == nil {
		t.Fatal("root.Stop 后 HTTP 端点仍可访问")
	}
}

// TestBootRequiresEndpoints 验证 require 声明的端点缺失时启动失败并回收已启动服务端
// （缺端点属装配错误，不能静默返回空串）。
func TestBootRequiresEndpoints(t *testing.T) {
	httpSrv, err := atlashttp.NewServer(atlashttp.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构造 HTTP 服务端失败: %v", err)
	}
	cfg := &testBootstrap{Runtime: &configspb.Runtime{Name: "demo", Id: "demo-1"}}

	var h bootHandles
	root, _, err := Boot(context.Background(), cfg, serveGroup(httpSrv), &h, "grpc")
	if err == nil || !strings.Contains(err.Error(), "grpc") {
		t.Fatalf("缺 grpc 端点应报错，实际 %v", err)
	}
	if root != nil {
		t.Fatal("启动失败时不应返回根 App")
	}
	ep, err := httpSrv.Endpoint()
	if err != nil {
		t.Fatalf("查询端点失败: %v", err)
	}
	if _, err := http.Get("http://" + ep.Host); err == nil {
		t.Fatal("Boot 失败后应回收已启动的服务端")
	}
}
