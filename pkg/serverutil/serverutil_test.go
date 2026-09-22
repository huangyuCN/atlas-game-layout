package serverutil

import (
	"context"
	"testing"

	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
)

// TestEndpoints 验证按 scheme 归集端点：返回完整 URL（WS 之类带路径的协议需要它），
// 且拿到端点时服务端已真实监听。
func TestEndpoints(t *testing.T) {
	httpSrv, err := atlashttp.NewServer(atlashttp.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构造 http 服务端失败: %v", err)
	}
	grpcSrv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构造 gRPC 服务端失败: %v", err)
	}
	servers := []transport.Server{httpSrv, grpcSrv}
	t.Cleanup(func() {
		for _, srv := range servers {
			_ = srv.Stop(context.Background())
		}
	})

	eps, err := Endpoints(servers)
	if err != nil {
		t.Fatalf("Endpoints() 错误 = %v", err)
	}
	if len(eps) != 2 {
		t.Fatalf("应归集 2 个端点，实际 %v", eps)
	}
	for _, scheme := range []string{"http", "grpc"} {
		ep, ok := eps[scheme]
		if !ok || ep.Scheme != scheme || ep.Hostname() != "127.0.0.1" || ep.Port() == "0" {
			t.Fatalf("端点 %s 不符合预期: %v", scheme, ep)
		}
	}
}

// TestEndpointsRejectsNonEndpointer 验证不支持端点查询的服务端直接报错（不静默跳过）。
func TestEndpointsRejectsNonEndpointer(t *testing.T) {
	if _, err := Endpoints([]transport.Server{noEndpointServer{}}); err == nil {
		t.Fatal("非 Endpointer 服务端期望报错")
	}
}

// noEndpointServer 是不实现 Endpointer 的服务端桩。
type noEndpointServer struct{}

func (noEndpointServer) Start(context.Context) error { return nil }

func (noEndpointServer) Stop(context.Context) error { return nil }

// TestEndpointsSkipsDisabled 验证未启用的协议（值为 nil）被跳过而非报错：
// 装配层按配置节裁剪协议时，nil 仍留在 fx 值组里。
func TestEndpointsSkipsDisabled(t *testing.T) {
	httpSrv, err := atlashttp.NewServer(atlashttp.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构造 http 服务端失败: %v", err)
	}
	t.Cleanup(func() { _ = httpSrv.Stop(context.Background()) })

	eps, err := Endpoints([]transport.Server{nil, httpSrv, nil})
	if err != nil {
		t.Fatalf("Endpoints() 错误 = %v", err)
	}
	if len(eps) != 1 || eps["http"] == nil {
		t.Fatalf("应只归集已启用的 http 端点，实际 %v", eps)
	}
}
