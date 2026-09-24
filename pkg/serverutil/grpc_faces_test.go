package serverutil

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/transport"
)

// freeAddr 返回一个当前空闲的 127.0.0.1 地址（监听后立即释放），
// 供「Endpoint() 等于配置值」这类需要具体端口的断言使用。
func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("申请空闲端口失败: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("释放空闲端口失败: %v", err)
	}
	return addr
}

// startFace 后台启动一个面并返回就绪端点（测试结束自动停止）。
func startFace(t *testing.T, srv *FaceServer) *url.URL {
	t.Helper()
	go func() { _ = srv.Start(context.Background()) }()
	ep, err := WaitEndpoint(srv, 5*time.Second)
	if err != nil {
		t.Fatalf("等待 %s 面就绪失败: %v", srv.Face(), err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return ep
}

// TestGRPCServersTwoFacesIndependent 验证两个面各自独立监听、互不覆盖：
// 各自 Endpoint() 的 host:port 等于配置值，scheme 按面区分（internal=grpc / edge=grpc-edge）。
func TestGRPCServersTwoFacesIndependent(t *testing.T) {
	edgeAddr, internalAddr := freeAddr(t), freeAddr(t)
	faces, err := GRPCServers(&configspb.Server_GRPC{EdgeAddr: edgeAddr, InternalAddr: internalAddr}, nil)
	if err != nil {
		t.Fatalf("GRPCServers() 错误 = %v", err)
	}
	if faces.Edge == nil || faces.Internal == nil {
		t.Fatalf("两面都应启用，实际 edge=%v internal=%v", faces.Edge, faces.Internal)
	}
	edgeEP := startFace(t, faces.Edge)
	internalEP := startFace(t, faces.Internal)

	if edgeEP.Host != edgeAddr {
		t.Errorf("edge 面端点 = %q，期望配置值 %q", edgeEP.Host, edgeAddr)
	}
	if internalEP.Host != internalAddr {
		t.Errorf("internal 面端点 = %q，期望配置值 %q", internalEP.Host, internalAddr)
	}
	if edgeEP.Scheme != SchemeGRPCEdge || internalEP.Scheme != SchemeGRPC {
		t.Errorf("端点 scheme = (%q, %q)，期望 (%q, %q)",
			edgeEP.Scheme, internalEP.Scheme, SchemeGRPCEdge, SchemeGRPC)
	}
	if edgeEP.Host == internalEP.Host {
		t.Fatalf("两面端点相同（%q）：地址被互相覆盖", edgeEP.Host)
	}
	// 两个面都在真实监听：各自都有 health 服务（框架内置），且服务信息互相独立。
	for _, srv := range []*FaceServer{faces.Edge, faces.Internal} {
		if len(srv.GetServiceInfo()) == 0 {
			t.Errorf("%s 面未注册任何服务（未真正监听？）", srv.Face())
		}
	}
}

// TestGRPCServersRandomPortsAllowed 验证 127.0.0.1:0（进程内/测试形态）两面都启用：
// 端口 0 由内核分配随机端口，两面必然不同，故「地址字面相同」不算冲突。
func TestGRPCServersRandomPortsAllowed(t *testing.T) {
	const randomPort = "127.0.0.1:0"
	faces, err := GRPCServers(&configspb.Server_GRPC{EdgeAddr: randomPort, InternalAddr: randomPort}, nil)
	if err != nil {
		t.Fatalf("GRPCServers(127.0.0.1:0, 127.0.0.1:0) 错误 = %v", err)
	}
	if faces.Edge == nil || faces.Internal == nil {
		t.Fatal("两面都应启用")
	}
	edgeEP := startFace(t, faces.Edge)
	internalEP := startFace(t, faces.Internal)
	if edgeEP.Port() == "0" || internalEP.Port() == "0" {
		t.Fatalf("端点端口未解析：edge=%v internal=%v", edgeEP, internalEP)
	}
	if edgeEP.Port() == internalEP.Port() {
		t.Fatalf("两面随机端口相同（%s）：互不覆盖的保证被破坏", edgeEP.Port())
	}
}

// TestGRPCServersSameAddrRejected 验证两面地址相同即装配期报错（不构造任何面）。
func TestGRPCServersSameAddrRejected(t *testing.T) {
	addr := freeAddr(t)
	faces, err := GRPCServers(&configspb.Server_GRPC{EdgeAddr: addr, InternalAddr: addr}, nil)
	if err == nil {
		t.Fatal("两面地址相同期望报错，实际为 nil")
	}
	if faces.Edge != nil || faces.Internal != nil {
		t.Fatalf("报错时不应返回任何面：%+v", faces)
	}
}

// TestGRPCServersEmptyAddrDisablesFace 验证地址为空 = 该面不启用（返回 nil、不监听），
// 另一面不受影响；两面都空则都不启用，且不报错（进程仍可用）。
func TestGRPCServersEmptyAddrDisablesFace(t *testing.T) {
	addr := freeAddr(t)
	cases := []struct {
		name         string
		cfg          *configspb.Server_GRPC
		wantEdge     bool
		wantInternal bool
	}{
		{"节点缺失", nil, false, false},
		{"节点在但两面都空", &configspb.Server_GRPC{}, false, false},
		{"只启用 edge", &configspb.Server_GRPC{EdgeAddr: addr}, true, false},
		{"只启用 internal", &configspb.Server_GRPC{InternalAddr: addr}, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			faces, err := GRPCServers(c.cfg, nil)
			if err != nil {
				t.Fatalf("GRPCServers() 错误 = %v", err)
			}
			if (faces.Edge != nil) != c.wantEdge || (faces.Internal != nil) != c.wantInternal {
				t.Fatalf("启用状态 = (edge %v, internal %v)，期望 (%v, %v)",
					faces.Edge != nil, faces.Internal != nil, c.wantEdge, c.wantInternal)
			}
			// 未启用的面必须是 nil 接口（非 nil 接口持空指针会绕过 ActiveServers 过滤）。
			servers := ActiveServers([]transport.Server{faces.Edge.Transport(), faces.Internal.Transport()})
			want := 0
			if c.wantEdge {
				want++
			}
			if c.wantInternal {
				want++
			}
			if len(servers) != want {
				t.Fatalf("ActiveServers 过滤后 = %d 个，期望 %d 个", len(servers), want)
			}
		})
	}
}

// TestFaceServerNilTransport 验证未启用面取 transport.Server 得到 nil 接口（nil 接收者安全）。
func TestFaceServerNilTransport(t *testing.T) {
	var s *FaceServer
	if got := s.Transport(); got != nil {
		t.Fatalf("nil 面 Transport() = %v，期望 nil 接口", got)
	}
}
