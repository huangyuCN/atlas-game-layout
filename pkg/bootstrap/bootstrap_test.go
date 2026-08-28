package bootstrap

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas"
	"github.com/huangyuCN/atlas/transport"
	"go.uber.org/fx"
)

// mockServer 是 transport.Server 的测试替身（可选 Endpointer）。
type mockServer struct {
	started  chan struct{}
	stopOnce sync.Once
	stopCh   chan struct{}
	endpoint *url.URL
}

func newMockServer() *mockServer {
	return &mockServer{
		started: make(chan struct{}),
		stopCh:  make(chan struct{}),
		// 端点必须非空：atlas App.Run → buildInstance 会对实现
		// Endpointer 的 Server 直接调用 Endpoint().String()。
		endpoint: &url.URL{Scheme: "grpc", Host: "127.0.0.1:0"},
	}
}

func (m *mockServer) Start(context.Context) error {
	select {
	case <-m.started:
	default:
		close(m.started)
	}
	return nil
}

func (m *mockServer) Stop(context.Context) error {
	m.stopOnce.Do(func() { close(m.stopCh) })
	return nil
}

func (m *mockServer) Endpoint() (*url.URL, error) { return m.endpoint, nil }

// 编译期接口满足性断言。
var _ transport.Server = (*mockServer)(nil)

// TestNewAssemblesApp 验证 New 组装出的 App 元数据与 Server 列表。
func TestNewAssemblesApp(t *testing.T) {
	srv := newMockServer()
	res, err := New(Params{Servers: []transport.Server{srv}}, Options{
		Name:    "demo",
		ID:      "demo-1",
		Version: "v1.0.0",
	})
	if err != nil {
		t.Fatalf("New() 错误 = %v", err)
	}
	if res.App == nil {
		t.Fatal("New() 应返回 App")
	}
	if res.App.Name() != "demo" || res.App.ID() != "demo-1" || res.App.Version() != "v1.0.0" {
		t.Fatalf("App 元数据不符合预期: %s/%s/%s", res.App.Name(), res.App.ID(), res.App.Version())
	}
}

// TestNewEmptyServers 验证无 Server 时组装仍可用（App 允许空服务器列表）。
func TestNewEmptyServers(t *testing.T) {
	res, err := New(Params{}, Options{Name: "demo"})
	if err != nil {
		t.Fatalf("New() 错误 = %v", err)
	}
	if res.App == nil || res.App.Name() != "demo" {
		t.Fatal("空 Server 组装结果不符合预期")
	}
}

// TestRegisterLifecycle 验证 fx 生命周期：Start 触发 server.Start，Stop 触发 server.Stop。
func TestRegisterLifecycle(t *testing.T) {
	srv := newMockServer()
	res, err := New(Params{Servers: []transport.Server{srv}}, Options{Name: "demo"})
	if err != nil {
		t.Fatalf("New() 错误 = %v", err)
	}

	app := fx.New(
		fx.Provide(func() *atlas.App { return res.App }),
		fx.Invoke(RegisterLifecycle),
	)
	startCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("fx.Start() 错误 = %v", err)
	}
	select {
	case <-srv.started:
	case <-startCtx.Done():
		t.Fatal("等待 server.Start 超时")
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("fx.Stop() 错误 = %v", err)
	}
	// server.Stop 由 App.Run 的 goroutine 在 ctx 取消后异步执行。
	select {
	case <-srv.stopCh:
	case <-time.After(5 * time.Second):
		t.Fatal("server.Stop 未被调用（等待超时）")
	}
}
