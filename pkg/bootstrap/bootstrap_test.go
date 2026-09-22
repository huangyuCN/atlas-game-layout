package bootstrap

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas"
	"github.com/huangyuCN/atlas-game-layout/lib/version"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/registry"
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
		fx.Supply(res.Start),
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

// failingRegistrar 是注册必然失败的测试替身。
type failingRegistrar struct {
	err error
}

func (f *failingRegistrar) Register(context.Context, *registry.ServiceInstance) error {
	return f.err
}

func (f *failingRegistrar) Deregister(context.Context, *registry.ServiceInstance) error { return nil }

// TestRegisterLifecycleFailsOnRegisterError 验证注册失败升级为启动失败：
// 否则实例会带着「端口已监听但注册中心里没有」的半死状态继续运行。
func TestRegisterLifecycleFailsOnRegisterError(t *testing.T) {
	srv := newMockServer()
	res, err := New(Params{
		Servers:   []transport.Server{srv},
		Registrar: &failingRegistrar{err: registry.ErrInstanceConflict},
	}, Options{Name: "demo"})
	if err != nil {
		t.Fatalf("New() 错误 = %v", err)
	}

	app := fx.New(
		fx.Provide(func() *atlas.App { return res.App }),
		fx.Supply(res.Start),
		fx.Invoke(RegisterLifecycle),
	)
	err = app.Start(context.Background())
	if err == nil {
		t.Fatal("注册失败应导致启动失败，实际为 nil")
	}
	if !errors.Is(err, registry.ErrInstanceConflict) {
		t.Fatalf("启动错误应包裹 ErrInstanceConflict，实际 %v", err)
	}
}

// TestRegisterLifecycleWaitsReady 验证 Start 在服务注册完成后才返回：
// 启动结果信号未到不得提前返回（否则注册失败会被当成启动成功）。
func TestRegisterLifecycleWaitsReady(t *testing.T) {
	srv := newMockServer()
	res, err := New(Params{Servers: []transport.Server{srv}}, Options{Name: "demo"})
	if err != nil {
		t.Fatalf("New() 错误 = %v", err)
	}

	app := fx.New(
		fx.Provide(func() *atlas.App { return res.App }),
		fx.Supply(res.Start),
		fx.Invoke(RegisterLifecycle),
	)
	if err := app.Start(context.Background()); err != nil {
		t.Fatalf("fx.Start() 错误 = %v", err)
	}
	// Start 返回时 App 已完成注册（本用例无注册器，等价于已就绪）。
	select {
	case <-srv.started:
	default:
		t.Fatal("Start 返回时 server 尚未启动：就绪信号未生效")
	}
	if err := app.Stop(context.Background()); err != nil {
		t.Fatalf("fx.Stop() 错误 = %v", err)
	}
}

// TestModuleForVersionSource 验证 App 版本与链路资源属性/指标标签同源：
// runtime.version 优先，缺省回退构建注入值（lib/version）——两处各取一份会让同一进程
// 在注册中心与链路里上报两个版本号。
func TestModuleForVersionSource(t *testing.T) {
	cases := []struct {
		name       string
		configured string
		want       string
	}{
		{"配置优先", "v9.9.9", "v9.9.9"},
		{"缺省回退构建注入值", "", version.Version},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &testBootstrap{Runtime: &configspb.Runtime{Name: "demo", Id: "demo-1", Version: c.configured}}
			srv := newMockServer()
			var app *atlas.App
			root := fx.New(
				fx.NopLogger,
				fx.Supply(cfg),
				fx.Provide(fx.Annotate(func() transport.Server { return srv }, fx.ResultTags(`group:"servers"`))),
				ModuleFor(cfg),
				fx.Populate(&app),
			)
			if err := root.Err(); err != nil {
				t.Fatalf("依赖图校验失败: %v", err)
			}
			if app.Version() != c.want {
				t.Fatalf("App 版本 = %q，期望 %q", app.Version(), c.want)
			}
		})
	}
}
