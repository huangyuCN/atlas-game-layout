package app

import (
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	"go.uber.org/fx"
)

// TestGraphStaticValidation 对 gateway 依赖图做静态校验：
// 清单中无重复提供的类型、Invoke 依赖闭包完整、双 servers 组注解正确
// （已知局限：部分 Provider 可能沿 Invoke 链被实例化但构造错误会被忽略，
// 组件级行为由 services/gateway/assemble 与 test/e2e 兜底）。
func TestGraphStaticValidation(t *testing.T) {
	cfg := &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "gateway", Id: "graph-test"},
	}
	if err := fx.ValidateApp(
		fx.Supply(cfg),
		// 指标采集器由 bootstrap 装配供给（AssembleLoaded），静态校验时补 noop。
		fx.Provide(func() metrics.Collector { return metrics.Noop() }),
		Module,
		// 消费 servers 组：强制 ValidateApp 展开传输层构造依赖链
		fx.Invoke(fx.Annotate(
			func([]transport.Server) {},
			fx.ParamTags(`group:"servers"`),
		)),
	); err != nil {
		t.Fatalf("依赖图静态校验失败: %v", err)
	}
}

// TestNewServerSetTrimsDisabledProtocols 验证协议裁剪：未声明（server.<proto> 缺失）的协议
// 不进 servers 值组——nil 进组会让 atlas.App 对着空服务端调 Start 而 panic。
func TestNewServerSetTrimsDisabledProtocols(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{
		Http:      &configspb.Server_HTTP{Addr: "127.0.0.1:0"},
		Tcp:       &configspb.Server_TCP{Addr: "127.0.0.1:0"},
		Websocket: &configspb.Server_WebSocket{Addr: "127.0.0.1:0"},
	}}
	httpSrv, err := serverutil.HTTPServer(cfg.GetServer().GetHttp(), nil, nil)
	if err != nil {
		t.Fatalf("构造 HTTP 服务端失败: %v", err)
	}
	tcpSrv, err := serverutil.TCPServer(cfg.GetServer().GetTcp())
	if err != nil {
		t.Fatalf("构造 TCP 服务端失败: %v", err)
	}
	wsSrv, err := serverutil.WSServer(cfg.GetServer().GetWebsocket())
	if err != nil {
		t.Fatalf("构造 WebSocket 服务端失败: %v", err)
	}
	kcpSrv, err := serverutil.KCPServer(cfg.GetServer().GetKcp())
	if err != nil {
		t.Fatalf("构造 KCP 服务端失败: %v", err)
	}
	udpSrv, err := serverutil.UDPServer(cfg.GetServer().GetUdp())
	if err != nil {
		t.Fatalf("构造 UDP 服务端失败: %v", err)
	}

	set := newServerSet(cfg, httpSrv, tcpSrv, wsSrv, kcpSrv, udpSrv)
	if set.HTTP == nil || set.TCP == nil || set.WS == nil {
		t.Fatalf("已声明协议应进启停组: %+v", set)
	}
	if set.KCP != nil || set.UDP != nil {
		t.Fatalf("未声明协议不应进启停组: kcp=%v udp=%v", set.KCP, set.UDP)
	}
}
