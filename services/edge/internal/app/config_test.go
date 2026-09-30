package app

import (
	"context"
	"errors"
	"testing"
	"time"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	"github.com/huangyuCN/atlas/metrics"
)

// stubResolver 是配置映射测试用的占位 Resolver。
type stubResolver struct{}

// Resolve 实现 edge.Resolver（本测试只校验配置映射，不真正解析）。
func (stubResolver) Resolve(context.Context, edge.ResolveRequest) (edge.Backend, error) {
	return edge.Backend{}, errors.New("stubResolver: 不解析")
}

// testTicketKeyBase64 是 0x01..0x20 的 base64（与 battle 本地开发配置同值）。
const testTicketKeyBase64 = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA="

// validEdgeConfig 返回一份合法的 edge 服务配置。
func validEdgeConfig() *conf.Bootstrap {
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "edge", Namespace: "test"},
		Server:  &configspb.Server{Http: &configspb.Server_HTTP{Addr: "127.0.0.1:0"}},
		Edge: &conf.Edge{
			TicketKey: testTicketKeyBase64,
			Listeners: []*conf.Edge_Listener{
				{Name: "ws", Network: conf.Edge_NETWORK_TCP, Addr: "127.0.0.1:7100", Carrier: conf.Edge_CARRIER_WS_UPGRADE},
				{Name: "kcp", Network: conf.Edge_NETWORK_TCP, Addr: "127.0.0.1:7101", Carrier: conf.Edge_CARRIER_STREAM_HELLO},
				{Name: "udp", Network: conf.Edge_NETWORK_UDP, Addr: "127.0.0.1:7102", Carrier: conf.Edge_CARRIER_DATAGRAM},
			},
			FrameService: "battle-frame",
			MaxStreams:   32, IdleTimeout: "45s", NewConnRate: 7, MaxPerIp: 3,
			DialTimeout: "2s", ProbeAfter: "10s", ProbeTimeout: "3s",
		},
	}
}

// TestProxyConfigMapsEdgeSection 覆盖配置映射：密钥解码、监听面、限流与探活参数。
func TestProxyConfigMapsEdgeSection(t *testing.T) {
	cfg, err := proxyConfig(validEdgeConfig(), stubResolver{}, metrics.Noop(), nil)
	if err != nil {
		t.Fatalf("映射配置失败: %v", err)
	}
	if len(cfg.TicketKey) != 32 || cfg.TicketKey[0] != 0x01 || cfg.TicketKey[31] != 0x20 {
		t.Fatalf("票据密钥解码不对: %v", cfg.TicketKey)
	}
	if len(cfg.Listeners) != 3 {
		t.Fatalf("监听面数应为 3，实际 %d", len(cfg.Listeners))
	}
	want := []edge.Listener{
		{Name: "ws", Network: edge.NetworkTCP, Address: "127.0.0.1:7100", Carrier: edge.CarrierWSUpgrade},
		{Name: "kcp", Network: edge.NetworkTCP, Address: "127.0.0.1:7101", Carrier: edge.CarrierStreamHello},
		{Name: "udp", Network: edge.NetworkUDP, Address: "127.0.0.1:7102", Carrier: edge.CarrierDatagram},
	}
	for i, w := range want {
		if cfg.Listeners[i] != w {
			t.Fatalf("第 %d 个监听面应为 %+v，实际 %+v", i, w, cfg.Listeners[i])
		}
	}
	if cfg.MaxStreams != 32 || cfg.NewConnRate != 7 || cfg.MaxPerIP != 3 {
		t.Fatalf("限流参数映射不对: %+v", cfg)
	}
	if cfg.IdleTimeout != 45*time.Second || cfg.DialTimeout != 2*time.Second {
		t.Fatalf("时长映射不对: idle=%v dial=%v", cfg.IdleTimeout, cfg.DialTimeout)
	}
	if cfg.Probe.After != 10*time.Second || cfg.Probe.Timeout != 3*time.Second {
		t.Fatalf("探活参数映射不对: %+v", cfg.Probe)
	}
	if _, err := edge.New(cfg); err != nil {
		t.Fatalf("映射出的配置应能构造接入层: %v", err)
	}
}

// TestProxyConfigDefaults 覆盖缺省：frame_service/actor 命名空间与未配的时长交给框架缺省。
func TestProxyConfigDefaults(t *testing.T) {
	svc := validEdgeConfig()
	svc.Edge.FrameService = ""
	svc.Edge.IdleTimeout = ""
	svc.Edge.DialTimeout = ""
	svc.Edge.ProbeAfter = ""
	svc.Edge.ProbeTimeout = ""
	cfg, err := proxyConfig(svc, stubResolver{}, nil, nil)
	if err != nil {
		t.Fatalf("映射配置失败: %v", err)
	}
	if cfg.IdleTimeout != 0 && cfg.IdleTimeout != 60*time.Second {
		t.Fatalf("空闲超时缺省应为框架缺省，实际 %v", cfg.IdleTimeout)
	}
	if cfg.Probe.After != 0 || cfg.Probe.Timeout != 0 {
		t.Fatalf("未配探活参数时应留给框架推导，实际 %+v", cfg.Probe)
	}
}

// TestProxyConfigRejectsTicketKey 覆盖密钥缺失/非法/长度不符即启动失败。
func TestProxyConfigRejectsTicketKey(t *testing.T) {
	cases := []struct {
		name string
		edge *conf.Edge
	}{
		{"缺少 edge 段", nil},
		{"密钥为空", &conf.Edge{Listeners: validEdgeConfig().GetEdge().GetListeners()}},
		{"密钥非 base64", &conf.Edge{
			TicketKey: "!!!not-base64!!!", Listeners: validEdgeConfig().GetEdge().GetListeners()}},
		{"密钥长度不符", &conf.Edge{
			TicketKey: "c2hvcnQ=", Listeners: validEdgeConfig().GetEdge().GetListeners()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := validEdgeConfig()
			svc.Edge = tc.edge
			if _, err := proxyConfig(svc, stubResolver{}, metrics.Noop(), nil); !errors.Is(err, ticket.ErrKeySize) {
				t.Fatalf("应报 ticket.ErrKeySize，实际 %v", err)
			}
		})
	}
}

// TestProxyConfigRejectsBadListener 覆盖监听面非法即启动失败。
func TestProxyConfigRejectsBadListener(t *testing.T) {
	cases := []struct {
		name      string
		listeners []*conf.Edge_Listener
	}{
		{"无监听面", nil},
		{"面名为空", []*conf.Edge_Listener{{Network: conf.Edge_NETWORK_TCP,
			Addr: "127.0.0.1:7100", Carrier: conf.Edge_CARRIER_WS_UPGRADE}}},
		{"网络未配置", []*conf.Edge_Listener{{Name: "ws", Addr: "127.0.0.1:7100",
			Carrier: conf.Edge_CARRIER_WS_UPGRADE}}},
		{"载体未配置", []*conf.Edge_Listener{{Name: "ws", Network: conf.Edge_NETWORK_TCP,
			Addr: "127.0.0.1:7100"}}},
		{"地址为空", []*conf.Edge_Listener{{Name: "ws", Network: conf.Edge_NETWORK_TCP,
			Carrier: conf.Edge_CARRIER_WS_UPGRADE}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := validEdgeConfig()
			svc.Edge.Listeners = tc.listeners
			if _, err := proxyConfig(svc, stubResolver{}, metrics.Noop(), nil); err == nil {
				t.Fatal("非法监听面应报错")
			}
		})
	}
}

// TestProxyConfigRejectsBadDurations 覆盖时长字符串非法即启动失败。
func TestProxyConfigRejectsBadDurations(t *testing.T) {
	svc := validEdgeConfig()
	svc.Edge.IdleTimeout = "不是时长"
	if _, err := proxyConfig(svc, stubResolver{}, metrics.Noop(), nil); err == nil {
		t.Fatal("非法时长应报错")
	}
	svc = validEdgeConfig()
	svc.Edge.ProbeTimeout = "-1s"
	if _, err := proxyConfig(svc, stubResolver{}, metrics.Noop(), nil); err == nil {
		t.Fatal("非正时长应报错")
	}
}

// TestProxyConfigRequiresResolver 覆盖没有 Resolver 即启动失败（fx 缺依赖时也要在映射期暴露）。
func TestProxyConfigRequiresResolver(t *testing.T) {
	if _, err := proxyConfig(validEdgeConfig(), nil, metrics.Noop(), nil); err == nil {
		t.Fatal("Resolver 为空应报错")
	}
}
