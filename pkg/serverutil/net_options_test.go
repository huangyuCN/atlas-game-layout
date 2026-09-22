package serverutil

import (
	"context"
	"testing"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/transport"
)

// TestNetOptionsUnset 验证四协议在「未配置任何字段」时不追加选项（保持底层默认）。
func TestNetOptionsUnset(t *testing.T) {
	if opts, err := TCPOptions(&configspb.Server_TCP{}); err != nil || len(opts) != 0 {
		t.Fatalf("tcp 未配置字段时不应追加选项，实际 %d/%v", len(opts), err)
	}
	if opts, err := WSOptions(&configspb.Server_WebSocket{}); err != nil || len(opts) != 0 {
		t.Fatalf("websocket 未配置字段时不应追加选项，实际 %d/%v", len(opts), err)
	}
	if opts, err := KCPOptions(&configspb.Server_KCP{}); err != nil || len(opts) != 0 {
		t.Fatalf("kcp 未配置字段时不应追加选项，实际 %d/%v", len(opts), err)
	}
	if opts, err := UDPOptions(&configspb.Server_UDP{}); err != nil || len(opts) != 0 {
		t.Fatalf("udp 未配置字段时不应追加选项，实际 %d/%v", len(opts), err)
	}
	// 配置节缺失（nil）同样不追加：调用方按「节 nil = 协议不启用」处理。
	if opts, err := TCPOptions(nil); err != nil || len(opts) != 0 {
		t.Fatalf("tcp 配置缺失时不应追加选项，实际 %d/%v", len(opts), err)
	}
}

// TestNetOptionsDurations 验证四协议时长字段：合法值各追加一个选项，非法值启动期报错。
func TestNetOptionsDurations(t *testing.T) {
	for _, bad := range []struct {
		name string
		run  func() error
	}{
		{"tcp.keep_alive", func() error { _, err := TCPOptions(&configspb.Server_TCP{KeepAlive: "abc"}); return err }},
		{"tcp.idle_timeout 非正", func() error { _, err := TCPOptions(&configspb.Server_TCP{IdleTimeout: "0s"}); return err }},
		{"websocket.write_timeout 无单位", func() error {
			_, err := WSOptions(&configspb.Server_WebSocket{WriteTimeout: "10"})
			return err
		}},
		{"kcp.idle_timeout 负数", func() error { _, err := KCPOptions(&configspb.Server_KCP{IdleTimeout: "-1s"}); return err }},
		{"udp.idle_timeout 乱码", func() error { _, err := UDPOptions(&configspb.Server_UDP{IdleTimeout: "x"}); return err }},
	} {
		if err := bad.run(); err == nil {
			t.Fatalf("%s 非法时长期望报错，实际为 nil", bad.name)
		}
	}

	opts, err := TCPOptions(&configspb.Server_TCP{KeepAlive: "30s", IdleTimeout: "60s", WriteTimeout: "10s"})
	if err != nil || len(opts) != 3 {
		t.Fatalf("三个时长应各追加一个选项，实际 %d/%v", len(opts), err)
	}
}

// TestNetOptionsIntValidation 验证数值字段：0 = 交给底层默认（不追加），负数报错
// （静默忽略负数等于「配了不生效」）。
func TestNetOptionsIntValidation(t *testing.T) {
	for _, bad := range []struct {
		name string
		run  func() error
	}{
		{"tcp.max_conns", func() error { _, err := TCPOptions(&configspb.Server_TCP{MaxConns: -1}); return err }},
		{"websocket.pool_size", func() error { _, err := WSOptions(&configspb.Server_WebSocket{PoolSize: -1}); return err }},
		{"websocket.read_limit", func() error { _, err := WSOptions(&configspb.Server_WebSocket{ReadLimit: -1}); return err }},
		{"kcp.mtu", func() error { _, err := KCPOptions(&configspb.Server_KCP{Mtu: -1}); return err }},
		{"udp.max_peers", func() error { _, err := UDPOptions(&configspb.Server_UDP{MaxPeers: -1}); return err }},
	} {
		if err := bad.run(); err == nil {
			t.Fatalf("%s 为负期望报错，实际为 nil", bad.name)
		}
	}
	// 0 = 交给底层默认：不追加、不报错。
	if opts, err := TCPOptions(&configspb.Server_TCP{MaxConns: 0, ReadBuffer: 0}); err != nil || len(opts) != 0 {
		t.Fatalf("0 值不应追加选项，实际 %d/%v", len(opts), err)
	}
}

// TestWSOptionsBuffer 验证 WS 读写缓冲：底层按侧独立生效，故只配一侧也合法；
// 两侧都配只追加一个选项（底层 BufferSize 吃两个值）；负数报错。
func TestWSOptionsBuffer(t *testing.T) {
	if opts, err := WSOptions(&configspb.Server_WebSocket{ReadBuffer: 4096}); err != nil || len(opts) != 1 {
		t.Fatalf("只配 read_buffer 应追加一个选项，实际 %d/%v", len(opts), err)
	}
	if opts, err := WSOptions(&configspb.Server_WebSocket{WriteBuffer: 4096}); err != nil || len(opts) != 1 {
		t.Fatalf("只配 write_buffer 应追加一个选项，实际 %d/%v", len(opts), err)
	}
	if opts, err := WSOptions(&configspb.Server_WebSocket{ReadBuffer: 4096, WriteBuffer: 8192}); err != nil || len(opts) != 1 {
		t.Fatalf("成对配置应追加一个选项，实际 %d/%v", len(opts), err)
	}
	if _, err := WSOptions(&configspb.Server_WebSocket{ReadBuffer: -1}); err == nil {
		t.Fatal("read_buffer 为负期望报错")
	}
}

// TestKCPOptionsPairsAndPresets 验证 KCP 的成对参数与预设/加密口令映射：
// FEC 与窗口必须成对；预设（fast/lan profile）与 block_crypt 在有值时各追加一个选项。
func TestKCPOptionsPairsAndPresets(t *testing.T) {
	if _, err := KCPOptions(&configspb.Server_KCP{FecDataShards: 10}); err == nil {
		t.Fatal("只配 fec_data_shards 期望报错")
	}
	if _, err := KCPOptions(&configspb.Server_KCP{WindowRcv: 128}); err == nil {
		t.Fatal("只配 window_rcv 期望报错")
	}
	opts, err := KCPOptions(&configspb.Server_KCP{
		FecDataShards: 10, FecParityShards: 3, WindowSnd: 128, WindowRcv: 128,
		Mtu: 1400, FastProfile: true, LanProfile: true, BlockCrypt: "0123456789abcdef0123456789abcdef",
	})
	if err != nil {
		t.Fatalf("KCPOptions() 错误 = %v", err)
	}
	// FEC + 窗口 + MTU + 两个预设 + 加密口令 = 6 个选项。
	if len(opts) != 6 {
		t.Fatalf("应追加 6 个选项，实际 %d", len(opts))
	}
}

// TestNetOptionsTLS 验证 TCP/WebSocket 的 TLS 映射（KCP/UDP 无 TLS 能力，故不提供字段）。
func TestNetOptionsTLS(t *testing.T) {
	pki := newTestPKI(t)
	opts, err := TCPOptions(&configspb.Server_TCP{
		Tls: &configspb.Server_TLS{Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey},
	})
	if err != nil || len(opts) != 1 {
		t.Fatalf("启用 TLS 应追加一个选项，实际 %d/%v", len(opts), err)
	}
	if _, err := WSOptions(&configspb.Server_WebSocket{Tls: &configspb.Server_TLS{Enabled: true}}); err == nil {
		t.Fatal("启用 TLS 但缺证书期望报错")
	}
}

// TestWSServerAppliesPath 验证 path 参数真的落到服务端（此前该参数完全没被使用）：
// 端点路径即配置的挂载路径。
func TestWSServerAppliesPath(t *testing.T) {
	srv, err := WSServer(&configspb.Server_WebSocket{Addr: "127.0.0.1:0", Path: "/ws"})
	if err != nil {
		t.Fatalf("WSServer() 错误 = %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	ep, err := srv.Endpoint()
	if err != nil {
		t.Fatalf("Endpoint() 错误 = %v", err)
	}
	if ep.Path != "/ws" || ep.Scheme != "ws" {
		t.Fatalf("WS 端点 = %s，期望 ws://<host>/ws", ep)
	}
}

// TestNetServers 验证四个协议的构造器可用且实现 transport.Server。
func TestNetServers(t *testing.T) {
	tcpSrv, err := TCPServer(&configspb.Server_TCP{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("TCPServer() 错误 = %v", err)
	}
	wsSrv, err := WSServer(&configspb.Server_WebSocket{Addr: "127.0.0.1:0", Path: "/ws"})
	if err != nil {
		t.Fatalf("WSServer() 错误 = %v", err)
	}
	kcpSrv, err := KCPServer(&configspb.Server_KCP{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("KCPServer() 错误 = %v", err)
	}
	udpSrv, err := UDPServer(&configspb.Server_UDP{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("UDPServer() 错误 = %v", err)
	}
	for name, srv := range map[string]transport.Server{
		"tcp": tcpSrv, "websocket": wsSrv, "kcp": kcpSrv, "udp": udpSrv,
	} {
		if srv == nil {
			t.Fatalf("%s 构造结果不应为 nil", name)
		}
	}
}

// TestNetServersDisabledProtocols 验证协议未配置（该协议不启用）时返回 nil 且不报错：
// 装配层按「nil = 未启用」决定不进启停组；此处若报错，裁剪协议的部署形态会直接起不来。
func TestNetServersDisabledProtocols(t *testing.T) {
	if srv, err := TCPServer(nil); err != nil || srv != nil {
		t.Fatalf("未配置 tcp 应返回 (nil, nil)，实际 %v/%v", srv, err)
	}
	if srv, err := WSServer(nil); err != nil || srv != nil {
		t.Fatalf("未配置 websocket 应返回 (nil, nil)，实际 %v/%v", srv, err)
	}
	if srv, err := KCPServer(nil); err != nil || srv != nil {
		t.Fatalf("未配置 kcp 应返回 (nil, nil)，实际 %v/%v", srv, err)
	}
	if srv, err := UDPServer(nil); err != nil || srv != nil {
		t.Fatalf("未配置 udp 应返回 (nil, nil)，实际 %v/%v", srv, err)
	}
}
