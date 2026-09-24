package serverutil

import (
	"testing"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
)

// 本文件逐节点验证唯一缺省约定的三态行为（docs/config.md §1/§4）：
//
//	① 节点缺失          ≡ 其全部参数为空
//	② 节点在但必需参数空 ⇒ 该能力**不启用**（与①同结果，不是「启用 + 全默认」）
//	③ 必需参数非空       ⇒ 该能力启用
//
// 覆盖 pkg/serverutil 手上的全部节点：server.tcp/websocket/kcp/udp（addr 为必需参数）、
// server.http（常驻面，无必需参数）、server.grpc（edge_addr/internal_addr 各管一个面）、
// server.*.tls（enabled+cert/key 为必需项）。

// TestNodeDefaultsTCP 验证 server.tcp 三态：节点缺失/addr 空 = 不启用，addr 非空 = 启用。
func TestNodeDefaultsTCP(t *testing.T) {
	if s, err := TCPServer(nil); err != nil || s != nil {
		t.Errorf("① 节点缺失：期望不构造（nil），实际 %v/%v", s, err)
	}
	if s, err := TCPServer(&configspb.Server_TCP{}); err != nil || s != nil {
		t.Errorf("② addr 空：期望不构造（nil）——节点在 ≠ 启用，实际 %v/%v", s, err)
	}
	if s, err := TCPServer(&configspb.Server_TCP{Addr: "127.0.0.1:0"}); err != nil || s == nil {
		t.Errorf("③ addr 非空：期望构造，实际 %v/%v", s, err)
	}
}

// TestNodeDefaultsWebSocket 验证 server.websocket 三态（语义同 TCP）。
func TestNodeDefaultsWebSocket(t *testing.T) {
	if s, err := WSServer(nil); err != nil || s != nil {
		t.Errorf("① 节点缺失：期望不构造（nil），实际 %v/%v", s, err)
	}
	if s, err := WSServer(&configspb.Server_WebSocket{}); err != nil || s != nil {
		t.Errorf("② addr 空：期望不构造（nil），实际 %v/%v", s, err)
	}
	if s, err := WSServer(&configspb.Server_WebSocket{Addr: "127.0.0.1:0"}); err != nil || s == nil {
		t.Errorf("③ addr 非空：期望构造，实际 %v/%v", s, err)
	}
}

// TestNodeDefaultsKCP 验证 server.kcp 三态（语义同 TCP）。
func TestNodeDefaultsKCP(t *testing.T) {
	if s, err := KCPServer(nil); err != nil || s != nil {
		t.Errorf("① 节点缺失：期望不构造（nil），实际 %v/%v", s, err)
	}
	if s, err := KCPServer(&configspb.Server_KCP{}); err != nil || s != nil {
		t.Errorf("② addr 空：期望不构造（nil），实际 %v/%v", s, err)
	}
	if s, err := KCPServer(&configspb.Server_KCP{Addr: "127.0.0.1:0"}); err != nil || s == nil {
		t.Errorf("③ addr 非空：期望构造，实际 %v/%v", s, err)
	}
}

// TestNodeDefaultsUDP 验证 server.udp 三态（语义同 TCP）。
func TestNodeDefaultsUDP(t *testing.T) {
	if s, err := UDPServer(nil); err != nil || s != nil {
		t.Errorf("① 节点缺失：期望不构造（nil），实际 %v/%v", s, err)
	}
	if s, err := UDPServer(&configspb.Server_UDP{}); err != nil || s != nil {
		t.Errorf("② addr 空：期望不构造（nil），实际 %v/%v", s, err)
	}
	if s, err := UDPServer(&configspb.Server_UDP{Addr: "127.0.0.1:0"}); err != nil || s == nil {
		t.Errorf("③ addr 非空：期望构造，实际 %v/%v", s, err)
	}
}

// TestNodeDefaultsHTTP 验证 server.http 是**常驻面**（模板健康检查）：没有必需参数，
// 节点缺失或 addr 空都仍启用（addr 空 = 底层默认随机端口），这是分类表里的显式语义。
func TestNodeDefaultsHTTP(t *testing.T) {
	if s, err := HTTPServer(nil, nil, nil); err != nil || s == nil {
		t.Errorf("① 节点缺失：期望仍构造（常驻面），实际 %v/%v", s, err)
	}
	if s, err := HTTPServer(&configspb.Server_HTTP{}, nil, nil); err != nil || s == nil {
		t.Errorf("② addr 空：期望仍构造（底层默认随机端口），实际 %v/%v", s, err)
	}
	if s, err := HTTPServer(&configspb.Server_HTTP{Addr: "127.0.0.1:0"}, nil, nil); err != nil || s == nil {
		t.Errorf("③ addr 非空：期望构造，实际 %v/%v", s, err)
	}
}

// TestNodeDefaultsGRPCFaces 验证 server.grpc 的两面各自判启用：地址为空的面不启用，
// 另一面不受影响（两面独立地址/冲突/隔离的完整断言见 grpc_faces_test.go）。
func TestNodeDefaultsGRPCFaces(t *testing.T) {
	if f, err := GRPCServers(nil, nil); err != nil || f.Edge != nil || f.Internal != nil {
		t.Errorf("① 节点缺失：期望两面都不启用，实际 %+v/%v", f, err)
	}
	if f, err := GRPCServers(&configspb.Server_GRPC{}, nil); err != nil || f.Edge != nil || f.Internal != nil {
		t.Errorf("② 节点在但两面地址空：期望两面都不启用，实际 %+v/%v", f, err)
	}
	f, err := GRPCServers(&configspb.Server_GRPC{EdgeAddr: "127.0.0.1:0", InternalAddr: "127.0.0.1:0"}, nil)
	if err != nil || f.Edge == nil || f.Internal == nil {
		t.Errorf("③ 两面地址非空：期望两面都启用，实际 %+v/%v", f, err)
	}
}

// TestNodeDefaultsTLS 验证 server.*.tls 三态：未启用（缺失或 enabled=false）⇒ 明文（不追加选项）；
// 启用后 cert_file/key_file 是必需项，缺失即装配期报错。
func TestNodeDefaultsTLS(t *testing.T) {
	if conf, err := TLSConfig(nil); err != nil || conf != nil {
		t.Errorf("① 节点缺失：期望明文（nil 配置），实际 %v/%v", conf, err)
	}
	if conf, err := TLSConfig(&configspb.Server_TLS{}); err != nil || conf != nil {
		t.Errorf("② enabled=false：期望明文（nil 配置），实际 %v/%v", conf, err)
	}
	if _, err := TLSConfig(&configspb.Server_TLS{Enabled: true}); err == nil {
		t.Error("③ 启用 TLS 但缺 cert_file/key_file：期望装配期报错")
	}
	pki := newTestPKI(t)
	conf, err := TLSConfig(&configspb.Server_TLS{Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey})
	if err != nil || conf == nil {
		t.Errorf("③ 启用 TLS 且证书齐备：期望构造出 TLS 配置，实际 %v/%v", conf, err)
	}
}

// TestNodeDefaultsOptionalParams 验证可选参数为空 = 不覆盖底层默认（启用节点上「参数空」的表现）：
// 只配必需参数时只追加地址选项，其余字段一个都不追加。
func TestNodeDefaultsOptionalParams(t *testing.T) {
	if opts, err := GRPCOptions(&configspb.Server_GRPC{EdgeAddr: "127.0.0.1:0"}, "127.0.0.1:0"); err != nil || len(opts) != 1 {
		t.Errorf("仅配 edge_addr 时只应追加地址选项，实际 %d 个/%v", len(opts), err)
	}
	if opts, err := TCPOptions(&configspb.Server_TCP{Addr: "127.0.0.1:0"}); err != nil || len(opts) != 1 {
		t.Errorf("仅配 tcp.addr 时只应追加地址选项，实际 %d 个/%v", len(opts), err)
	}
}
