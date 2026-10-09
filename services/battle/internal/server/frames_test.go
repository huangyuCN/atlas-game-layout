package server

import (
	"strings"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
)

// TestNewFrameOpsCoversClientOps 验证帧 op 服务端覆盖 BattleServiceRouteTable 的全部
// access=CLIENT op（逐一注册，与网关同口径）；INTERNAL op 不进帧面（可信面不得暴露给直连客户端）。
func TestNewFrameOpsCoversClientOps(t *testing.T) {
	ops, err := NewFrameOps(nil, testRecorder(), testTicketKey())
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	registered := make(map[string]bool)
	if err := ops.Each(func(entry relay.RouteEntry) error {
		registered[entry.Operation] = true
		return nil
	}); err != nil {
		t.Fatalf("Each: %v", err)
	}
	client, internal := 0, 0
	for op, entry := range battlev1.BattleServiceRouteTable {
		if entry.Access == relay.AccessClient {
			client++
			if !registered[op] {
				t.Errorf("CLIENT op %s 未注册到帧面", op)
			}
			continue
		}
		internal++
		if registered[op] {
			t.Errorf("INTERNAL op %s 不应注册到帧面（可信面不得暴露给直连客户端）", op)
		}
	}
	if client == 0 || internal == 0 || len(registered) != client {
		t.Fatalf("注册集合异常: client=%d internal=%d registered=%d", client, internal, len(registered))
	}
}

// TestNewFrameOpsRejectsUnknownOperation 验证未注册 op 不命中路由表（装配期不一致不得静默投递）。
func TestNewFrameOpsRejectsUnknownOperation(t *testing.T) {
	ops, err := NewFrameOps(nil, testRecorder(), testTicketKey())
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	if _, ok := ops.Lookup("/battle.v1.BattleService/NoSuchOp"); ok {
		t.Fatal("未注册 op 竟然命中路由表")
	}
}

// TestNewFrameServersEnabledFaces 验证按配置构造帧面：启用的面非 nil（具体类型与值组双路），
// 未声明的面为 nil（不监听、不注册、不进启停组）。
func TestNewFrameServersEnabledFaces(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{
		Kcp:       &configspb.Server_KCP{Addr: "127.0.0.1:0"},
		Websocket: &configspb.Server_WebSocket{Addr: "127.0.0.1:0"},
	}}
	ops, err := NewFrameOps(nil, testRecorder(), testTicketKey())
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	servers, err := NewFrameServers(cfg, ops, testPolicy(), testRecorder())
	if err != nil {
		t.Fatalf("NewFrameServers: %v", err)
	}
	if servers.Faces.KCP == nil || servers.Faces.WS == nil {
		t.Fatalf("启用的帧面为 nil: %+v", servers.Faces)
	}
	if servers.Faces.UDP != nil {
		t.Fatal("未声明的 UDP 帧面不应构造")
	}
	if servers.KCP == nil || servers.WS == nil || servers.UDP != nil {
		t.Fatalf("值组装配不符: kcp=%v ws=%v udp=%v", servers.KCP, servers.WS, servers.UDP)
	}
}

// TestNewFrameServersRejectsOversizeBody 验证「单包 ≤16 KiB」契约在装配期生效：显式把
// max_body_size 配成大于契约值即启动失败（配大了不会生效，只会让大回执在接入层被丢弃）。
func TestNewFrameServersRejectsOversizeBody(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{
		Udp: &configspb.Server_UDP{Addr: "127.0.0.1:0", MaxBodySize: 1 << 20},
	}}
	ops, err := NewFrameOps(nil, testRecorder(), testTicketKey())
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	if _, err := NewFrameServers(cfg, ops, testPolicy(), testRecorder()); err == nil ||
		!strings.Contains(err.Error(), "单包上限") {
		t.Fatalf("超限的 server.udp.max_body_size 应让装配失败: %v", err)
	}
}
