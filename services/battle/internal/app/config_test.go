package app

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
)

// testEdgeEndpoints 返回三面接入层地址（与 services/edge 的监听面一致：
// ws 走 tcp、kcp 与 udp 各走一个 udp 端口——单地址无法让 SDK 知道该拨哪个端口）。
func testEdgeEndpoints() []*battlev1.EdgeEndpoint {
	return []*battlev1.EdgeEndpoint{
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.example.com:7100"},
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: "edge.example.com:7101"},
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UDP, Address: "edge.example.com:7102"},
	}
}

// testKeyBase64 返回 32 字节测试密钥的 base64 编码（与生产同形）。
func testKeyBase64() string {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return base64.StdEncoding.EncodeToString(key)
}

// TestNewActorConfigIssuesTicketConfig 验证出票配置装配：
// 密钥 base64 解码为 32 字节、ticket_ttl 缺省 120s 且可显式覆盖、接入层地址列表逐字透传，
// 其余战斗参数保持默认值。
func TestNewActorConfigIssuesTicketConfig(t *testing.T) {
	cfg := &conf.Bootstrap{Battle: &conf.BattleConf{
		TicketKey:     testKeyBase64(),
		EdgeEndpoints: testEdgeEndpoints(),
	}}
	got, err := newActorConfig(cfg)
	if err != nil {
		t.Fatalf("newActorConfig: %v", err)
	}
	if len(got.TicketKey) != 32 {
		t.Errorf("TicketKey 长度 = %d, 期望 32", len(got.TicketKey))
	}
	if got.TicketTTL != actor.DefaultTicketTTL {
		t.Errorf("TicketTTL = %v, 期望缺省 %v", got.TicketTTL, actor.DefaultTicketTTL)
	}
	if len(got.EdgeEndpoints) != 3 {
		t.Fatalf("EdgeEndpoints 面数 = %d, 期望 3", len(got.EdgeEndpoints))
	}
	for i, want := range testEdgeEndpoints() {
		ep := got.EdgeEndpoints[i]
		if ep.GetTransport() != want.GetTransport() || ep.GetAddress() != want.GetAddress() {
			t.Errorf("EdgeEndpoints[%d] = %s/%s, 期望 %s/%s", i,
				ep.GetTransport(), ep.GetAddress(), want.GetTransport(), want.GetAddress())
		}
	}
	if got.TickInterval != actor.DefaultTickInterval || got.TrackLen != actor.DefaultTrackLen {
		t.Errorf("默认战斗参数被改动: %+v", got)
	}

	cfg.Battle.TicketTtl = "45s"
	got, err = newActorConfig(cfg)
	if err != nil {
		t.Fatalf("newActorConfig（显式 ttl）: %v", err)
	}
	if got.TicketTTL != 45*time.Second {
		t.Errorf("TicketTTL = %v, 期望 45s", got.TicketTTL)
	}
}

// TestNewActorConfigAcceptsSingleEdgeFace 验证「不要求三面齐全」：
// 只开 ws 的部署（如内网灰度只放行 WS）同样能装配成功，缺面只影响用该面的客户端。
func TestNewActorConfigAcceptsSingleEdgeFace(t *testing.T) {
	cfg := &conf.Bootstrap{Battle: &conf.BattleConf{
		TicketKey: testKeyBase64(),
		EdgeEndpoints: []*battlev1.EdgeEndpoint{
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.example.com:7100"},
		},
	}}
	got, err := newActorConfig(cfg)
	if err != nil {
		t.Fatalf("单面配置应装配成功: %v", err)
	}
	if len(got.EdgeEndpoints) != 1 || got.EdgeEndpoints[0].GetAddress() != "edge.example.com:7100" {
		t.Errorf("EdgeEndpoints = %v, 期望只含 ws 一面", got.EdgeEndpoints)
	}
}

// TestNewActorConfigRejectsBadTicketKey 验证密钥缺失/非法 base64/长度不符/ttl 非法
// 一律装配失败（不提供默认密钥，也不静默接受 0 时长）。
func TestNewActorConfigRejectsBadTicketKey(t *testing.T) {
	ok := testEdgeEndpoints()
	cases := []struct {
		name    string
		battle  *conf.BattleConf
		keySize bool // 错误是否应可用 errors.Is(err, ticket.ErrKeySize) 判定
	}{
		{name: "配置段缺失", battle: nil, keySize: false},
		{name: "密钥缺失", battle: &conf.BattleConf{EdgeEndpoints: ok}, keySize: true},
		{name: "非法 base64", battle: &conf.BattleConf{TicketKey: "!!!", EdgeEndpoints: ok}, keySize: true},
		{
			name:    "长度不足",
			battle:  &conf.BattleConf{TicketKey: base64.StdEncoding.EncodeToString(make([]byte, 16)), EdgeEndpoints: ok},
			keySize: true,
		},
		{name: "ttl 非法", battle: &conf.BattleConf{TicketKey: testKeyBase64(), TicketTtl: "不是时长", EdgeEndpoints: ok}},
		{name: "ttl 非正", battle: &conf.BattleConf{TicketKey: testKeyBase64(), TicketTtl: "-1s", EdgeEndpoints: ok}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newActorConfig(&conf.Bootstrap{Battle: tc.battle})
			if err == nil {
				t.Fatal("期望装配失败，实际为 nil（说明回落了默认值）")
			}
			if tc.keySize && !errors.Is(err, ticket.ErrKeySize) {
				t.Errorf("错误应可用 errors.Is(err, ticket.ErrKeySize) 判定，实际 %v", err)
			}
		})
	}
}

// TestNewActorConfigRejectsBadEdgeEndpoints 验证接入层地址列表的强校验（缺一即启动失败）：
// 一面都没有、传输面未指定、地址为空、同一面重复配置——都会下发一张连不上的局
// （或让 SDK 猜端口），故一律在装配期拒绝。
func TestNewActorConfigRejectsBadEdgeEndpoints(t *testing.T) {
	cases := []struct {
		name      string
		endpoints []*battlev1.EdgeEndpoint
	}{
		{name: "一面都没有", endpoints: nil},
		{name: "空列表", endpoints: []*battlev1.EdgeEndpoint{}},
		{
			name:      "传输面未指定",
			endpoints: []*battlev1.EdgeEndpoint{{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UNSPECIFIED, Address: "e:1"}},
		},
		{
			name:      "地址为空",
			endpoints: []*battlev1.EdgeEndpoint{{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS}},
		},
		{
			name: "同一面重复",
			endpoints: []*battlev1.EdgeEndpoint{
				{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "e:1"},
				{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "e:2"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &conf.Bootstrap{Battle: &conf.BattleConf{
				TicketKey:     testKeyBase64(),
				EdgeEndpoints: tc.endpoints,
			}}
			if _, err := newActorConfig(cfg); err == nil {
				t.Fatal("期望装配失败，实际为 nil")
			}
		})
	}
}

// TestBattleFrameAdvertiseHostPresence 验证配置解析的「显式存在」语义（frame_advertise_host 是
// proto3 optional 字段）：写空串时字段**存在**（指针非 nil，装配期据此报错、不静默回落），
// 整项缺失时指针为 nil（装配期回落 edge_endpoints 主机）。两条语义都依赖显式存在，
// 故在真实 YAML → protojson 路径上钉住。
func TestBattleFrameAdvertiseHostPresence(t *testing.T) {
	const head = `runtime:
  name: battle
  id: battle-test
  namespace: test
battle:
  ticket_key: "%s"
  edge_endpoints:
    - transport: EDGE_TRANSPORT_WS
      address: "10.10.9.36:7100"
`
	cases := []struct {
		name    string
		line    string // battle 段里追加的一行（空 = 整项缺失）
		present bool   // 期望指针是否非 nil
	}{
		{"显式空串", `  frame_advertise_host: ""`, true},
		{"显式主机", `  frame_advertise_host: "battle-1.internal"`, true},
		{"整项缺失", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			data := fmt.Sprintf(head, testKeyBase64()) + tc.line + "\n"
			if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
				t.Fatalf("写临时配置失败: %v", err)
			}
			var cfg conf.Bootstrap
			if err := config.Load(path, &cfg); err != nil {
				t.Fatalf("config.Load() 错误 = %v", err)
			}
			got := cfg.GetBattle().FrameAdvertiseHost != nil
			if got != tc.present {
				t.Fatalf("frame_advertise_host 存在性 = %v，期望 %v（nil 表示回落 edge_endpoints 主机）", got, tc.present)
			}
		})
	}
}
