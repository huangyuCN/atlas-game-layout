package app

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/server"
	"github.com/huangyuCN/atlas/registry"
)

// fakeRegistrar 是注册器桩：记录注册/注销调用，供断言帧面实例内容与注销时机。
type fakeRegistrar struct {
	registered   []*registry.ServiceInstance
	deregistered []*registry.ServiceInstance
}

// Register 记录注册的实例。
func (f *fakeRegistrar) Register(_ context.Context, si *registry.ServiceInstance) error {
	f.registered = append(f.registered, si)
	return nil
}

// Deregister 记录注销的实例。
func (f *fakeRegistrar) Deregister(_ context.Context, si *registry.ServiceInstance) error {
	f.deregistered = append(f.deregistered, si)
	return nil
}

// frameBootstrap 构造注册帧面实例所需的最小配置（节点 ID + 接入层地址列表）。
// 帧端口不在这里：它们来自帧面服务端已绑定的真实地址（配置 addr 为 0 时由内核分配）。
func frameBootstrap() *conf.Bootstrap {
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "battle", Id: "battle-node-1", Namespace: "test"},
		Battle: &conf.BattleConf{
			EdgeEndpoints: []*battlev1.EdgeEndpoint{
				{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "10.10.9.36:7100"},
				{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: "10.10.9.36:7101"},
				{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UDP, Address: "10.10.9.36:7102"},
			},
		},
	}
}

// newTestFrameFaces 构造三个直连帧面（固定回环端口，测试结束由返回的清理函数关闭）。
func newTestFrameFaces(t *testing.T) (server.FrameFaces, func()) {
	t.Helper()
	kcp, err := serverutil.KCPServer(&configspb.Server_KCP{Addr: "127.0.0.1:19401"})
	if err != nil {
		t.Fatalf("构造 KCP 帧面失败: %v", err)
	}
	udp, err := serverutil.UDPServer(&configspb.Server_UDP{Addr: "127.0.0.1:19402"})
	if err != nil {
		t.Fatalf("构造 UDP 帧面失败: %v", err)
	}
	ws, err := serverutil.WSServer(&configspb.Server_WebSocket{Addr: "127.0.0.1:19403"})
	if err != nil {
		t.Fatalf("构造 WS 帧面失败: %v", err)
	}
	cleanup := func() {
		ctx := context.Background()
		for _, s := range []interface{ Stop(context.Context) error }{kcp, udp, ws} {
			_ = s.Stop(ctx)
		}
	}
	return server.FrameFaces{KCP: kcp, UDP: udp, WS: ws}, cleanup
}

// TestFrameRegistrarDeregister 验证停机时注销同一个实例：注册中心里不留残留帧面实例，
// 否则接入层会解析到已经停掉的节点。
func TestFrameRegistrarDeregister(t *testing.T) {
	faces, cleanup := newTestFrameFaces(t)
	defer cleanup()
	reg := &fakeRegistrar{}
	r, err := NewFrameRegistrar(reg, faces, frameBootstrap())
	if err != nil {
		t.Fatalf("NewFrameRegistrar() 错误 = %v", err)
	}
	ctx := context.Background()
	if err := r.Register(ctx); err != nil {
		t.Fatalf("Register() 错误 = %v", err)
	}
	if err := r.Deregister(ctx); err != nil {
		t.Fatalf("Deregister() 错误 = %v", err)
	}
	if len(reg.deregistered) != 1 {
		t.Fatalf("注销次数 = %d，期望 1", len(reg.deregistered))
	}
	if !reg.deregistered[0].Equal(reg.registered[0]) {
		t.Errorf("注销实例 %v 与注册实例 %v 不一致", reg.deregistered[0], reg.registered[0])
	}
}

// TestNewFrameRegistrarRejectsIncomplete 验证构造期校验：缺注册器、缺节点 ID、
// 缺接入层地址、三个帧面都未启用，一律报错（启动期暴露，不注册一个解析不到或拨不通的实例）。
func TestNewFrameRegistrarRejectsIncomplete(t *testing.T) {
	faces, cleanup := newTestFrameFaces(t)
	defer cleanup()
	noNode := frameBootstrap()
	noNode.Runtime.Id = ""
	noEdge := frameBootstrap()
	noEdge.Battle.EdgeEndpoints = nil
	cases := []struct {
		name  string
		reg   registry.Registrar
		faces server.FrameFaces
		cfg   *conf.Bootstrap
	}{
		{"缺注册器", nil, faces, frameBootstrap()},
		{"缺节点 ID", &fakeRegistrar{}, faces, noNode},
		{"缺接入层地址", &fakeRegistrar{}, faces, noEdge},
		{"三个帧面都未启用", &fakeRegistrar{}, server.FrameFaces{}, frameBootstrap()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewFrameRegistrar(c.reg, c.faces, c.cfg); err == nil {
				t.Fatal("期望构造失败，实际为 nil")
			}
		})
	}
}

// TestFrameRegistrarRegisterContent 验证注册的帧面实例内容（规格 §2.1）：
// 服务名 battle-frame、元数据带 node_id 与三面端口、端点主机取接入层主机。
func TestFrameRegistrarRegisterContent(t *testing.T) {
	faces, cleanup := newTestFrameFaces(t)
	defer cleanup()
	reg := &fakeRegistrar{}
	r, err := NewFrameRegistrar(reg, faces, frameBootstrap())
	if err != nil {
		t.Fatalf("NewFrameRegistrar() 错误 = %v", err)
	}
	if err := r.Register(context.Background()); err != nil {
		t.Fatalf("Register() 错误 = %v", err)
	}
	if len(reg.registered) != 1 {
		t.Fatalf("注册次数 = %d，期望 1", len(reg.registered))
	}
	got := reg.registered[0]
	if got.Name != consts.ServiceBattleFrame {
		t.Errorf("服务名 = %q，期望 %q", got.Name, consts.ServiceBattleFrame)
	}
	if got.ID != "battle-node-1" {
		t.Errorf("实例 ID = %q，期望与 actor NodeID 同源（battle-node-1）", got.ID)
	}
	// 元数据必须逐键等于契约（node_id + 三面端口）；host 省略——端点主机是唯一主机来源。
	wantMeta := map[string]string{
		consts.FrameMetaNodeID:  "battle-node-1",
		consts.FrameMetaPortKCP: "19401",
		consts.FrameMetaPortUDP: "19402",
		consts.FrameMetaPortWS:  "19403",
	}
	if len(got.Metadata) != len(wantMeta) {
		t.Errorf("元数据键数 = %d（%v），期望 %d", len(got.Metadata), got.Metadata, len(wantMeta))
	}
	for k, want := range wantMeta {
		if got.Metadata[k] != want {
			t.Errorf("元数据 %s = %q，期望 %q", k, got.Metadata[k], want)
		}
	}
	// 端点是本节点帧面地址：scheme 取传输面、主机取接入层主机（同机部署，接入层据此拨号）。
	wantEP := []string{
		serverutil.SchemeKCP + "://10.10.9.36:19401",
		serverutil.SchemeUDP + "://10.10.9.36:19402",
		serverutil.SchemeWS + "://10.10.9.36:19403",
	}
	gotEP := append([]string(nil), got.Endpoints...)
	sort.Strings(gotEP)
	sort.Strings(wantEP)
	if len(gotEP) != len(wantEP) {
		t.Fatalf("端点 = %v，期望 %v", got.Endpoints, wantEP)
	}
	for i := range wantEP {
		if gotEP[i] != wantEP[i] {
			t.Errorf("端点[%d] = %q，期望 %q", i, gotEP[i], wantEP[i])
		}
	}
}

// TestFrameRegistrarUsesAdvertiseHost 验证显式配置 frame_advertise_host 时注册端点用该主机
// （**不是** edge_endpoints 的接入层主机）：跨机部署下接入层据此拨本节点帧端口。
func TestFrameRegistrarUsesAdvertiseHost(t *testing.T) {
	faces, cleanup := newTestFrameFaces(t)
	defer cleanup()
	cfg := frameBootstrap()
	cfg.Battle.FrameAdvertiseHost = strPtr("battle-1.internal")
	reg := &fakeRegistrar{}
	r, err := NewFrameRegistrar(reg, faces, cfg)
	if err != nil {
		t.Fatalf("NewFrameRegistrar() 错误 = %v", err)
	}
	if err := r.Register(context.Background()); err != nil {
		t.Fatalf("Register() 错误 = %v", err)
	}
	got := reg.registered[0]
	want := []string{
		serverutil.SchemeKCP + "://battle-1.internal:19401",
		serverutil.SchemeUDP + "://battle-1.internal:19402",
		serverutil.SchemeWS + "://battle-1.internal:19403",
	}
	assertEndpoints(t, got.Endpoints, want)
	// 端口仍取帧面服务端已绑定端口：元数据不受主机配置影响。
	if got.Metadata[consts.FrameMetaPortWS] != "19403" {
		t.Errorf("元数据 %s = %q，期望 19403", consts.FrameMetaPortWS, got.Metadata[consts.FrameMetaPortWS])
	}
}

// TestFrameRegistrarFallsBackToEdgeHost 验证未配置 frame_advertise_host（字段缺失）时
// 回落到 edge_endpoints 的主机（同机部署口径），且不报错。
func TestFrameRegistrarFallsBackToEdgeHost(t *testing.T) {
	faces, cleanup := newTestFrameFaces(t)
	defer cleanup()
	cfg := frameBootstrap()
	if cfg.Battle.FrameAdvertiseHost != nil {
		t.Fatal("前置条件：frame_advertise_host 应为未配置")
	}
	reg := &fakeRegistrar{}
	r, err := NewFrameRegistrar(reg, faces, cfg)
	if err != nil {
		t.Fatalf("NewFrameRegistrar() 错误 = %v", err)
	}
	if err := r.Register(context.Background()); err != nil {
		t.Fatalf("Register() 错误 = %v", err)
	}
	for _, ep := range reg.registered[0].Endpoints {
		u, err := url.Parse(ep)
		if err != nil {
			t.Fatalf("端点 %q 解析失败: %v", ep, err)
		}
		if u.Hostname() != "10.10.9.36" {
			t.Errorf("端点 %q 的主机 = %q，期望回落为 edge_endpoints 主机 10.10.9.36", ep, u.Hostname())
		}
	}
}

// TestNewFrameRegistrarRejectsBadAdvertiseHost 验证显式配置的帧面主机非法即装配期报错
// （空串、纯空白、含端口都不接受——不静默截断、不静默回落）。
func TestNewFrameRegistrarRejectsBadAdvertiseHost(t *testing.T) {
	faces, cleanup := newTestFrameFaces(t)
	defer cleanup()
	for _, raw := range []string{"", "   ", "10.10.9.36:9401"} {
		t.Run(fmt.Sprintf("%q", raw), func(t *testing.T) {
			cfg := frameBootstrap()
			cfg.Battle.FrameAdvertiseHost = strPtr(raw)
			if _, err := NewFrameRegistrar(&fakeRegistrar{}, faces, cfg); err == nil {
				t.Fatalf("frame_advertise_host=%q 期望装配期报错，实际为 nil", raw)
			}
		})
	}
}

// assertEndpoints 断言端点集合逐项相等（与期望同为「scheme://host:port」形态，排序后比较）。
func assertEndpoints(t *testing.T, got, want []string) {
	t.Helper()
	gotEP := append([]string(nil), got...)
	sort.Strings(gotEP)
	sort.Strings(want)
	if len(gotEP) != len(want) {
		t.Fatalf("端点 = %v，期望 %v", got, want)
	}
	for i := range want {
		if gotEP[i] != want[i] {
			t.Errorf("端点[%d] = %q，期望 %q", i, gotEP[i], want[i])
		}
	}
}

// strPtr 返回字符串指针（proto3 optional 字段的「显式配置」形态）。
func strPtr(s string) *string { return &s }
