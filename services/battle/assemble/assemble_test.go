package assemble

import (
	"errors"
	"strings"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battleactor "github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas/namespace"
)

// TestNewBootstrap 验证 Options 到 *conf.Bootstrap 的映射与监听地址缺省。
func TestNewBootstrap(t *testing.T) {
	opts := Options{
		NodeID:        "battle-it",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
		MongoURI:      "mongodb://127.0.0.1:27017",
		MongoDB:       "battle_it",
		Namespace:     "p5-a",
		TicketKey:     "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
		TicketTTL:     "45s",
		EdgeEndpoints: testEdgeEndpoints(),
	}
	cfg, err := newBootstrap(opts)
	if err != nil {
		t.Fatalf("newBootstrap: %v", err)
	}

	if got := cfg.GetRuntime().GetName(); got != "battle" {
		t.Errorf("runtime.name = %q, 期望 battle", got)
	}
	if got := cfg.GetRuntime().GetId(); got != "battle-it" {
		t.Errorf("runtime.id = %q, 期望 battle-it", got)
	}

	// 命名空间单源：runtime.namespace 显式传入，五面前缀全由框架 namespace.Derive 派生。
	assertNamespaceFaces(t, cfg.GetRuntime().GetNamespace(), "p5-a")

	eps := cfg.GetRegistry().GetEtcd().GetEndpoints()
	if len(eps) != 1 || eps[0] != "127.0.0.1:12379" {
		t.Errorf("registry.etcd.endpoints = %v", eps)
	}
	if got := cfg.GetData().GetNats().GetUrl(); got != "nats://127.0.0.1:14222" {
		t.Errorf("data.nats.url = %q", got)
	}
	if got := cfg.GetData().GetMongo().GetUri(); got != "mongodb://127.0.0.1:27017" {
		t.Errorf("data.mongo.uri = %q", got)
	}
	if got := cfg.GetData().GetMongo().GetDatabase(); got != "battle_it" {
		t.Errorf("data.mongo.database = %q", got)
	}
	// 进程内形态只启用 internal gRPC 面（随机端口）；edge 面自阶段 3 批次 5 起停用
	// （客户端战斗 op 直连帧面，不再经网关转 Edge 面）——留空即不监听，这是刻意的破坏性切换。
	if got := cfg.GetServer().GetGrpc().GetEdgeAddr(); got != "" {
		t.Errorf("server.grpc.edge_addr = %q, 期望留空（Edge 面不再启用）", got)
	}
	if got := cfg.GetServer().GetGrpc().GetInternalAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.grpc.internal_addr = %q, 期望缺省 127.0.0.1:0", got)
	}
	if got := cfg.GetServer().GetHttp().GetAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.http.addr = %q, 期望缺省 127.0.0.1:0", got)
	}
	// 出票三件随进程内形态装配进 bootstrap.battle 段（校验在 app.newActorConfig）。
	if got := cfg.GetBattle().GetTicketKey(); got != opts.TicketKey {
		t.Errorf("battle.ticket_key = %q, 期望 %q", got, opts.TicketKey)
	}
	if got := cfg.GetBattle().GetTicketTtl(); got != "45s" {
		t.Errorf("battle.ticket_ttl = %q, 期望 45s", got)
	}
	if got := cfg.GetBattle().GetEdgeEndpoints(); len(got) != len(opts.EdgeEndpoints) {
		t.Errorf("battle.edge_endpoints 面数 = %d, 期望 %d", len(got), len(opts.EdgeEndpoints))
	} else {
		for i, want := range opts.EdgeEndpoints {
			if got[i].GetTransport() != want.GetTransport() || got[i].GetAddress() != want.GetAddress() {
				t.Errorf("battle.edge_endpoints[%d] = %s/%s, 期望 %s/%s", i,
					got[i].GetTransport(), got[i].GetAddress(), want.GetTransport(), want.GetAddress())
			}
		}
	}
}

// testEdgeEndpoints 返回三面接入层地址（ws/kcp/udp；单地址无法让 SDK 知道该拨哪个端口）。
func testEdgeEndpoints() []*battlev1.EdgeEndpoint {
	return []*battlev1.EdgeEndpoint{
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.example.com:7100"},
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: "edge.example.com:7101"},
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UDP, Address: "edge.example.com:7102"},
	}
}

// TestApplyOverrideKeepsTicketConfig 验证战斗参数覆盖只动战斗参数：
// 票据密钥/TTL/接入层面列表由配置决定，不得被 e2e 的参数覆盖抹掉。
func TestApplyOverrideKeepsTicketConfig(t *testing.T) {
	base := battleactor.DefaultConfig()
	base.TicketKey = []byte("0123456789abcdef0123456789abcdef")
	base.TicketTTL = 45 * time.Second
	base.EdgeEndpoints = testEdgeEndpoints()

	got := applyOverride(base, &BattleConfig{TickInterval: 7, TrackLen: 8, MaxFrames: 9, SnapshotEvery: 10})
	if got.TickInterval != 7 || got.TrackLen != 8 || got.MaxFrames != 9 || got.SnapshotEvery != 10 {
		t.Errorf("战斗参数未被覆盖: %+v", got)
	}
	if len(got.TicketKey) != 32 || got.TicketTTL != 45*time.Second || len(got.EdgeEndpoints) != 3 {
		t.Errorf("票据三件被覆盖抹掉: %+v", got)
	}
	if same := applyOverride(base, nil); same.TicketKey == nil {
		t.Errorf("nil 覆盖应原样透传基础配置: %+v", same)
	}
}

// TestNewBootstrapRequiresNamespace 验证 R9 严格模式：runtime.namespace 缺失即装配失败
// （**不回落 default/env**），错误可用 errors.Is 判定且含字段全名与示例值。
func TestNewBootstrapRequiresNamespace(t *testing.T) {
	_, err := newBootstrap(Options{
		NodeID:        "battle-it",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
	})
	if err == nil {
		t.Fatal("runtime.namespace 缺失时期望装配失败，实际为 nil（说明回落了缺省）")
	}
	if !errors.Is(err, namespace.ErrInvalid) {
		t.Errorf("错误应可用 errors.Is(err, namespace.ErrInvalid) 判定，实际 %v", err)
	}
	if !strings.Contains(err.Error(), "runtime.namespace") || !strings.Contains(err.Error(), "test") {
		t.Errorf("错误信息应含字段全名 runtime.namespace 与示例值 test，实际 %v", err)
	}
}

// assertNamespaceFaces 断言 runtime.namespace 经框架派生出的五面前缀与预期逐面一致：
// 注册中心键前缀 / 集群 NATS subject 前缀 / 业务 topic 前缀 / redis 键前缀 / etcd 目录。
func assertNamespaceFaces(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("runtime.namespace = %q, 期望 %q", got, want)
	}
	derived, err := namespace.Derive(got)
	if err != nil {
		t.Fatalf("namespace.Derive(%q): %v", got, err)
	}
	faces := []struct{ name, got, want string }{
		{"注册中心键前缀", derived.RegistryPrefix, "/atlas/services/" + want},
		{"集群 subject 前缀", derived.ClusterSubjectPrefix, "atlas_actor." + want},
		{"业务 topic 前缀", derived.TopicPrefix, "atlas." + want},
		{"redis 键前缀", derived.RedisKeyPrefix, "atlas:" + want + ":"},
		{"etcd 目录", derived.EtcdDirectory, "/atlas/actors/" + want},
	}
	for _, f := range faces {
		if f.got != f.want {
			t.Errorf("%s = %q, 期望 %q", f.name, f.got, f.want)
		}
	}
}

// TestBattleConfigRoundTrip 验证 BattleConfig 与 actor.Config 的镜像映射无丢失
// （两类型的四个战斗参数字段一一对应，是 e2e 注入自定义战斗参数的唯一通道）。
func TestBattleConfigRoundTrip(t *testing.T) {
	in := BattleConfig{TickInterval: 42, TrackLen: 25, MaxFrames: 7, SnapshotEvery: 3}
	got := applyOverride(battleactor.Config{}, &in)
	if got.TickInterval != 42 || got.TrackLen != 25 || got.MaxFrames != 7 || got.SnapshotEvery != 3 {
		t.Fatalf("applyOverride() = %+v, 期望四个战斗参数被覆盖", got)
	}

	// DefaultBattleConfig 应完整填充默认参数（非零）。
	def := DefaultBattleConfig()
	if def.TickInterval == 0 || def.TrackLen == 0 || def.MaxFrames == 0 || def.SnapshotEvery == 0 {
		t.Fatalf("DefaultBattleConfig() 存在零值字段: %+v", def)
	}
}
