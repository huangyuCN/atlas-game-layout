package assemble

import (
	"errors"
	"strings"
	"testing"

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
	// 进程内形态两个 gRPC 面都显式写随机端口（空 = 不启用该面，不能靠留空）。
	if got := cfg.GetServer().GetGrpc().GetEdgeAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.grpc.edge_addr = %q, 期望缺省 127.0.0.1:0", got)
	}
	if got := cfg.GetServer().GetGrpc().GetInternalAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.grpc.internal_addr = %q, 期望缺省 127.0.0.1:0", got)
	}
	if got := cfg.GetServer().GetHttp().GetAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.http.addr = %q, 期望缺省 127.0.0.1:0", got)
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
// （两类型字段一一对应，是 e2e 注入自定义战斗参数的唯一通道）。
func TestBattleConfigRoundTrip(t *testing.T) {
	in := BattleConfig{TickInterval: 42, TrackLen: 25, MaxFrames: 7, SnapshotEvery: 3}
	got := in.toActor()
	want := battleactor.Config{TickInterval: 42, TrackLen: 25, MaxFrames: 7, SnapshotEvery: 3}
	if got != want {
		t.Fatalf("toActor() = %+v, 期望 %+v", got, want)
	}

	// DefaultBattleConfig 应完整填充默认参数（非零）。
	def := DefaultBattleConfig()
	if def.TickInterval == 0 || def.TrackLen == 0 || def.MaxFrames == 0 || def.SnapshotEvery == 0 {
		t.Fatalf("DefaultBattleConfig() 存在零值字段: %+v", def)
	}
}
