package assemble

import (
	"testing"

	battleactor "github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
)

// TestNewBootstrap 验证 Options 到 *conf.Bootstrap 的映射与缺省回退。
func TestNewBootstrap(t *testing.T) {
	opts := Options{
		NodeID:        "battle-it",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
		MongoURI:      "mongodb://127.0.0.1:27017",
		MongoDB:       "battle_it",
	}
	cfg := newBootstrap(opts)

	if got := cfg.GetRuntime().GetName(); got != "battle" {
		t.Errorf("runtime.name = %q, 期望 battle", got)
	}
	if got := cfg.GetRuntime().GetId(); got != "battle-it" {
		t.Errorf("runtime.id = %q, 期望 battle-it", got)
	}
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
	// 进程内形态监听地址固定随机端口。
	if got := cfg.GetServer().GetGrpc().GetAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.grpc.addr = %q, 期望缺省 127.0.0.1:0", got)
	}
	if got := cfg.GetServer().GetHttp().GetAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.http.addr = %q, 期望缺省 127.0.0.1:0", got)
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
