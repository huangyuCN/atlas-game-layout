package assemble

import (
	"testing"
)

// TestNewBootstrap 验证 Options 到 *conf.Bootstrap 的映射与缺省回退。
func TestNewBootstrap(t *testing.T) {
	opts := Options{
		NodeID:        "game-it",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
		RedisAddr:     "127.0.0.1:16379",
		MongoURI:      "mongodb://127.0.0.1:27017",
		MongoDB:       "game_it",
	}
	cfg := newBootstrap(opts)

	// runtime：服务名固定 game，ID 取 NodeID（懒激活要求实例 ID == actor NodeID）。
	if got := cfg.GetRuntime().GetName(); got != "game" {
		t.Errorf("runtime.name = %q, 期望 game", got)
	}
	if got := cfg.GetRuntime().GetId(); got != "game-it" {
		t.Errorf("runtime.id = %q, 期望 game-it", got)
	}

	// registry：etcd 端点透传。
	eps := cfg.GetRegistry().GetEtcd().GetEndpoints()
	if len(eps) != 1 || eps[0] != "127.0.0.1:12379" {
		t.Errorf("registry.etcd.endpoints = %v, 期望 [127.0.0.1:12379]", eps)
	}

	// data：redis/nats/mongo 透传。
	if got := cfg.GetData().GetRedis().GetAddr(); got != "127.0.0.1:16379" {
		t.Errorf("data.redis.addr = %q", got)
	}
	if got := cfg.GetData().GetNats().GetUrl(); got != "nats://127.0.0.1:14222" {
		t.Errorf("data.nats.url = %q", got)
	}
	if got := cfg.GetData().GetMongo().GetUri(); got != "mongodb://127.0.0.1:27017" {
		t.Errorf("data.mongo.uri = %q", got)
	}
	if got := cfg.GetData().GetMongo().GetDatabase(); got != "game_it" {
		t.Errorf("data.mongo.database = %q", got)
	}

	// server：进程内形态未指定监听地址时回退随机端口。
	if got := cfg.GetServer().GetGrpc().GetAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.grpc.addr = %q, 期望缺省 127.0.0.1:0", got)
	}
	if got := cfg.GetServer().GetHttp().GetAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.http.addr = %q, 期望缺省 127.0.0.1:0", got)
	}
}
