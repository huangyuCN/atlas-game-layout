package assemble

import (
	"testing"
)

// TestNewBootstrap 验证 Options 到 *conf.Bootstrap 的映射与缺省回退。
func TestNewBootstrap(t *testing.T) {
	opts := Options{
		NodeID:        "matcher-it",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
		RedisAddr:     "127.0.0.1:16379",
	}
	cfg := newBootstrap(opts)

	if got := cfg.GetRuntime().GetName(); got != "matcher" {
		t.Errorf("runtime.name = %q, 期望 matcher", got)
	}
	if got := cfg.GetRuntime().GetId(); got != "matcher-it" {
		t.Errorf("runtime.id = %q, 期望 matcher-it", got)
	}
	eps := cfg.GetRegistry().GetEtcd().GetEndpoints()
	if len(eps) != 1 || eps[0] != "127.0.0.1:12379" {
		t.Errorf("registry.etcd.endpoints = %v", eps)
	}
	if got := cfg.GetData().GetRedis().GetAddr(); got != "127.0.0.1:16379" {
		t.Errorf("data.redis.addr = %q", got)
	}
	if got := cfg.GetData().GetNats().GetUrl(); got != "nats://127.0.0.1:14222" {
		t.Errorf("data.nats.url = %q", got)
	}
	// 进程内形态监听地址固定随机端口。
	if got := cfg.GetServer().GetGrpc().GetAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.grpc.addr = %q, 期望缺省 127.0.0.1:0", got)
	}
	if got := cfg.GetServer().GetHttp().GetAddr(); got != "127.0.0.1:0" {
		t.Errorf("server.http.addr = %q, 期望缺省 127.0.0.1:0", got)
	}
}
