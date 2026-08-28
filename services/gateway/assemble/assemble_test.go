package assemble

import (
	"testing"
)

// TestNewBootstrap 验证 Options 到 *conf.Bootstrap 的映射与缺省回退。
func TestNewBootstrap(t *testing.T) {
	opts := Options{
		ID:            "a",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
		RedisAddr:     "127.0.0.1:16379",
	}
	cfg := newBootstrap(opts)

	// 实例 ID 统一加 gw- 前缀（会话路由与跨实例踢人依赖该约定）。
	if got := cfg.GetRuntime().GetId(); got != "gw-a" {
		t.Errorf("runtime.id = %q, 期望 gw-a", got)
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
	// 进程内形态四协议与 http 监听地址固定随机端口。
	for name, got := range map[string]string{
		"tcp":       cfg.GetTcp().GetAddr(),
		"websocket": cfg.GetWebsocket().GetAddr(),
		"kcp":       cfg.GetKcp().GetAddr(),
		"udp":       cfg.GetUdp().GetAddr(),
		"grpc":      cfg.GetServer().GetGrpc().GetAddr(),
		"http":      cfg.GetServer().GetHttp().GetAddr(),
	} {
		if got != "127.0.0.1:0" {
			t.Errorf("%s.addr = %q, 期望缺省 127.0.0.1:0", name, got)
		}
	}
}
