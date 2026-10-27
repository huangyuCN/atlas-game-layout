package directory

import (
	"strings"
	"testing"
	"time"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
	"github.com/huangyuCN/atlas/locator"
	"github.com/huangyuCN/atlas/namespace"
)

// actorWithDirectory 构造只带目录段的 actor 配置。
func actorWithDirectory(d *configspb.Directory) *configspb.Actor {
	return &configspb.Actor{Directory: d}
}

// redisDir 构造 backend=redis 的目录段（含一个可用 endpoints）。
func redisDir() *configspb.Directory {
	return &configspb.Directory{
		Backend: "redis",
		Redis:   &configspb.DirectoryRedis{Endpoints: []string{"127.0.0.1:6379"}},
	}
}

// TestParseDefaultsToEtcd 验证缺段/空 backend = etcd：切后端的开关必须是显式的，
// 不配置就保持现网行为（这是本功能「可回退」的前提）。
func TestParseDefaultsToEtcd(t *testing.T) {
	cases := []struct {
		name string
		conf *configspb.Actor
	}{
		{name: "nil_actor", conf: nil},
		{name: "empty_actor", conf: &configspb.Actor{}},
		{name: "empty_directory", conf: actorWithDirectory(&configspb.Directory{})},
		{name: "whitespace_backend", conf: actorWithDirectory(&configspb.Directory{Backend: "  "})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts, err := Parse(c.conf)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if opts.Backend != BackendEtcd {
				t.Fatalf("backend = %q，期望缺省 etcd", opts.Backend)
			}
			if opts.Redis.Endpoints != nil {
				t.Fatalf("etcd 后端不应解析 redis 连接段: %+v", opts.Redis)
			}
		})
	}
}

// TestParseRejectsUnknownBackend 验证未知后端启动失败（不静默回落到 etcd）。
func TestParseRejectsUnknownBackend(t *testing.T) {
	_, err := Parse(actorWithDirectory(&configspb.Directory{Backend: "consul"}))
	if err == nil {
		t.Fatal("未知后端应报错")
	}
}

// TestParseRedisRequiresEndpoints 验证 backend=redis 时 endpoints 必填（不猜缺省地址）。
func TestParseRedisRequiresEndpoints(t *testing.T) {
	conf := &configspb.Directory{Backend: "redis", Redis: &configspb.DirectoryRedis{MasterName: "atlas"}}
	if _, err := Parse(actorWithDirectory(conf)); err == nil {
		t.Fatal("backend=redis 且缺 endpoints 应报错")
	}
	conf.Redis.Endpoints = []string{"", "  "} // 只有空串同样视为未配置
	if _, err := Parse(actorWithDirectory(conf)); err == nil {
		t.Fatal("端点为空白串应视为未配置")
	}
}

// TestParseRedisOptions 验证 redis 连接段逐字段映射与缺省（sync_timeout 缺省 2s）。
func TestParseRedisOptions(t *testing.T) {
	conf := actorWithDirectory(&configspb.Directory{
		Backend:        "Redis", // 大小写不敏感
		QuiesceGrace:   "8s",
		ReplicationLag: "500ms",
		Redis: &configspb.DirectoryRedis{
			Endpoints:          []string{" 127.0.0.1:26380 ", "127.0.0.1:26381"},
			MasterName:         " atlas ",
			Password:           "secret",
			Db:                 3,
			Tls:                true,
			InsecureSkipVerify: true,
			SyncReplicas:       1,
		},
	})
	opts, err := Parse(conf)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if opts.Backend != BackendRedis {
		t.Fatalf("backend = %q，期望 redis", opts.Backend)
	}
	if opts.QuiesceGrace != 8*time.Second || opts.ReplicationLag != 500*time.Millisecond {
		t.Fatalf("grace/lag = %s/%s", opts.QuiesceGrace, opts.ReplicationLag)
	}
	r := opts.Redis
	if len(r.Endpoints) != 2 || r.Endpoints[0] != "127.0.0.1:26380" {
		t.Fatalf("endpoints 未归一: %+v", r.Endpoints)
	}
	if r.MasterName != "atlas" || r.Password != "secret" || r.DB != 3 {
		t.Fatalf("连接参数映射错误: %+v", r)
	}
	if !r.TLS || !r.InsecureSkipVerify {
		t.Fatalf("TLS 参数映射错误: %+v", r)
	}
	if r.SyncReplicas != 1 || r.SyncTimeout != 2*time.Second {
		t.Fatalf("同步复制参数 = %d/%s，期望 1/2s", r.SyncReplicas, r.SyncTimeout)
	}
}

// TestParseRedisGraceBelowLagRejected 验证切主双活的硬约束：grace 小于复制滞后即启动失败。
func TestParseRedisGraceBelowLagRejected(t *testing.T) {
	conf := redisDir()
	conf.QuiesceGrace = "1s"
	conf.ReplicationLag = "2s"
	if _, err := Parse(actorWithDirectory(conf)); err == nil {
		t.Fatal("grace < replication_lag 必须启动失败（否则切主会双活）")
	}
}

// TestParseRedisGraceDefaultsToFrameworkGrace 验证未配 quiesce_grace 时按框架缺省（5s）比较滞后。
func TestParseRedisGraceDefaultsToFrameworkGrace(t *testing.T) {
	conf := redisDir()
	conf.ReplicationLag = "5s" // 恰好等于框架缺省 grace，应通过
	if _, err := Parse(actorWithDirectory(conf)); err != nil {
		t.Fatalf("lag=5s 应通过（缺省 grace 5s）: %v", err)
	}
	conf.ReplicationLag = "6s"
	if _, err := Parse(actorWithDirectory(conf)); err == nil {
		t.Fatal("lag > 缺省 grace 应报错")
	}
}

// TestPrefixBothBackends 验证两端用同一份派生：etcd 与 redis 前缀一一对应。
func TestPrefixBothBackends(t *testing.T) {
	derived, err := namespace.Derive("p5-test")
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if got := Prefix(BackendEtcd, derived); got != "/atlas/actors/p5-test" {
		t.Fatalf("etcd 前缀 = %q", got)
	}
	if got := Prefix(BackendRedis, derived); got != "atlas:p5-test:actors" {
		t.Fatalf("redis 前缀 = %q", got)
	}
}

// TestOpenEtcdRequiresClient 验证 etcd 后端缺客户端即报错（不静默变成内存实现）。
func TestOpenEtcdRequiresClient(t *testing.T) {
	derived, _ := namespace.Derive("p5-test")
	if _, _, err := Open(Options{Backend: BackendEtcd}, nil, derived); err == nil {
		t.Fatal("etcd 后端缺客户端应报错")
	}
}

// TestOpenRedisBuildsCASLocator 验证 redis 后端产出的 locator 同时满足
// cluster.Backend（Locator + CASLocator）与 locator.Leaser，且 release 可重复调用。
func TestOpenRedisBuildsCASLocator(t *testing.T) {
	derived, _ := namespace.Derive("p5-test")
	opts := Options{Backend: BackendRedis, Redis: RedisOptions{Endpoints: []string{"127.0.0.1:6379"}}}
	loc, release, err := Open(opts, nil, derived) // redis 后端不需要 etcd 客户端
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer release()
	if _, ok := loc.(cluster.Backend); !ok {
		t.Fatalf("redis locator 未满足 cluster.Backend: %T", loc)
	}
	if _, ok := loc.(locator.Leaser); !ok {
		t.Fatalf("redis locator 未满足 locator.Leaser: %T", loc)
	}
	release() // 释放函数幂等
}

// TestNewRedisClientRequiresEndpoints 验证连接构造的入参校验。
func TestNewRedisClientRequiresEndpoints(t *testing.T) {
	if _, err := NewRedisClient(RedisOptions{}); err == nil {
		t.Fatal("缺 endpoints 应报错")
	}
}

// TestEffectiveGrace 验证「未配置取框架缺省」的口径。
func TestEffectiveGrace(t *testing.T) {
	if got := EffectiveGrace(0); got != cluster.DefaultLease().QuiesceGrace {
		t.Fatalf("未配置 grace = %s，期望框架缺省", got)
	}
	if got := EffectiveGrace(3 * time.Second); got != 3*time.Second {
		t.Fatalf("显式 grace = %s", got)
	}
}

// TestOpenEmptyBackendDefaultsToEtcd 验证零值 Options（直接构造、不经过 Parse 的调用方：
// 示例/测试/嵌入）按 etcd 处理，而不是报「未知后端」——否则升级后旧调用方会启动失败。
func TestOpenEmptyBackendDefaultsToEtcd(t *testing.T) {
	derived, _ := namespace.Derive("p5-test")
	_, _, err := Open(Options{}, nil, derived)
	if err == nil {
		t.Fatal("etcd 后端缺客户端应报错")
	}
	if !strings.Contains(err.Error(), "etcd") {
		t.Fatalf("零值 Backend 应走 etcd 分支，实际错误: %v", err)
	}
	if got := Prefix("", derived); got != derived.EtcdDirectory {
		t.Fatalf("零值 Backend 的前缀 = %q，期望 etcd 目录", got)
	}
}
