package actor

import (
	"strings"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/directory"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/namespace"
)

// TestDirectoryFromConfDefaultsToEtcd 锁定「不配置 = etcd」：切后端必须显式，
// 否则老部署升级后会被悄悄换掉存储（目录里已有的归属记录读不到）。
func TestDirectoryFromConfDefaultsToEtcd(t *testing.T) {
	for _, conf := range []*configspb.Actor{nil, {}, {Directory: &configspb.Directory{}}} {
		opts, err := DirectoryFromConf(conf)
		if err != nil {
			t.Fatalf("DirectoryFromConf: %v", err)
		}
		if opts.Backend != directory.BackendEtcd {
			t.Fatalf("backend = %q，期望缺省 etcd", opts.Backend)
		}
	}
}

// TestDirectoryFromConfRedis 验证 actor.directory 的 redis 段能解析出来。
func TestDirectoryFromConfRedis(t *testing.T) {
	conf := &configspb.Actor{Directory: &configspb.Directory{
		Backend:      "redis",
		QuiesceGrace: "7s",
		Redis: &configspb.DirectoryRedis{
			Endpoints:    []string{"127.0.0.1:26380"},
			MasterName:   "atlas",
			SyncReplicas: 1,
		},
	}}
	opts, err := DirectoryFromConf(conf)
	if err != nil {
		t.Fatalf("DirectoryFromConf: %v", err)
	}
	if opts.Backend != directory.BackendRedis || opts.QuiesceGrace != 7*time.Second {
		t.Fatalf("解析结果 = %+v", opts)
	}
	if len(opts.Redis.Endpoints) != 1 || opts.Redis.MasterName != "atlas" || opts.Redis.SyncReplicas != 1 {
		t.Fatalf("redis 配置 = %+v", opts.Redis)
	}
}

// TestNewRuntimeRejectsRedisGraceBelowLag 验证装配期硬约束：redis 后端 + grace < 复制滞后
// 必须**在建连接之前**就失败（Options 直接构造的调用方也要被拦住），错误文案指向双活原因。
func TestNewRuntimeRejectsRedisGraceBelowLag(t *testing.T) {
	_, err := NewRuntime(Options{
		NodeID:        "n1",
		EtcdEndpoints: []string{"127.0.0.1:2379"},
		NatsURL:       "nats://127.0.0.1:4222",
		Namespace:     "p5-test",
		Directory: directory.Options{
			Backend:        directory.BackendRedis,
			QuiesceGrace:   time.Second,
			ReplicationLag: 3 * time.Second,
			Redis:          directory.RedisOptions{Endpoints: []string{"127.0.0.1:6379"}},
		},
	})
	if err == nil {
		t.Fatal("grace < replication_lag 应启动失败")
	}
	if !strings.Contains(err.Error(), "quiesce grace") {
		t.Fatalf("错误应指向保护窗/复制滞后约束，实际: %v", err)
	}
}

// TestDirectoryPrefixMatchesNamespace 验证目录前缀两端同源：写入侧与 edge 只读侧都从
// namespace.Derive 取值（etcd /atlas/actors/<ns>；redis atlas:<ns>:actors）。
func TestDirectoryPrefixMatchesNamespace(t *testing.T) {
	derived, err := namespace.Derive("p5-test")
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	if got := directory.Prefix(directory.BackendEtcd, derived); got != derived.EtcdDirectory {
		t.Fatalf("etcd 前缀 = %q，期望 %q", got, derived.EtcdDirectory)
	}
	if got := directory.Prefix(directory.BackendRedis, derived); got != derived.RedisKeyPrefix+"actors" {
		t.Fatalf("redis 前缀 = %q，期望 %q", got, derived.RedisKeyPrefix+"actors")
	}
}
