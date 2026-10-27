package app

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/directory"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
	actorv1 "github.com/huangyuCN/atlas/contrib/actor/proto/actorv1"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/namespace"
	"go.uber.org/fx"
)

// fakeLifecycle 记录 Append 的 hook，供用例验证「连接关闭挂在停机钩子上」。
type fakeLifecycle struct {
	hooks []fx.Hook
}

// Append 实现 fx.Lifecycle。
func (f *fakeLifecycle) Append(h fx.Hook) { f.hooks = append(f.hooks, h) }

// redisEdgeBootstrap 构造「actor 目录走 redis」的接入层配置（命名空间取自 runtime.namespace）。
func redisEdgeBootstrap() *conf.Bootstrap {
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Namespace: "p5-test"},
		Actor: &configspb.Actor{Directory: &configspb.Directory{
			Backend: "redis",
			Redis: &configspb.DirectoryRedis{
				Endpoints:    []string{"127.0.0.1:26380"},
				MasterName:   "atlas",
				SyncReplicas: 1,
			},
		}},
	}
}

// TestNewActorDirectoryRedisBackend 验证接入层按同一份 actor.directory 配置切后端：
// backend=redis 时不需要 etcd 客户端，且自建连接的关闭挂在停机钩子上。
func TestNewActorDirectoryRedisBackend(t *testing.T) {
	lc := &fakeLifecycle{}
	dir, err := newActorDirectory(lc, redisEdgeBootstrap(), nil)
	if err != nil {
		t.Fatalf("newActorDirectory(redis): %v", err)
	}
	if dir == nil {
		t.Fatal("目录适配器为 nil")
	}
	if len(lc.hooks) != 1 || lc.hooks[0].OnStop == nil {
		t.Fatalf("应挂 1 个停机钩子关闭 redis 连接，实际 %d 个", len(lc.hooks))
	}
	if err := lc.hooks[0].OnStop(context.Background()); err != nil {
		t.Fatalf("停机钩子: %v", err)
	}
}

// TestNewActorDirectoryEtcdNeedsClient 验证 etcd 后端仍要求注入客户端（不静默退化）。
func TestNewActorDirectoryEtcdNeedsClient(t *testing.T) {
	cfg := &conf.Bootstrap{Runtime: &configspb.Runtime{Namespace: "p5-test"}}
	if _, err := newActorDirectory(&fakeLifecycle{}, cfg, nil); err == nil {
		t.Fatal("etcd 后端缺客户端应报错")
	}
}

// TestNewActorDirectoryRejectsBadConfig 验证装配期错误不被吞掉：未知后端与非法规格都启动失败。
func TestNewActorDirectoryRejectsBadConfig(t *testing.T) {
	bad := []*conf.Bootstrap{
		{Runtime: &configspb.Runtime{Namespace: "p5-test"},
			Actor: &configspb.Actor{Directory: &configspb.Directory{Backend: "consul"}}},
		{Runtime: &configspb.Runtime{Namespace: "bad ns"}, // 命名空间非法
			Actor: &configspb.Actor{Directory: &configspb.Directory{Backend: "redis",
				Redis: &configspb.DirectoryRedis{Endpoints: []string{"127.0.0.1:6379"}}}}},
	}
	for i, cfg := range bad {
		if _, err := newActorDirectory(&fakeLifecycle{}, cfg, nil); err == nil {
			t.Fatalf("第 %d 个非法配置应报错", i)
		}
	}
}

// TestActorDirectoryReadsRedisWrites 端到端验证「写侧与读侧同一个后端、同一份前缀」：
// 用 pkg/directory 在 Redis 上写一条归属记录（模拟 battle 侧），接入层的只读目录
// 必须能按 PID 读到属主。配错后端/前缀的典型表现就是这里读不到（帧流永远建不起来）。
// 依赖 REDIS_ADDR（缺省 127.0.0.1:6379），不可达即 Skip。
func TestActorDirectoryReadsRedisWrites(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	cfg := redisEdgeBootstrap()
	cfg.Runtime.Namespace = fmt.Sprintf("p5e2e%d", time.Now().UnixNano())
	cfg.Actor.Directory.Redis.Endpoints = []string{addr}
	cfg.Actor.Directory.Redis.MasterName = "" // 单点直连：用例只验证「读写同源」，不需要哨兵

	lc := &fakeLifecycle{}
	reader, err := newActorDirectory(lc, cfg, nil)
	if err != nil {
		t.Fatalf("newActorDirectory: %v", err)
	}
	defer func() {
		if len(lc.hooks) > 0 && lc.hooks[0].OnStop != nil {
			_ = lc.hooks[0].OnStop(context.Background())
		}
	}()

	derived, err := namespace.Derive(cfg.Runtime.Namespace)
	if err != nil {
		t.Fatalf("Derive: %v", err)
	}
	writeOpts, err := directory.Parse(cfg.GetActor())
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	loc, release, err := directory.Open(writeOpts, nil, derived)
	if err != nil {
		t.Fatalf("Open(writer): %v", err)
	}
	defer release()
	be, ok := loc.(cluster.Backend)
	if !ok {
		t.Fatalf("写侧 locator 未满足 cluster.Backend: %T", loc)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := be.Upsert(ctx, pidKey("battle:b1"), &types.ActorLocation{Location: &actorv1.Location{
		OwnerNode: "nodeA", Epoch: 1, Status: actorv1.ActorStatus_ACTOR_STATUS_ACTIVE,
	}}); err != nil {
		t.Skipf("redis 不可达（%s）: %v", addr, err)
	}
	owner, err := reader.OwnerNode(ctx, "battle:b1")
	if err != nil {
		t.Fatalf("OwnerNode: %v", err)
	}
	if owner != "nodeA" {
		t.Fatalf("读到属主 = %q，期望 nodeA（两端后端/前缀不一致？）", owner)
	}
}

// pidKey 是 locator.Key 的最小实现（PID 字符串即完整键）。
type pidKey string

// Prefix 返回完整键。
func (k pidKey) Prefix() string { return string(k) }

// String 返回完整键。
func (k pidKey) String() string { return string(k) }
