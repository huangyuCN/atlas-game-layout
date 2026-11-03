package placement

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	etcdreg "github.com/huangyuCN/atlas/contrib/registry/etcd"
	"github.com/huangyuCN/atlas/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// coldStartEtcd 连真实 etcd 并构造一个独立命名空间的注册中心（未设置 ATLAS_MATCHER_ETCD 即跳过）。
//
// 按 AGENTS.md 在集成服务器执行：
//
//	ATLAS_MATCHER_ETCD=10.10.9.36:12379 go test ./services/matcher/internal/placement/ \
//	    -run ColdStartFrameRegistration -count=1 -v
func coldStartEtcd(t *testing.T) *etcdreg.Registry {
	t.Helper()
	raw := os.Getenv("ATLAS_MATCHER_ETCD")
	if raw == "" {
		t.Skip("未设置 ATLAS_MATCHER_ETCD，跳过冷启动集成用例")
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(raw, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("etcd 不可用: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	reg, err := pkgregistry.NewEtcd(cli, pkgregistry.Options{
		Namespace: fmt.Sprintf("/atlas-it/placement/%d", time.Now().UnixNano()),
		TTL:       10 * time.Second,
	})
	if err != nil {
		t.Fatalf("构造注册中心失败: %v", err)
	}
	return reg
}

// registerColdStartFixtures 注册两台 battle（grpc 面）与**只一台**帧面实例（冷启动窗口），
// 返回全量实例以便用例结束时注销。
func registerColdStartFixtures(t *testing.T, reg *etcdreg.Registry) []*registry.ServiceInstance {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	battles := []*registry.ServiceInstance{
		{ID: "battle-1", Name: consts.ServiceBattle, Version: "v1", Endpoints: []string{"grpc://127.0.0.1:9301"}},
		{ID: "battle-b2", Name: consts.ServiceBattle, Version: "v1", Endpoints: []string{"grpc://127.0.0.1:9311"}},
	}
	frames := []*registry.ServiceInstance{
		{ID: "battle-1", Name: consts.ServiceBattleFrame, Version: "v1", Endpoints: []string{"ws://127.0.0.1:9411"}},
		{ID: "battle-b2", Name: consts.ServiceBattleFrame, Version: "v1", Endpoints: []string{"ws://127.0.0.1:9412"}},
	}
	all := append(append([]*registry.ServiceInstance{}, battles...), frames...)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, si := range all {
			_ = reg.Deregister(cleanupCtx, si)
		}
	})
	for _, si := range append(battles, frames[0]) {
		if err := reg.Register(ctx, si); err != nil {
			t.Fatalf("注册 %s 失败: %v", si.ID, err)
		}
	}
	return all
}

// TestColdStartFrameRegistrationIntegration 是「冷启动偏斜」的**集成复现**：
// 真实 etcd + 真实注册发现，两台 battle 实例都已注册、但第二台的帧面实例晚 200ms 才注册
// （重启后各进程的注册先后不一）。同一份注册状态下跑两遍首轮 100 局：
//   - 旧口径（不等候选）⇒ 整轮落先就绪的那台（0/100）；
//   - 修复后（等候选集齐）⇒ 50/50。
func TestColdStartFrameRegistrationIntegration(t *testing.T) {
	reg := coldStartEtcd(t)
	all := registerColdStartFixtures(t, reg)

	// ① 旧口径：候选只剩一台时立刻出票（ReadyTimeout 极小 = 关掉等待）。
	before := placeN(t, New(reg, Options{CacheTTL: time.Minute, ReadyTimeout: time.Nanosecond}), 100)
	t.Logf("旧口径（不等候选）首轮 100 局: %v", before)
	if before["127.0.0.1:9301"] != 100 {
		t.Fatalf("旧口径应整轮落先就绪的那台，实际 %v", before)
	}

	// ② 修复后：第二台帧面 200ms 后才进注册中心，首轮必须等齐再按 50/50 出票。
	late := all[len(all)-1]
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = reg.Register(context.Background(), late)
	}()
	p := New(reg, Options{CacheTTL: time.Minute, ReadyTimeout: 2 * time.Second, ReadyPoll: 10 * time.Millisecond})
	start := time.Now()
	after := placeN(t, p, 100)
	t.Logf("修复后首轮 100 局: %v，首局等待 %v", after, time.Since(start))
	assertEven(t, after, 100)
}
