package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	pkgetcd "github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	atlasregistry "github.com/huangyuCN/atlas/registry"
)

// battleOpts 返回共用同一实例 ID 的 battle 装配参数（ns 是命名空间 token，
// 五面前缀由装配层经 namespace.Derive 派生；battle 进程内形态会自行注册实例，
// 端口为随机端口，不冲突）。
func battleOpts(ns string) battleassemble.Options {
	return battleassemble.Options{
		NodeID:        "battle-dup",
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		MongoURI:      itMongoURI,
		MongoDB:       itMongoDB,
		Namespace:     ns,
	}
}

// TestE2EInstanceConflictFailsFast 验证同一实例键已被占用时第二个实例装配失败：
// 静默覆盖会让两个进程互相顶替，且先者的注销会摘掉后者的注册。
func TestE2EInstanceConflictFailsFast(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first, err := battleassemble.New(ctx, battleOpts(itNS))
	if err != nil {
		t.Fatalf("首个实例装配失败: %v", err)
	}
	t.Cleanup(func() { _ = first.Stop(context.Background()) })

	second, err := battleassemble.New(ctx, battleOpts(itNS))
	// 同键实例必须快速失败——两道闸门都可能先命中，断言接受任一，避免把闸门顺序写进测试：
	//   1. actor 节点归属（pkgactor.ErrNodeConflict）：同 NodeID 会共用 NATS 节点 subject，
	//      该检查在 actor 运行时启动时执行，通常早于注册中心的实例键检查；
	//   2. 注册中心实例键冲突（atlasregistry.ErrInstanceConflict）。
	if !errors.Is(err, atlasregistry.ErrInstanceConflict) && !errors.Is(err, pkgactor.ErrNodeConflict) {
		t.Fatalf("同键实例装配应返回注册冲突或节点归属冲突，实际 %v（实例 %v）", err, second)
	}
}

// TestE2ENamespaceIsolatesInstances 验证不同命名空间下「同名同 ID」实例可共存，
// 且各自前缀只看到自己的实例：这是多套部署共用同一 etcd 时的隔离保证
// （前缀相同则会互相覆盖并互相注销）。
func TestE2ENamespaceIsolatesInstances(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tokens := []string{itNS + "-a", itNS + "-b"}
	for _, ns := range tokens {
		b, err := battleassemble.New(ctx, battleOpts(ns))
		if err != nil {
			t.Fatalf("命名空间 %s 下装配失败: %v", ns, err)
		}
		t.Cleanup(func() { _ = b.Stop(context.Background()) })
	}

	ec, err := pkgetcd.NewClient(pkgetcd.Options{Endpoints: []string{itEtcdEndpoints}})
	if err != nil {
		t.Fatalf("构造 etcd 客户端失败: %v", err)
	}
	defer func() { _ = ec.Close() }()
	for _, ns := range tokens {
		// 注册中心键前缀由同一 token 经 namespace.Derive 派生（与装配层同源）。
		reg, err := pkgregistry.NewEtcd(ec, pkgregistry.Options{Namespace: mustDerive(ns).RegistryPrefix})
		if err != nil {
			t.Fatalf("构造注册中心失败: %v", err)
		}
		instances, err := reg.GetService(ctx, consts.ServiceBattle)
		if err != nil {
			t.Fatalf("命名空间 %s 查询实例失败: %v", ns, err)
		}
		if len(instances) != 1 || instances[0].ID != "battle-dup" {
			t.Fatalf("命名空间 %s 应恰好看到自己的 1 个实例，实际 %+v", ns, instances)
		}
	}
}
