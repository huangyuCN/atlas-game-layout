package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgetcd "github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	atlasregistry "github.com/huangyuCN/atlas/registry"
)

// battleOpts 返回共用同一实例 ID 的 battle 装配参数
// （battle 进程内形态会自行注册实例，端口为随机端口，不冲突）。
func battleOpts(namespace string) battleassemble.Options {
	return battleassemble.Options{
		NodeID:        "battle-dup",
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		MongoURI:      itMongoURI,
		MongoDB:       itMongoDB,
		Namespace:     namespace,
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

	first, err := battleassemble.New(ctx, battleOpts(itNamespace))
	if err != nil {
		t.Fatalf("首个实例装配失败: %v", err)
	}
	t.Cleanup(func() { _ = first.Stop(context.Background()) })

	second, err := battleassemble.New(ctx, battleOpts(itNamespace))
	if !errors.Is(err, atlasregistry.ErrInstanceConflict) {
		t.Fatalf("同键实例装配应返回 ErrInstanceConflict，实际 %v（实例 %v）", err, second)
	}
}

// TestE2ENamespaceIsolatesInstances 验证不同键前缀下「同名同 ID」实例可共存，
// 且各自前缀只看到自己的实例：这是多套部署共用同一 etcd 时的隔离保证
// （前缀相同则会互相覆盖并互相注销）。
func TestE2ENamespaceIsolatesInstances(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skip(reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	namespaces := []string{itNamespace + "-a", itNamespace + "-b"}
	for _, namespace := range namespaces {
		b, err := battleassemble.New(ctx, battleOpts(namespace))
		if err != nil {
			t.Fatalf("命名空间 %s 下装配失败: %v", namespace, err)
		}
		t.Cleanup(func() { _ = b.Stop(context.Background()) })
	}

	ec, err := pkgetcd.NewClient(pkgetcd.Options{Endpoints: []string{itEtcdEndpoints}})
	if err != nil {
		t.Fatalf("构造 etcd 客户端失败: %v", err)
	}
	defer func() { _ = ec.Close() }()
	for _, namespace := range namespaces {
		reg, err := pkgregistry.NewEtcd(ec, pkgregistry.Options{Namespace: namespace})
		if err != nil {
			t.Fatalf("构造注册中心失败: %v", err)
		}
		instances, err := reg.GetService(ctx, consts.ServiceBattle)
		if err != nil {
			t.Fatalf("命名空间 %s 查询实例失败: %v", namespace, err)
		}
		if len(instances) != 1 || instances[0].ID != "battle-dup" {
			t.Fatalf("命名空间 %s 应恰好看到自己的 1 个实例，实际 %+v", namespace, instances)
		}
	}
}
