package actor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/namespace"
	"github.com/huangyuCN/atlas/registry"
)

// fakeDiscovery 是懒激活候选节点用的内存服务发现。
type fakeDiscovery struct {
	instances []*registry.ServiceInstance
}

func (d *fakeDiscovery) GetService(context.Context, string) ([]*registry.ServiceInstance, error) {
	return d.instances, nil
}

func (d *fakeDiscovery) Watch(context.Context, string) (registry.Watcher, error) {
	return nil, fmt.Errorf("watch 未实现")
}

// echoHandler 是最小 actor：记录 Tell 内容，Ask 回显。
// 注意：集群传输的 payload 必须是 []byte 或 proto.Message。
type echoHandler struct {
	last []byte
}

func (h *echoHandler) OnStart(core.ActorContext) error                  { return nil }
func (h *echoHandler) OnStop(core.ActorContext, types.ExitReason) error { return nil }
func (h *echoHandler) OnTell(_ core.ActorContext, msg any) error {
	if s, ok := msg.([]byte); ok {
		h.last = s
	}
	return nil
}
func (h *echoHandler) OnAsk(_ core.ActorContext, _ any) (any, error) { return h.last, nil }

// TestClusterLazyActivation 集群懒激活集成测试：
// 双节点共享 etcd（127.0.0.1:12379）+ nats（127.0.0.1:14222），
// 节点 A 注册 SpawnAuto 类型，节点 B 向不存在 PID 发消息触发懒激活。
// 无 etcd/nats 时自动跳过（按 AGENTS.md 在 10.10.9.36 执行）。
//
// 跳过口径（踩过坑）：**装配配置错误一律 Fatal，不得被当成「依赖不可用」跳过**——
// 本用例曾因夹具漏配 Namespace 而永远 SKIP，且 skip 文案误报成「nats 连接失败」，
// 等于用例从未真正跑过。配置类错误（namespace 非法/必填项缺失）走 failOrSkipUnavailable
// 的 Fatal 分支，只有真·后端不可达才 Skip。
func TestClusterLazyActivation(t *testing.T) {
	// 节点 ID 每次运行唯一：节点归属键有租约（残留会挡住同 ID 重启），
	// 用固定 ID 会让上一次中断运行的残留把本次测试变成"跳过"。
	suffix := fmt.Sprintf("-%d", time.Now().UnixNano())
	idA, idB := "m2-nodeA"+suffix, "m2-nodeB"+suffix
	base := Options{
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
		ServiceName:   "atlas-actor",
		// Namespace 每次运行唯一：它派生 etcd 目录前缀与 NATS subject 前缀（R9：缺失即装配失败），
		// 唯一化既与常驻部署隔离，也让并行用例互不串台。
		Namespace: namespace.Namespace("it" + suffix),
		Discovery: &fakeDiscovery{instances: []*registry.ServiceInstance{
			{ID: idA, Name: "atlas-actor"},
			{ID: idB, Name: "atlas-actor"},
		}},
	}
	// 先按实际传给 NewRuntime 的选项做配置断言，再建连接：配置漏项必须 Fatal（见上方跳过口径）。
	optA, optB := withNodeID(base, idA), withNodeID(base, idB)
	mustFixtureOptions(t, optA)
	mustFixtureOptions(t, optB)
	rtA, err := NewRuntime(optA)
	if err != nil {
		failOrSkipUnavailable(t, "节点 A 装配失败", err)
	}
	rtB, err := NewRuntime(optB)
	if err != nil {
		rtA.Shutdown(context.Background())
		failOrSkipUnavailable(t, "节点 B 装配失败", err)
	}

	ctx := context.Background()
	if err := rtA.Start(ctx); err != nil {
		if errors.Is(err, ErrNodeConflict) {
			t.Fatalf("节点归属冲突不应被当作依赖不可用: %v", err)
		}
		failOrSkipUnavailable(t, "etcd 不可用（节点 A 目录启动失败）", err)
	}
	if err := rtB.Start(ctx); err != nil {
		rtA.Shutdown(ctx)
		if errors.Is(err, ErrNodeConflict) {
			t.Fatalf("节点归属冲突不应被当作依赖不可用: %v", err)
		}
		failOrSkipUnavailable(t, "etcd 不可用（节点 B 目录启动失败）", err)
	}
	t.Cleanup(func() {
		_ = rtA.Shutdown(context.Background())
		_ = rtB.Shutdown(context.Background())
	})

	// 双节点都注册 SpawnAuto 类型（懒激活工厂：所有节点部署相同代码）。
	props := core.Props{
		Type:       "echo",
		NewHandler: func(_ types.PID) core.Handler { return &echoHandler{} },
		SpawnMode:  core.SpawnAuto,
	}
	if err := rtA.Register(props); err != nil {
		t.Fatalf("Register(A): %v", err)
	}
	if err := rtB.Register(props); err != nil {
		t.Fatalf("Register(B): %v", err)
	}

	// 节点 B 向不存在的 PID 发消息 → 目录路由 → 懒激活到节点 A → 投递。
	pid, err := ParsePID("echo:it-1")
	if err != nil {
		t.Fatalf("ParsePID: %v", err)
	}
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := rtB.Tell(tctx, pid, []byte("hello-lazy")); err != nil {
		t.Fatalf("Tell（懒激活）: %v", err)
	}
	// Ask 验证回显（懒激活后 actor 已在节点 A 运行）。
	reply, err := rtB.Ask(tctx, pid, []byte("query"))
	if err != nil {
		t.Fatalf("Ask: %v", err)
	}
	if got, ok := reply.([]byte); !ok || string(got) != "hello-lazy" {
		t.Fatalf("Ask 回显 = %v, 期望 hello-lazy", reply)
	}
}

// mustFixtureOptions 断言夹具配置齐备：配置类问题必须在用例里炸出来（Fatal），
// 不能被后面的「依赖不可达 → Skip」吞掉——那会让用例静默变成永不执行。
func mustFixtureOptions(t *testing.T, o Options) {
	t.Helper()
	switch {
	case o.NodeID == "":
		t.Fatal("夹具缺少 NodeID（装配必填）")
	case o.Namespace == "":
		t.Fatal("夹具缺少 Namespace（R9：命名空间缺失即装配失败，不得被当作依赖不可用跳过）")
	case len(o.EtcdEndpoints) == 0:
		t.Fatal("夹具缺少 EtcdEndpoints（装配必填）")
	case o.NatsURL == "":
		t.Fatal("夹具缺少 NatsURL（装配必填）")
	}
}

// failOrSkipUnavailable 区分两类失败：装配**配置**错误（namespace 非法等）一律 Fatal，
// 绝不当成「依赖不可用」跳过；只有真·后端不可达才 Skip（本机没起 etcd/NATS 时去 10.10.9.36 跑）。
func failOrSkipUnavailable(t *testing.T, what string, err error) {
	t.Helper()
	if errors.Is(err, namespace.ErrInvalid) {
		t.Fatalf("%s：装配配置错误（不得跳过）: %v", what, err)
	}
	t.Skipf("%s（NATS/etcd 不可达）: %v", what, err)
}

// withNodeID 复制 Options 并设置 NodeID。
func withNodeID(o Options, nodeID string) Options {
	o.NodeID = nodeID
	return o
}
