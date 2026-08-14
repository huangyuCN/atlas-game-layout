package actor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
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
func TestClusterLazyActivation(t *testing.T) {
	base := Options{
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
		ServiceName:   "atlas-actor",
		Discovery: &fakeDiscovery{instances: []*registry.ServiceInstance{
			{ID: "m2-nodeA", Name: "atlas-actor"},
			{ID: "m2-nodeB", Name: "atlas-actor"},
		}},
	}
	rtA, err := NewRuntime(withNodeID(base, "m2-nodeA"))
	if err != nil {
		t.Skipf("actor 集群不可用（nats 连接失败）: %v", err)
	}
	rtB, err := NewRuntime(withNodeID(base, "m2-nodeB"))
	if err != nil {
		rtA.Shutdown(context.Background())
		t.Skipf("actor 集群不可用: %v", err)
	}

	ctx := context.Background()
	if err := rtA.Start(ctx); err != nil {
		t.Skipf("etcd 不可用（目录启动失败）: %v", err)
	}
	if err := rtB.Start(ctx); err != nil {
		rtA.Shutdown(ctx)
		t.Skipf("etcd 不可用: %v", err)
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

// withNodeID 复制 Options 并设置 NodeID。
func withNodeID(o Options, nodeID string) Options {
	o.NodeID = nodeID
	return o
}
