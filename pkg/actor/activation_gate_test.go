package actor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/registry"
)

// switchGate 是激活闸门桩：记录被询问的 PID，按当前开关放行或拒绝（err 可切换）。
type switchGate struct {
	mu   sync.Mutex
	seen []string
	err  error
}

// AllowActivation 实现 cluster.ActivationGate：记录问询并返回当前设定的错误。
func (g *switchGate) AllowActivation(_ context.Context, pid types.PID) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seen = append(g.seen, pid.String())
	return g.err
}

// setErr 切换闸门判定（nil = 放行）。
func (g *switchGate) setErr(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.err = err
}

// asked 返回闸门被询问过的 PID 快照。
func (g *switchGate) asked() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.seen...)
}

// reasonArchived 是闸门拒绝用的稳定 reason（结构化错误才能跨投递路径保留 reason；
// 裸错误会在集群边界退化成 TRANSPORT_MISSING_REASON 500）。
const reasonArchived = "BATTLE_ENDED"

// gateFixture 是带闸门的单节点运行时装置（依赖不可用即跳过用例）。
type gateFixture struct {
	rt   *Runtime
	gate *switchGate
	pid  types.PID
	ctx  context.Context
}

// newGateFixture 起一个节点（真 etcd/nats，缺失即跳过；按 AGENTS.md 在集成服务器执行），
// 注册一个 SpawnAuto 类型并装好「先拒绝」的闸门。
func newGateFixture(t *testing.T) *gateFixture {
	t.Helper()
	gate := &switchGate{err: atlaserrors.Conflict(reasonArchived, "对局已结束（留档）")}
	suffix := fmt.Sprintf("-%d", time.Now().UnixNano())
	nodeID := "m2-gate" + suffix
	rt, err := NewRuntime(Options{
		NodeID:        nodeID,
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
		ServiceName:   "atlas-actor",
		Namespace:     "it",
		Discovery: &fakeDiscovery{instances: []*registry.ServiceInstance{
			{ID: nodeID, Name: "atlas-actor"},
		}},
		ActivationGate: gate,
	})
	if err != nil {
		t.Skipf("actor 集群不可用（nats 连接失败）: %v", err)
	}
	ctx := context.Background()
	if err := rt.Start(ctx); err != nil {
		t.Skipf("etcd 不可用（目录启动失败）: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	props := core.Props{
		Type:       "gateecho",
		NewHandler: func(types.PID) core.Handler { return &echoHandler{} },
		SpawnMode:  core.SpawnAuto,
	}
	if err := rt.Register(props); err != nil {
		t.Fatalf("Register: %v", err)
	}
	pid, err := ParsePID("gateecho:g-1")
	if err != nil {
		t.Fatalf("ParsePID: %v", err)
	}
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	t.Cleanup(cancel)
	return &gateFixture{rt: rt, gate: gate, pid: pid, ctx: tctx}
}

// TestActivationGateBlocksSpawnAuto 验证 Options.ActivationGate 接在**激活路径**上：
// 闸门拒绝时懒激活失败、错误原样上抛、cell 未被创建。
func TestActivationGateBlocksSpawnAuto(t *testing.T) {
	fx := newGateFixture(t)
	err := fx.rt.Tell(fx.ctx, fx.pid, []byte("blocked"))
	if err == nil || !types.IsReason(err, reasonArchived) {
		t.Fatalf("闸门拒绝时 Tell 错误 = %v（reason=%q），期望 %q",
			err, atlaserrors.Reason(err), reasonArchived)
	}
	if _, ok := fx.rt.Raw().Local().Stats(fx.pid); ok {
		t.Fatal("闸门拒绝后不得创建 cell")
	}
	if asked := fx.gate.asked(); len(asked) != 1 || asked[0] != fx.pid.String() {
		t.Fatalf("闸门问询记录 = %v，期望 [%s]", asked, fx.pid.String())
	}
}

// TestActivationGateAllowsAndSkipsReuse 验证闸门放行后同一 PID 正常激活，
// 且**复用快路径不再询问**闸门（闸门只管创建，不管投递）。
func TestActivationGateAllowsAndSkipsReuse(t *testing.T) {
	fx := newGateFixture(t)
	fx.gate.setErr(nil)
	if err := fx.rt.Tell(fx.ctx, fx.pid, []byte("allowed")); err != nil {
		t.Fatalf("闸门放行后 Tell: %v", err)
	}
	if _, ok := fx.rt.Raw().Local().Stats(fx.pid); !ok {
		t.Fatal("闸门放行后应已创建 cell")
	}
	before := len(fx.gate.asked())
	if err := fx.rt.Tell(fx.ctx, fx.pid, []byte("again")); err != nil {
		t.Fatalf("复用快路径 Tell: %v", err)
	}
	if after := len(fx.gate.asked()); after != before {
		t.Fatalf("复用快路径不应询问闸门：问询数 %d → %d", before, after)
	}
}
