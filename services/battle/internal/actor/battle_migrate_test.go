// 战斗 actor 迁移用例（规格 §8/§9.6）：单机两 runtime 模拟两节点，
// 用内存实现替代 etcd 目录与 NATS 传输，验证迁移编排与 actor 侧状态搬运的真实行为。
//
// 覆盖三条断言：
//  1. 迁移后新节点继续推进帧，参战名单与帧号一致（不回到第 0 帧）；
//  2. 迁移窗口内掉线不判负，窗口关闭后重新起算（规格 §9.6）；
//  3. epoch 栅栏：陈旧状态（source_epoch 不小于本实例 epoch）拒绝恢复。
package actor

import (
	"context"
	"sync"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/migrate"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	"github.com/huangyuCN/atlas/contrib/actor/rollout"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	lockstepimpl "github.com/huangyuCN/atlas/contrib/lockstep"
)

// 两节点用例的节点 ID 与战斗 PID。
const (
	nodeA      = "node-a"
	nodeB      = "node-b"
	migrateBID = "b-test01"
)

// twoNodePort 是迁移 Port 的内存实现：按 PID 类型把投递路由到对应节点的本地运行时
// （battlemigrate:<node> 直接按 UID 定位节点，battle:<id> 查内存归属表）。
type twoNodePort struct {
	nodes map[string]*core.LocalRuntime
	ops   *memMigrateOps
}

// Ask 实现 migrate.Port。
func (p *twoNodePort) Ask(ctx context.Context, pid types.PID, req any) (any, error) {
	rt, err := p.runtimeOf(pid)
	if err != nil {
		return nil, err
	}
	return rt.Ask(ctx, pid, req)
}

// Tell 实现 migrate.Port。
func (p *twoNodePort) Tell(ctx context.Context, pid types.PID, msg any) error {
	rt, err := p.runtimeOf(pid)
	if err != nil {
		return err
	}
	return rt.Tell(ctx, pid, msg)
}

// runtimeOf 返回 PID 当前所在节点的运行时。
func (p *twoNodePort) runtimeOf(pid types.PID) (*core.LocalRuntime, error) {
	if pid.Type() == consts.ActorTypeBattleMigrate {
		return p.nodes[pid.UID()], nil
	}
	if rt, ok := p.nodes[p.ops.ownerOf(pid)]; ok {
		return rt, nil
	}
	return nil, types.NewNotFoundErr(pid)
}

// memMigrateOps 是 rollout.ClusterOps 的内存实现：归属表 + epoch 单调抬升 + 真实停/起。
type memMigrateOps struct {
	mu    sync.Mutex
	nodes map[string]*core.LocalRuntime
	owner map[string]string
	epoch map[string]uint64
}

// ownerOf 返回 PID 当前属主节点（空 = 无归属）。
func (o *memMigrateOps) ownerOf(pid types.PID) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.owner[pid.String()]
}

// DrainPID 实现 rollout.ClusterOps：本机 drain（用例里等于 OwnerNode 所在节点）。
func (o *memMigrateOps) DrainPID(ctx context.Context, pid types.PID) error {
	return o.DrainPIDOn(ctx, pid, o.ownerOf(pid))
}

// DrainPIDOn 实现 rollout.ClusterOps：停 cell + 释放归属（幂等）。
func (o *memMigrateOps) DrainPIDOn(ctx context.Context, pid types.PID, fromNode string) error {
	o.mu.Lock()
	cur := o.owner[pid.String()]
	o.mu.Unlock()
	if cur == "" {
		return nil // 幂等：已无主
	}
	if fromNode != "" && cur != fromNode {
		return types.NewNotOwnerErr(pid)
	}
	if rt := o.nodes[cur]; rt != nil {
		if err := rt.Stop(ctx, pid); err != nil {
			return err
		}
	}
	o.mu.Lock()
	delete(o.owner, pid.String())
	o.mu.Unlock()
	return nil
}

// ActivatePID 实现 rollout.ClusterOps：本机激活。
func (o *memMigrateOps) ActivatePID(ctx context.Context, pid types.PID) (rollout.OwnerEpoch, error) {
	return o.ActivatePIDOn(ctx, pid, "")
}

// ActivatePIDOn 实现 rollout.ClusterOps：Claim（抬 epoch）+ 在目标节点拉起。
func (o *memMigrateOps) ActivatePIDOn(ctx context.Context, pid types.PID, preferNode string) (rollout.OwnerEpoch, error) {
	target := preferNode
	if target == "" {
		target = nodeA
	}
	rt := o.nodes[target]
	if rt == nil {
		return rollout.OwnerEpoch{}, types.NewNotFoundErr(pid)
	}
	o.mu.Lock()
	o.epoch[pid.String()]++
	epoch := o.epoch[pid.String()]
	o.owner[pid.String()] = target
	o.mu.Unlock()
	if _, err := rt.Spawn(ctx, pid, core.WithSpawnEpoch(epoch)); err != nil {
		return rollout.OwnerEpoch{}, err
	}
	return rollout.OwnerEpoch{PID: pid.String(), OwnerNode: target, Epoch: epoch}, nil
}

// VerifyPID 实现 rollout.ClusterOps。
func (o *memMigrateOps) VerifyPID(ctx context.Context, pid types.PID, wantEpoch uint64) error {
	return o.VerifyPIDOn(ctx, pid, "", wantEpoch)
}

// VerifyPIDOn 实现 rollout.ClusterOps：归属与 epoch 双校验。
func (o *memMigrateOps) VerifyPIDOn(_ context.Context, pid types.PID, wantNode string, wantEpoch uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.owner[pid.String()] == "" {
		return types.NewNotFoundErr(pid)
	}
	if wantNode != "" && o.owner[pid.String()] != wantNode {
		return types.NewNotOwnerErr(pid)
	}
	if o.epoch[pid.String()] != wantEpoch {
		return types.NewEpochMismatchErr(pid, wantEpoch, o.epoch[pid.String()])
	}
	return nil
}

// LookupSnapshot 实现 rollout.ClusterOps。
func (o *memMigrateOps) LookupSnapshot(_ context.Context, pids []types.PID) ([]rollout.OwnerEpoch, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]rollout.OwnerEpoch, 0, len(pids))
	for _, pid := range pids {
		out = append(out, rollout.OwnerEpoch{PID: pid.String(),
			OwnerNode: o.owner[pid.String()], Epoch: o.epoch[pid.String()]})
	}
	return out, nil
}

// migrateNode 是单个节点的运行时与推送记录。
type migrateNode struct {
	id     string
	rt     *core.LocalRuntime
	pusher *memPusher
	result *memResultRepo
	pub    *memPublisher
}

// migrateEnv 是两节点迁移用例环境。
type migrateEnv struct {
	a     *migrateNode
	b     *migrateNode
	ops   *memMigrateOps
	inner *migrate.Ops
	pid   types.PID
}

// startMigrateNode 起一个节点的本地运行时并注册战斗 actor 与迁移收件箱。
func startMigrateNode(t *testing.T, id string, runner MigrateRunner, cfg Config, presence *fakePresence,
	metrics *staticCollector) *migrateNode {
	t.Helper()
	// 节点 ID 必须显式设置：战斗 actor 按 ctx.Node() 定位本机迁移收件箱（battlemigrate:<node>）。
	rt, err := core.NewLocalRuntime(core.WithNodeID(id))
	if err != nil {
		t.Fatalf("NewLocalRuntime(%s): %v", id, err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Runtime Start(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	reg, err := pubsub.New(rt)
	if err != nil {
		t.Fatalf("pubsub(%s): %v", id, err)
	}
	node := &migrateNode{id: id, rt: rt, pusher: newMemPusher(), result: &memResultRepo{}, pub: &memPublisher{}}
	err = rt.Register(NewProps(Props{
		Rt: localRT{rt}, Registry: reg, Storage: lockstepimpl.NewMemoryStorage(),
		ResultRepo: node.result, Pusher: node.pusher, Publisher: node.pub,
		Presence: presence, Metrics: metrics, Cfg: cfg,
	}))
	if err != nil {
		t.Fatalf("Register battle(%s): %v", id, err)
	}
	if err := rt.Register(NewMigrationInboxProps(id, runner, nil)); err != nil {
		t.Fatalf("Register inbox(%s): %v", id, err)
	}
	return node
}

// newMigrateEnv 起两节点环境；战斗 actor 尚未拉起（由 start 在 node-a 拉起）。
func newMigrateEnv(t *testing.T, presence *fakePresence, cfg Config) *migrateEnv {
	t.Helper()
	col := newStaticCollector()
	ops := &memMigrateOps{owner: map[string]string{}, epoch: map[string]uint64{}}
	port := &twoNodePort{nodes: map[string]*core.LocalRuntime{}, ops: ops}
	pid, err := types.NewPID(consts.ActorTypeBattle, migrateBID)
	if err != nil {
		t.Fatalf("NewPID: %v", err)
	}
	runner := &inboxRunnerSpy{}
	env := &migrateEnv{ops: ops, pid: pid}
	env.a = startMigrateNode(t, nodeA, runner, cfg, presence, col)
	env.b = startMigrateNode(t, nodeB, runner, cfg, presence, col)
	// 两个假件都在节点建好之后才装配节点表（收件箱 Props 已注册，但只在调用时才经端口寻址）。
	ops.nodes = map[string]*core.LocalRuntime{nodeA: env.a.rt, nodeB: env.b.rt}
	port.nodes[nodeA], port.nodes[nodeB] = env.a.rt, env.b.rt
	env.inner = migrate.NewOps(ops, port, nodeA, nil)
	runner.inner = env.inner
	return env
}

// sendInputsAt 在指定节点投递帧输入（本局 PID 由环境给出）。
func (e *migrateEnv) sendInputsAt(t *testing.T, node *migrateNode, from, n uint64) {
	t.Helper()
	sendInputs(t, node, e.pid, from, n)
}

// inboxPID 返回某节点的迁移收件箱 PID。
func inboxPID(t *testing.T, node string) types.PID {
	t.Helper()
	pid, err := types.NewPID(consts.ActorTypeBattleMigrate, node)
	if err != nil {
		t.Fatalf("收件箱 PID(%s): %v", node, err)
	}
	return pid
}

// inboxRunnerSpy 把编排转发到 migrate.Ops（收件箱在两个节点都需要它）。
type inboxRunnerSpy struct{ inner *migrate.Ops }

// Migrate 实现 MigrateRunner。
func (s *inboxRunnerSpy) Migrate(ctx context.Context, battleID, target string) (migrate.Outcome, error) {
	return s.inner.Migrate(ctx, battleID, target)
}
