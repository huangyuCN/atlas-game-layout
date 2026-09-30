// 迁移装饰器与选点用例：用内存假件覆盖 rollout.ClusterOps 与投递端口，
// 验证「状态搬运插进 drain/activate 之间」的接线、栅栏与失败回滚语义。
package migrate

import (
	"context"
	"errors"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas/contrib/actor/rebalance"
	"github.com/huangyuCN/atlas/contrib/actor/rollout"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"google.golang.org/protobuf/proto"
)

// fakePort 是迁移投递端口的内存假件：记录调用序列与最后一次预置的状态。
type fakePort struct {
	asked   []string
	told    []string
	state   *battlev1.BattleMigrationState
	askByte bool // true = 以线格式字节回执（模拟跨节点 Ask）
	askErr  error
	tellErr error
	staged  *battlev1.BattleMigrationState
	resumed bool
}

// Ask 实现 Port。
func (p *fakePort) Ask(_ context.Context, pid types.PID, req any) (any, error) {
	if p.askErr != nil {
		return nil, p.askErr
	}
	p.asked = append(p.asked, pid.String())
	switch req.(type) {
	case *battlev1.PrepareBattleMigrationRequest:
		if p.askByte {
			raw, err := proto.Marshal(p.state)
			if err != nil {
				return nil, err
			}
			return raw, nil
		}
		return p.state, nil
	case *battlev1.ResumeBattleMigrationRequest:
		p.resumed = true
		return &battlev1.BattleMigrationState{}, nil
	}
	return nil, errors.New("fakePort: 未预期的请求类型")
}

// Tell 实现 Port。
func (p *fakePort) Tell(_ context.Context, pid types.PID, msg any) error {
	if p.tellErr != nil {
		return p.tellErr
	}
	p.told = append(p.told, pid.String())
	if req, ok := msg.(*battlev1.StageBattleMigrationRequest); ok {
		p.staged = req.GetState()
	}
	return nil
}

// fakeInner 是 rollout.ClusterOps 的内存假件。
type fakeInner struct {
	drained     []types.PID
	activated   []types.PID
	epoch       uint64
	lookup      []rollout.OwnerEpoch
	drainErr    error
	activateErr error
	verified    uint64
}

// DrainPID 实现 rollout.ClusterOps。
func (f *fakeInner) DrainPID(ctx context.Context, pid types.PID) error {
	return f.DrainPIDOn(ctx, pid, "")
}

// DrainPIDOn 实现 rollout.ClusterOps。
func (f *fakeInner) DrainPIDOn(_ context.Context, pid types.PID, _ string) error {
	if f.drainErr != nil {
		return f.drainErr
	}
	f.drained = append(f.drained, pid)
	return nil
}

// ActivatePID 实现 rollout.ClusterOps。
func (f *fakeInner) ActivatePID(ctx context.Context, pid types.PID) (rollout.OwnerEpoch, error) {
	return f.ActivatePIDOn(ctx, pid, "")
}

// ActivatePIDOn 实现 rollout.ClusterOps。
func (f *fakeInner) ActivatePIDOn(_ context.Context, pid types.PID, node string) (rollout.OwnerEpoch, error) {
	if f.activateErr != nil {
		return rollout.OwnerEpoch{}, f.activateErr
	}
	f.activated = append(f.activated, pid)
	return rollout.OwnerEpoch{PID: pid.String(), OwnerNode: node, Epoch: f.epoch}, nil
}

// VerifyPID 实现 rollout.ClusterOps。
func (f *fakeInner) VerifyPID(ctx context.Context, pid types.PID, want uint64) error {
	return f.VerifyPIDOn(ctx, pid, "", want)
}

// VerifyPIDOn 实现 rollout.ClusterOps。
func (f *fakeInner) VerifyPIDOn(_ context.Context, _ types.PID, _ string, want uint64) error {
	f.verified = want
	return nil
}

// LookupSnapshot 实现 rollout.ClusterOps。
func (f *fakeInner) LookupSnapshot(_ context.Context, _ []types.PID) ([]rollout.OwnerEpoch, error) {
	return f.lookup, nil
}

// newHarness 构造一对假件与被测装饰器（默认：node-a 持有战斗、epoch 1）。
func newHarness() (*Ops, *fakeInner, *fakePort, types.PID) {
	inner := &fakeInner{epoch: 2, lookup: []rollout.OwnerEpoch{{OwnerNode: "node-a", Epoch: 1}}}
	port := &fakePort{state: &battlev1.BattleMigrationState{
		BattleId: "b1", Players: []string{"p-a"}, SourceEpoch: 1, SourceNode: "node-a", SnapshotFrame: 9}}
	pid, _ := types.NewPID("battle", "b1")
	return NewOps(inner, port, "node-a", nil), inner, port, pid
}

// TestOpsCarriesStateAcrossDrainAndActivate 验证 drain 取状态、activate 预置状态。
func TestOpsCarriesStateAcrossDrainAndActivate(t *testing.T) {
	ops, inner, port, pid := newHarness()
	ctx := context.Background()
	if err := ops.DrainPIDOn(ctx, pid, "node-a"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(port.asked) != 1 || len(inner.drained) != 1 {
		t.Fatalf("drain 未取状态：asks=%v drained=%v", port.asked, inner.drained)
	}
	if ops.Stat().Pending != 1 {
		t.Fatalf("drain 后应保留待搬运状态，实际 %d", ops.Stat().Pending)
	}
	oe, err := ops.ActivatePIDOn(ctx, pid, "node-b")
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if port.staged == nil || port.staged.GetSnapshotFrame() != 9 {
		t.Fatalf("activate 未预置状态: %+v", port.staged)
	}
	if oe.Epoch != 2 || len(inner.activated) != 1 {
		t.Fatalf("activate 回执不符: %+v", oe)
	}
	if ops.Stat().Pending != 0 {
		t.Fatalf("搬运完成后不应残留待搬运状态，实际 %d", ops.Stat().Pending)
	}
}

// TestOpsDecodesWireReply 验证跨节点 Ask 的线格式回执（[]byte）也能解码。
func TestOpsDecodesWireReply(t *testing.T) {
	ops, _, port, pid := newHarness()
	port.askByte = true
	if err := ops.DrainPIDOn(context.Background(), pid, "node-a"); err != nil {
		t.Fatalf("drain（线格式回执）: %v", err)
	}
	if ops.Stat().Pending != 1 {
		t.Fatal("线格式回执未被解码")
	}
}

// TestOpsRejectsStaleEpoch 验证栅栏：激活后 epoch 未抬升即作废（不静默退化）。
func TestOpsRejectsStaleEpoch(t *testing.T) {
	ops, inner, _, pid := newHarness()
	inner.epoch = 0 // 比导出时的代际更旧
	ctx := context.Background()
	if err := ops.DrainPIDOn(ctx, pid, "node-a"); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if _, err := ops.ActivatePIDOn(ctx, pid, "node-b"); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("陈旧 epoch 应被拒绝，实际 %v", err)
	}
}

// TestOpsDrainFailureResumesOldOwner 验证 drain 失败即通知旧属主退出迁移窗口。
func TestOpsDrainFailureResumesOldOwner(t *testing.T) {
	ops, inner, port, pid := newHarness()
	inner.drainErr = errors.New("boom")
	if err := ops.DrainPIDOn(context.Background(), pid, "node-a"); err == nil {
		t.Fatal("drain 失败应如实返回错误")
	}
	if !port.resumed {
		t.Fatal("drain 失败应通知旧属主退出迁移窗口")
	}
	if ops.Stat().Pending != 0 {
		t.Fatal("drain 失败不应残留待搬运状态")
	}
}

// TestOpsPassthroughNonBattle 验证非战斗 actor 原样透传（装饰器可挂到全局编排器上）。
func TestOpsPassthroughNonBattle(t *testing.T) {
	ops, inner, port, _ := newHarness()
	pid, _ := types.NewPID("player", "p-a")
	if err := ops.DrainPIDOn(context.Background(), pid, "node-a"); err != nil {
		t.Fatalf("透传 drain: %v", err)
	}
	if len(port.asked) != 0 {
		t.Fatal("非战斗 actor 不应取状态")
	}
	if len(inner.drained) != 1 {
		t.Fatal("非战斗 actor 应直接透传给框架实现")
	}
}

// TestMigratePipeline 验证 Migrate 的 drain → activate → verify 闭环与结果摘要。
func TestMigratePipeline(t *testing.T) {
	ops, inner, port, _ := newHarness()
	inner.lookup = []rollout.OwnerEpoch{{OwnerNode: "node-a", Epoch: 1}}
	out, err := ops.Migrate(context.Background(), "b1", "node-b")
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if out.FromNode != "node-a" || out.ToNode != "node-b" || out.Epoch != 2 {
		t.Fatalf("迁移结果不符: %+v", out)
	}
	if inner.verified != 2 {
		t.Fatalf("未按新 epoch 校验归属: %d", inner.verified)
	}
	if port.staged == nil {
		t.Fatal("drain 之前应已完成预置（缩短归属空窗）")
	}
}

// TestMigrateRejectsSameTarget 验证同节点迁移被明确拒绝（不产生无意义的重启）。
func TestMigrateRejectsSameTarget(t *testing.T) {
	ops, _, _, _ := newHarness()
	if _, err := ops.Migrate(context.Background(), "b1", "node-a"); err == nil {
		t.Fatal("目标节点即当前属主时应拒绝")
	}
}

// TestChooseTargetUsesRebalancePlan 验证选点复用 rebalance 的迁移计划（失衡时按计划迁出）。
func TestChooseTargetUsesRebalancePlan(t *testing.T) {
	pid, _ := types.NewPID("battle", "b1")
	snap := &rebalance.Snapshot{
		NodeCounts: map[string]int{"node-a": 2, "node-b": 0},
		Samples:    map[string][]types.PID{"node-a": {pid}},
	}
	nodes := []rebalance.Node{{ID: "node-a", ProcCount: 2}, {ID: "node-b", ProcCount: 0}}
	target, err := ChooseTarget(context.Background(), snap, nodes, pid, "node-a", Picker{})
	if err != nil {
		t.Fatalf("ChooseTarget: %v", err)
	}
	if target != "node-b" {
		t.Fatalf("应迁往 node-b，实际 %q", target)
	}
}

// TestChooseTargetFallsBackWhenBalanced 验证负载均衡（无计划）时回落选点：
// 迁移是运维显式触发，不能因为「恰好均衡」而拒绝执行。
func TestChooseTargetFallsBackWhenBalanced(t *testing.T) {
	pid, _ := types.NewPID("battle", "b1")
	snap := &rebalance.Snapshot{
		NodeCounts: map[string]int{"node-a": 1, "node-b": 1},
		Samples:    map[string][]types.PID{"node-a": {pid}},
	}
	nodes := []rebalance.Node{{ID: "node-a", ProcCount: 1}, {ID: "node-b", ProcCount: 1}}
	target, err := ChooseTarget(context.Background(), snap, nodes, pid, "node-a", Picker{})
	if err != nil {
		t.Fatalf("ChooseTarget: %v", err)
	}
	if target != "node-b" {
		t.Fatalf("均衡时应按负载/节点序选 node-b，实际 %q", target)
	}
}

// TestPickerExcludesSource 验证选点排除源节点（否则迁移会原地打转）。
func TestPickerExcludesSource(t *testing.T) {
	nodes := []rebalance.Node{{ID: "node-a", ProcCount: 0}}
	if _, err := (Picker{}).Select(context.Background(), nodes, "node-a"); !errors.Is(err, ErrNoTarget) {
		t.Fatalf("只剩源节点时应报无可用目标，实际 %v", err)
	}
}
