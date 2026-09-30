// 迁移隔离窗口用例（批次 7 ⑥.3）：用**真实框架集群运行时**扮演旧属主节点，
// 验证 drain（DrainAndRelease 释放目录归属）之后立即安装隔离窗口，
// 使旧节点在「Release↔新属主 Claim」空窗内无法再懒激活同一 battle:<id>
// （否则会与新属主形成双 actor：重复推进帧、状态分叉）。
package migrate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/contrib/locator/memory"
)

// noopBattleHandler 是旧属主节点上占位用的战斗 actor（用例只关心归属与激活，不关心业务）。
type noopBattleHandler struct{}

// OnStart 空实现。
func (h *noopBattleHandler) OnStart(core.ActorContext) error { return nil }

// OnStop 空实现。
func (h *noopBattleHandler) OnStop(core.ActorContext, types.ExitReason) error { return nil }

// OnTell 空实现。
func (h *noopBattleHandler) OnTell(core.ActorContext, any) error { return nil }

// OnAsk 空实现。
func (h *noopBattleHandler) OnAsk(core.ActorContext, any) (any, error) { return nil, nil }

// oldOwnerFixture 是「旧属主节点」装置：真实集群运行时（内存 Locator + 内存传输）
// + 真实框架 ClusterOps（drain 走 DrainAndRelease）+ 被测迁移装饰器。
type oldOwnerFixture struct {
	rt   *cluster.Runtime
	dir  cluster.Directory
	ops  *Ops
	pid  types.PID
	node string
}

// newOldOwnerFixture 构造装置：node-a 已注册 battle actor 类型（尚未拉起实例）。
func newOldOwnerFixture(t *testing.T) *oldOwnerFixture {
	t.Helper()
	ctx := context.Background()
	const node = "node-a"
	pid, err := types.NewPID(consts.ActorTypeBattle, "b1")
	if err != nil {
		t.Fatalf("NewPID: %v", err)
	}
	dir := cluster.NewDirectory(memory.NewLocator(), node, 10*time.Second)
	rt, err := cluster.NewRuntime(
		cluster.Config{Mode: cluster.ModeCluster, NodeID: node, Lease: cluster.DefaultLease()},
		cluster.WithDirectory(dir), cluster.WithTransport(cluster.NewMemTransport()),
		cluster.WithPlacement(cluster.NewDefaultPlacement("")))
	if err != nil {
		t.Fatalf("cluster.NewRuntime: %v", err)
	}
	if err := rt.Register(core.Props{
		Type: consts.ActorTypeBattle, SpawnMode: core.SpawnAuto,
		NewHandler: func(types.PID) core.Handler { return &noopBattleHandler{} },
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background(), cluster.ShutdownKill) })
	// 导出失败不影响 drain（装饰器按无状态 drain 继续），用例只验证隔离窗口接线。
	port := &fakePort{askErr: errors.New("用例：迁移状态导出失败")}
	inner := cluster.NewClusterOps(dir, rt, "")
	return &oldOwnerFixture{rt: rt, dir: dir, ops: NewOps(inner, port, node, nil).WithQuarantine(rt),
		pid: pid, node: node}
}

// TestDrainInstallsQuarantineOnOldOwner 验证：drain 成功后旧属主立刻进入隔离窗口，
// 窗口内对同一 battle:<id> 的激活（在途帧 op / 重投的激活请求触发的懒激活）被拒，
// 且 cell 未复活——这段空窗正是批次 7 报出的双 actor 竞态。
func TestDrainInstallsQuarantineOnOldOwner(t *testing.T) {
	fx := newOldOwnerFixture(t)
	ctx := context.Background()
	if _, err := fx.rt.Spawn(ctx, fx.pid); err != nil {
		t.Fatalf("前置：旧属主拉起战斗失败: %v", err)
	}

	if err := fx.ops.DrainPIDOn(ctx, fx.pid, fx.node); err != nil {
		t.Fatalf("drain 失败: %v", err)
	}
	// 目录归属已释放：Release 之后、新属主 Claim 之前是真正的空窗。
	if _, err := fx.dir.Lookup(ctx, fx.pid); !types.IsReason(err, types.ReasonNotFound) {
		t.Fatalf("drain 后目录不应再有归属: err=%v", err)
	}
	if !fx.rt.Quarantined(fx.pid) {
		t.Fatal("drain 后旧属主应立即处于隔离窗口内")
	}
	if _, err := fx.rt.Spawn(ctx, fx.pid); !types.IsReason(err, cluster.ReasonQuarantined) {
		t.Fatalf("窗口内旧节点激活应被拒（否则出现第二个 actor）: err=%v", err)
	}
	if _, alive := fx.rt.Local().Stats(fx.pid); alive {
		t.Fatal("被拒的激活不应复活 cell")
	}
	if _, err := fx.dir.Lookup(ctx, fx.pid); !types.IsReason(err, types.ReasonNotFound) {
		t.Fatalf("被拒的激活不应重新 Claim 归属: err=%v", err)
	}
}

// TestDrainFailureDoesNotQuarantine 验证：drain 失败即迁移未发生，不安装隔离窗口
// （旧属主继续服务；隔离只在 Release 成功之后才有意义）。
func TestDrainFailureDoesNotQuarantine(t *testing.T) {
	fx := newOldOwnerFixture(t)
	ctx := context.Background()
	inner := &fakeInner{drainErr: errors.New("用例：drain 失败")}
	ops := NewOps(inner, &fakePort{askErr: errors.New("用例：导出失败")}, fx.node, nil).WithQuarantine(fx.rt)
	if err := ops.DrainPIDOn(ctx, fx.pid, fx.node); err == nil {
		t.Fatal("drain 失败应返回错误")
	}
	if fx.rt.Quarantined(fx.pid) {
		t.Fatal("drain 失败不应安装隔离窗口")
	}
	if _, err := fx.rt.Spawn(ctx, fx.pid); err != nil {
		t.Fatalf("未迁移的战斗应照旧可激活: %v", err)
	}
}

// recordingQuarantiner 记录隔离安装调用（断言窗口取值与是否安装）。
type recordingQuarantiner struct {
	pids    []types.PID
	windows []time.Duration
}

// QuarantinePID 实现 Quarantiner。
func (r *recordingQuarantiner) QuarantinePID(pid types.PID, window time.Duration) error {
	r.pids = append(r.pids, pid)
	r.windows = append(r.windows, window)
	return nil
}

// TestQuarantineWindowMatchesOrchestration 验证本机 drain 路径安装的窗口等于
// QuarantineWindow（正数，覆盖迁移编排的实际时长，取值依据见该常量注释）。
func TestQuarantineWindowMatchesOrchestration(t *testing.T) {
	rec := &recordingQuarantiner{}
	ops := NewOps(&fakeInner{}, &fakePort{askErr: errors.New("用例：导出失败")}, "node-a", nil).WithQuarantine(rec)
	pid, _ := types.NewPID(consts.ActorTypeBattle, "b1")
	if err := ops.DrainPIDOn(context.Background(), pid, "node-a"); err != nil {
		t.Fatalf("drain 失败: %v", err)
	}
	if len(rec.windows) != 1 || rec.windows[0] != QuarantineWindow {
		t.Fatalf("窗口取值应为 %v，实际 %v", QuarantineWindow, rec.windows)
	}
	if QuarantineWindow <= 0 {
		t.Fatalf("窗口必须为正数，实际 %v", QuarantineWindow)
	}
}

// TestQuarantineSkipsNonBattlePID 验证装饰器只对战斗 actor 安装隔离：
// 其它类型原样透传给框架 drain，不产生隔离（本装饰器没有它们的迁移语义）。
func TestQuarantineSkipsNonBattlePID(t *testing.T) {
	rec := &recordingQuarantiner{}
	inner := &fakeInner{}
	ops := NewOps(inner, &fakePort{}, "node-a", nil).WithQuarantine(rec)
	pid, err := types.NewPID("room", "r1")
	if err != nil {
		t.Fatalf("NewPID: %v", err)
	}
	if err := ops.DrainPIDOn(context.Background(), pid, "node-a"); err != nil {
		t.Fatalf("非战斗 PID 应透传 drain: %v", err)
	}
	if len(inner.drained) != 1 || len(rec.pids) != 0 {
		t.Fatalf("非战斗 PID 不应安装隔离：drained=%v quarantined=%v", inner.drained, rec.pids)
	}
}

// TestQuarantineSkipsRemoteDrain 记录当前边界：跨节点 drain（fromNode != 本机）由
// **执行 Release 的旧属主节点**自行安装隔离，本编排器不把窗口误装到本机。
// 本模板的迁移由旧属主节点的收件箱驱动（见 actor.MigrationInbox），故该分支未启用；
// 若将来接入跨节点 drain 的编排器路径，隔离需在框架远端 drain 落点安装。
func TestQuarantineSkipsRemoteDrain(t *testing.T) {
	rec := &recordingQuarantiner{}
	inner := &fakeInner{}
	ops := NewOps(inner, &fakePort{askErr: errors.New("用例：导出失败")}, "node-a", nil).WithQuarantine(rec)
	pid, _ := types.NewPID(consts.ActorTypeBattle, "b1")
	if err := ops.DrainPIDOn(context.Background(), pid, "node-b"); err != nil {
		t.Fatalf("drain 失败: %v", err)
	}
	if len(inner.drained) != 1 {
		t.Fatalf("drain 应已下发到旧属主: %v", inner.drained)
	}
	if len(rec.pids) != 0 {
		t.Fatalf("跨节点 drain 不应在本机安装隔离: %v", rec.pids)
	}
}
