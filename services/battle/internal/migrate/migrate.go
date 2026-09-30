// Package migrate 实现战斗 actor 的迁移编排（阶段 3 批次 7，规格 §8）：
// 在框架 rollout.ClusterOps（drain / activate / verify，目录属主与 epoch 语义）之外，
// 补上框架没有的那一环——**业务状态的搬运**。
//
// 框架的能力边界（读 contrib/actor/{rollout,rebalance} 得到的事实）：
//   - rollout.ClusterOps 只做「停止接受新消息 → 停 cell → 释放归属 → 在新节点 Claim+拉起」，
//     拉起走 core.Props.NewHandler + OnStart，**不带任何业务状态**；
//   - rebalance/rollout 的编排器（状态机、锁、计划）只消费 ClusterOps 接口，
//     因此「带状态搬运」的正确扩展点是**装饰 ClusterOps**，而不是改编排器。
//
// 于是本包给出 Ops：一个实现 rollout.ClusterOps 的装饰器，把状态搬运插进
// Drain/Activate 两步之间：
//
//	Prepare（旧属主：暂停掉线计时 + 暂停会话 + 导出状态）
//	  → Stage（把状态预置到目标节点的本机收件箱）
//	  → Drain（框架语义：DRAINING → Stop(Relocation) → Release）
//	  → Quarantine（旧属主隔离窗口：窗口内拒绝懒激活本 PID，盖住 Release↔Claim 空窗）
//	  → Activate（框架语义：Claim 抬 epoch → 新节点拉起）
//	  → 新属主 OnStart 在建立会话 actor **之前**消费收件箱状态，会话按快照恢复到同一帧
//
// 栅栏：状态携带导出时的 epoch，新属主只在「自己的 epoch 更大」时认这份状态
// （目录 Claim 单调抬 epoch，陈旧状态一律丢弃，不产生双活或状态回退）。
//
// 隔离窗口（批次 7 ⑥.3）：drain 成功即旧属主已 Release 目录归属，而新属主 Claim 之前
// 目录里没有归属记录——旧节点若被在途帧 op 或重投的激活请求命中，会重新 Claim 并拉起
// cell，与新属主形成双 actor（重复推进帧、状态分叉）。因此 DrainPIDOn 在框架 drain
// 成功之后立刻经 Quarantiner 安装隔离窗口（时长与依据见 QuarantineWindow）。
package migrate

import (
	"context"
	"errors"
	"fmt"
	"sync"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/rollout"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/log"
	"google.golang.org/protobuf/proto"
)

// ErrStaleEpoch 表示激活后的 epoch 未超过导出时的 epoch：状态搬运作废（不静默退化）。
var ErrStaleEpoch = errors.New("migrate: 目标节点 epoch 未超过导出 epoch")

// ErrBadState 表示旧属主没能给出可搬运的状态（导出失败或回执类型不符）。
var ErrBadState = errors.New("migrate: 迁移状态导出失败")

// Port 是迁移对 actor 平面的最小依赖（按 PID 寻址的跨节点投递）。
// 生产实现是 pkg/actor.Runtime（Ask/Tell 自动按目录路由）；单测注入内存实现。
type Port interface {
	// Ask 向 pid 发起同步请求（收件箱取状态、旧属主导出状态）。
	Ask(ctx context.Context, pid types.PID, req any) (any, error)
	// Tell 向 pid 单向投递（预置状态、中止恢复）。
	Tell(ctx context.Context, pid types.PID, msg any) error
}

// Outcome 是一次迁移的结果摘要（日志与管理面回执用）。
type Outcome struct {
	// FromNode 是迁出节点（迁移前的属主）。
	FromNode string
	// ToNode 是迁入节点（迁移后的属主）。
	ToNode string
	// Epoch 是迁入后目录里的新 epoch。
	Epoch uint64
}

// Ops 是带状态搬运的 rollout.ClusterOps 装饰器：仅对**战斗 actor** 做搬运，
// 其余 PID 原样透传（装饰器可安全地挂到 rebalance/rollout 的编排器上）。
type Ops struct {
	inner rollout.ClusterOps
	port  Port
	node  string // 本节点 ID（Activate 未指定 preferNode 时即本机）
	log   log.Logger

	mu          sync.Mutex
	pending     map[string]*staged // pid → 已导出的状态与目标节点（drain 与 activate 之间）
	chooser     *AutoChooser       // 可选：Migrate 未指定目标时按 rebalance 计划选点
	quarantiner Quarantiner        // 可选：drain 后在旧属主安装隔离窗口（见 quarantine.go）
}

// WithChooser 注入自动选点（不注入时 Migrate 必须显式给出目标节点）。
func (o *Ops) WithChooser(c *AutoChooser) *Ops {
	o.chooser = c
	return o
}

// staged 是一次待完成搬运的状态。
type staged struct {
	state *battlev1.BattleMigrationState
	// stagedOn 是已预置状态的目标节点（空 = 尚未预置，Activate 时补做）。
	stagedOn string
}

// NewOps 构造装饰器；node 为本节点 ID，port 为 actor 平面投递端口。
func NewOps(inner rollout.ClusterOps, port Port, node string, logger log.Logger) *Ops {
	if logger == nil {
		logger = log.DefaultLogger
	}
	return &Ops{inner: inner, port: port, node: node, log: logger,
		pending: make(map[string]*staged)}
}

// Stat 报告装饰器是否被使用过（测试与观测用）。
type Stat struct {
	// Pending 是尚未完成搬运的 PID 数。
	Pending int
}

// Stat 返回当前待完成搬运的统计。
func (o *Ops) Stat() Stat {
	o.mu.Lock()
	defer o.mu.Unlock()
	return Stat{Pending: len(o.pending)}
}

// IsBattlePID 报告 PID 是否属于战斗 actor（只有战斗 actor 需要状态搬运）。
func IsBattlePID(pid types.PID) bool { return pid.Type() == consts.ActorTypeBattle }

// InboxPID 返回指定节点的迁移收件箱 PID（battlemigrate:<node_id>）。
func InboxPID(node string) (types.PID, error) {
	return types.NewPID(consts.ActorTypeBattleMigrate, node)
}

// DrainPID 实现 rollout.ClusterOps：本机 drain。
func (o *Ops) DrainPID(ctx context.Context, pid types.PID) error {
	return o.DrainPIDOn(ctx, pid, o.node)
}

// DrainPIDOn 实现 rollout.ClusterOps：先向属主要状态（并在旧属主开启迁移窗口），
// 再走框架 drain；drain 失败即要求旧属主退出窗口（不把旧属主留在「暂停计时」态）。
func (o *Ops) DrainPIDOn(ctx context.Context, pid types.PID, fromNode string) error {
	if !IsBattlePID(pid) {
		return o.inner.DrainPIDOn(ctx, pid, fromNode)
	}
	// 已预置过（Migrate 的 preStage 路径）就不再导出：重复导出只会多一次暂停与状态拷贝。
	state := o.pendingState(pid)
	if state == nil {
		exported, err := o.export(ctx, pid)
		if err != nil {
			// 状态拿不到不阻断 drain：目录里可能本就没有活着的实例（幂等 drain 语义）。
			o.log.Warn("migrate: 导出战斗状态失败，按无状态 drain 继续",
				"pid", pid.String(), "from", fromNode, "err", err)
		} else {
			state = exported
			o.remember(pid, state)
		}
	}
	if err := o.inner.DrainPIDOn(ctx, pid, fromNode); err != nil {
		o.forget(pid)
		if state != nil {
			o.resumeOldOwner(ctx, pid)
		}
		return err
	}
	// drain 成功 = 旧属主已 Release 目录归属：立刻安装隔离窗口，盖住到新属主 Claim
	// 之间的空窗（否则旧节点会懒激活同一 PID，与新属主形成双 actor）。
	o.quarantineOldOwner(pid, fromNode)
	return nil
}

// ActivatePIDOn 实现 rollout.ClusterOps：先把状态预置到目标节点，再走框架激活。
// 预置必须在 Claim 之前——新属主的 OnStart 要在建立会话 actor 之前拿到状态。
func (o *Ops) ActivatePIDOn(ctx context.Context, pid types.PID, preferNode string) (rollout.OwnerEpoch, error) {
	target := preferNode
	if target == "" {
		target = o.node
	}
	st := o.peek(pid)
	if st == nil {
		return o.inner.ActivatePIDOn(ctx, pid, preferNode)
	}
	if err := o.stage(ctx, pid, target, st); err != nil {
		return rollout.OwnerEpoch{}, err
	}
	oe, err := o.inner.ActivatePIDOn(ctx, pid, preferNode)
	if err != nil {
		// 状态保留到成功为止：编排器对激活失败的补偿重试（rollout 的 retryFailedActivates）
		// 会再次走到这里，届时按「已预置」跳过重复投递，不会把状态丢在半路。
		return rollout.OwnerEpoch{}, err
	}
	if oe.Epoch < st.state.GetSourceEpoch() {
		// 只拒绝严格更旧的代际：目录 Claim 在归属被释放后从 1 重新起算，
		// 正常迁移（drain → release → claim）后的 epoch 与导出时相等是合法结果。
		o.forget(pid)
		return rollout.OwnerEpoch{}, fmt.Errorf("%w: pid=%s source=%d got=%d",
			ErrStaleEpoch, pid.String(), st.state.GetSourceEpoch(), oe.Epoch)
	}
	o.forget(pid)
	return oe, nil
}

// ActivatePID 实现 rollout.ClusterOps：本节点激活（rebalance 单机路径）。
func (o *Ops) ActivatePID(ctx context.Context, pid types.PID) (rollout.OwnerEpoch, error) {
	return o.ActivatePIDOn(ctx, pid, "")
}

// VerifyPID 实现 rollout.ClusterOps：原样透传。
func (o *Ops) VerifyPID(ctx context.Context, pid types.PID, wantEpoch uint64) error {
	return o.inner.VerifyPID(ctx, pid, wantEpoch)
}

// VerifyPIDOn 实现 rollout.ClusterOps：原样透传。
func (o *Ops) VerifyPIDOn(ctx context.Context, pid types.PID, wantNode string, wantEpoch uint64) error {
	return o.inner.VerifyPIDOn(ctx, pid, wantNode, wantEpoch)
}

// LookupSnapshot 实现 rollout.ClusterOps：原样透传（编排器据此读当前归属）。
func (o *Ops) LookupSnapshot(ctx context.Context, pids []types.PID) ([]rollout.OwnerEpoch, error) {
	return o.inner.LookupSnapshot(ctx, pids)
}

// Migrate 执行一次「就地触发」的迁移闭环：目标已知时先预置状态，再 drain → activate → verify。
// 它是管理面（MigrateBattleRequest）的落地实现，也是 rebalance 编排路径之外的最小驱动。
func (o *Ops) Migrate(ctx context.Context, battleID, targetNode string) (Outcome, error) {
	pid, err := types.NewPID(consts.ActorTypeBattle, battleID)
	if err != nil {
		return Outcome{}, err
	}
	snapshot, err := o.inner.LookupSnapshot(ctx, []types.PID{pid})
	if err != nil || len(snapshot) == 0 {
		return Outcome{}, fmt.Errorf("migrate: 查询战斗 %s 的目录归属失败: %w", battleID, err)
	}
	from := snapshot[0].OwnerNode
	if from == "" {
		return Outcome{}, fmt.Errorf("migrate: 战斗 %s 当前无属主节点", battleID)
	}
	if targetNode == "" {
		targetNode, err = o.chooser.Choose(ctx, pid, from)
		if err != nil {
			return Outcome{}, fmt.Errorf("migrate: 自动选点失败: %w", err)
		}
	}
	if targetNode == from {
		return Outcome{}, fmt.Errorf("migrate: 目标节点 %s 已是战斗 %s 的属主", targetNode, battleID)
	}
	if err := o.preStage(ctx, pid, targetNode); err != nil {
		return Outcome{}, err
	}
	if err := o.DrainPIDOn(ctx, pid, from); err != nil {
		return Outcome{}, fmt.Errorf("migrate: drain %s 失败: %w", from, err)
	}
	oe, err := o.ActivatePIDOn(ctx, pid, targetNode)
	if err != nil {
		return Outcome{}, fmt.Errorf("migrate: 在 %s 激活失败: %w", targetNode, err)
	}
	if err := o.VerifyPIDOn(ctx, pid, targetNode, oe.Epoch); err != nil {
		return Outcome{}, fmt.Errorf("migrate: 校验 %s 归属失败: %w", targetNode, err)
	}
	return Outcome{FromNode: from, ToNode: targetNode, Epoch: oe.Epoch}, nil
}

// preStage 在 drain 之前把状态预置到目标节点（缩短「归属空窗」：drain 与 activate 之间
// 不再夹一次跨节点投递）。预置失败即中止，不进入 drain。
func (o *Ops) preStage(ctx context.Context, pid types.PID, target string) error {
	state, err := o.export(ctx, pid)
	if err != nil {
		return fmt.Errorf("migrate: 导出状态失败: %w", err)
	}
	if err := o.deliver(ctx, target, state); err != nil {
		return err
	}
	o.rememberStaged(pid, state, target)
	return nil
}

// export 向旧属主索要迁移状态：旧属主据此进入迁移窗口（暂停掉线计时 + 暂停会话）。
func (o *Ops) export(ctx context.Context, pid types.PID) (*battlev1.BattleMigrationState, error) {
	reply, err := o.port.Ask(ctx, pid, &battlev1.PrepareBattleMigrationRequest{BattleId: pid.UID()})
	if err != nil {
		return nil, err
	}
	return decodeReply(reply, func() *battlev1.BattleMigrationState { return &battlev1.BattleMigrationState{} })
}

// decodeReply 归一 Ask 回执：同节点直投得到具体消息，跨节点得到线格式 []byte，
// 两种形态都要认（框架的跨节点 Ask 回执保持序列化字节，由调用方按类型解码）。
func decodeReply[T proto.Message](raw any, build func() T) (T, error) {
	var zero T
	if msg, ok := raw.(T); ok {
		return msg, nil
	}
	payload, ok := raw.([]byte)
	if !ok {
		return zero, fmt.Errorf("%w: 回执类型 %T", ErrBadState, raw)
	}
	msg := build()
	if err := proto.Unmarshal(payload, msg); err != nil {
		return zero, fmt.Errorf("%w: 解码回执失败: %v", ErrBadState, err)
	}
	return msg, nil
}

// resumeOldOwner 让旧属主退出迁移窗口（仅在 drain 失败、迁移中止时调用）。
// 走 Ask 而非 Tell：恢复入口在战斗 actor 侧注册为请求式本地路由；回执内容不用，
// 失败只记日志——不能因为一次恢复通知失败掩盖 drain 的真实错误。
func (o *Ops) resumeOldOwner(ctx context.Context, pid types.PID) {
	if _, err := o.port.Ask(ctx, pid, &battlev1.ResumeBattleMigrationRequest{BattleId: pid.UID()}); err != nil {
		o.log.Warn("migrate: 通知旧属主退出迁移窗口失败", "pid", pid.String(), "err", err)
	}
}

// stage 预置状态到目标节点（Activate 路径调用；已预置过则跳过）。
func (o *Ops) stage(ctx context.Context, pid types.PID, target string, st *staged) error {
	if st.stagedOn == target {
		return nil
	}
	return o.deliver(ctx, target, st.state)
}

// deliver 把状态投递到目标节点的本机收件箱。
func (o *Ops) deliver(ctx context.Context, target string, state *battlev1.BattleMigrationState) error {
	inbox, err := InboxPID(target)
	if err != nil {
		return fmt.Errorf("migrate: 目标节点 %q 无法组成收件箱 PID: %w", target, err)
	}
	if err := o.port.Tell(ctx, inbox, &battlev1.StageBattleMigrationRequest{State: state}); err != nil {
		return fmt.Errorf("migrate: 预置状态到 %s 失败: %w", target, err)
	}
	return nil
}

// remember 记录待搬运状态（drain 之后、activate 之前）。
func (o *Ops) remember(pid types.PID, state *battlev1.BattleMigrationState) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending[pid.String()] = &staged{state: state}
}

// rememberStaged 记录「已预置」状态（预置路径先做完投递再记录，Activate 时跳过重复投递）。
func (o *Ops) rememberStaged(pid types.PID, state *battlev1.BattleMigrationState, target string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending[pid.String()] = &staged{state: state, stagedOn: target}
}

// peek 读取待搬运状态（不移除：只有搬进新属主成功后才 forget，
// 让编排器的激活重试仍能拿到同一份状态）。
func (o *Ops) peek(pid types.PID) *staged {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pending[pid.String()]
}

// pendingState 返回待搬运状态（没有则 nil）。
func (o *Ops) pendingState(pid types.PID) *battlev1.BattleMigrationState {
	o.mu.Lock()
	defer o.mu.Unlock()
	if st, ok := o.pending[pid.String()]; ok {
		return st.state
	}
	return nil
}

// forget 丢弃待搬运状态（失败路径）。
func (o *Ops) forget(pid types.PID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.pending, pid.String())
}
