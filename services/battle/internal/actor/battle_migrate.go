// 战斗 actor 的迁移侧（规格 §8 状态搬运 + §9.6 迁移期不误判掉线）。
//
// 一次迁移在本文件里的三处落点：
//  1. **旧属主导出**：PrepareBattleMigration 进入迁移窗口（暂停掉线计时 + 暂停会话帧推进）
//     并导出「lockstep 会话快照 + 参战名单 + 帧状态」；迁移中止时 ResumeBattleMigration 退出窗口。
//  2. **新属主恢复**：OnStart 在**建立会话 actor 之前**从本机收件箱取状态，写进会话存储，
//     会话 actor 的 OnStart 据此把模拟器恢复到同一帧；名单/帧状态一并还原。
//  3. **栅栏**：状态携带导出时的目录 epoch，新属主只在自身 epoch 更大时认它
//     （目录 Claim 单调抬 epoch；陈旧状态一律丢弃，不产生状态回退或双活）。
//
// 迁移窗口的计时开关复用批次 6 的接缝实现（enterMigrationWindow / exitMigrationWindow），
// 因此本地消息 MigrationPause 与迁移 op 两条入口语义完全一致。

package actor

import (
	"fmt"
	"sort"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/lockstep"
	"google.golang.org/protobuf/types/known/emptypb"
)

// migrationPauseReason 是迁移窗口内暂停 lockstep 会话的原因（进日志与帧状态，便于排障）。
const migrationPauseReason = "migration"

// onPrepareMigration 实现本地 Ask：进入迁移窗口并导出可搬运状态（规格 §8）。
// 导出失败即退出窗口——不把旧属主留在「暂停计时且帧不推进」的半死状态。
func (b *BattleActor) onPrepareMigration(ctx core.ActorContext, req *battlev1.PrepareBattleMigrationRequest) (any, error) {
	if id := req.GetBattleId(); id != "" && id != b.battleID {
		return nil, fmt.Errorf("actor: 迁移请求的战斗 %q 与实例 %q 不符", id, b.battleID)
	}
	b.enterMigrationWindow(ctx)
	state, err := b.exportState(ctx)
	if err != nil {
		b.exitMigrationWindow(ctx)
		return nil, err
	}
	return state, nil
}

// onResumeMigration 实现本地 Ask：退出迁移窗口（迁移中止路径）。
// 已结算的局不需要恢复计时，exitMigrationWindow 对空打点集合是 no-op。
func (b *BattleActor) onResumeMigration(ctx core.ActorContext, req *battlev1.ResumeBattleMigrationRequest) (any, error) {
	if id := req.GetBattleId(); id != "" && id != b.battleID {
		return nil, fmt.Errorf("actor: 迁移恢复请求的战斗 %q 与实例 %q 不符", id, b.battleID)
	}
	b.exitMigrationWindow(ctx)
	return &emptypb.Empty{}, nil
}

// enterMigrationWindow 进入迁移窗口：窗口内断开不算掉线（规格 §9.6），
// 且会话暂停推进——导出与恢复之间帧号严格一致，客户端重连后不会看到帧回退。
func (b *BattleActor) enterMigrationWindow(ctx core.ActorContext) {
	b.migration = true
	b.holdOfflineTimers()
	_ = b.rt.Tell(ctx.Context(), b.sessionPID, lockstep.PauseSession{Reason: migrationPauseReason})
}

// exitMigrationWindow 退出迁移窗口：恢复帧推进，并给仍打点的玩家重新起算掉线计时。
func (b *BattleActor) exitMigrationWindow(ctx core.ActorContext) {
	b.migration = false
	b.rearmOfflineTimers(ctx)
	if !b.settled {
		_ = b.rt.Tell(ctx.Context(), b.sessionPID, lockstep.ResumeSession{})
	}
}

// exportState 导出可搬运状态：向会话 actor 要「当前帧 + 当前快照 + 帧输入日志」，
// 连同参战名单、最近广播帧号、打点集合与目录 epoch 一起打包（规格 §8 快照内容）。
func (b *BattleActor) exportState(ctx core.ActorContext) (*battlev1.BattleMigrationState, error) {
	raw, err := b.rt.Ask(ctx.Context(), b.sessionPID, lockstep.ReconnectRequest{})
	if err != nil {
		return nil, fmt.Errorf("actor: 导出会话状态失败: %w", err)
	}
	reply, ok := raw.(lockstep.ReconnectResponse)
	if !ok {
		return nil, fmt.Errorf("actor: 会话状态回执类型 %T 不符", raw)
	}
	state := &battlev1.BattleMigrationState{
		BattleId:      b.battleID,
		MatchId:       b.matchID,
		Players:       sortedKeys(b.players),
		LastFrame:     b.lastFrame,
		Settled:       b.settled,
		OfflineMarked: markedPlayers(b.offline),
		SourceEpoch:   ctx.Epoch(),
		SourceNode:    ctx.Node(),
	}
	if snap := reply.Snapshot; snap != nil {
		state.SnapshotFrame = uint64(snap.Frame)
		state.SnapshotState = snap.State
		state.SnapshotHash = snap.Hash
	} else {
		state.SnapshotFrame = uint64(reply.CurrentFrame)
	}
	for _, group := range reply.MissedInputs {
		for _, in := range group {
			state.Inputs = append(state.Inputs, toPBInput(in))
		}
	}
	return state, nil
}

// takeStagedState 从本机收件箱取待恢复状态（无收件箱或无该局状态时返回 nil, nil）。
// 收件箱未装配（ReasonNotFound）视为「本节点不做恢复」，不是错误。
func (b *BattleActor) takeStagedState(ctx core.ActorContext) (*battlev1.BattleMigrationState, error) {
	inbox, err := types.NewPID(consts.ActorTypeBattleMigrate, ctx.Node())
	if err != nil {
		return nil, fmt.Errorf("actor: 迁移收件箱 PID 非法: %w", err)
	}
	raw, err := b.rt.Ask(ctx.Context(), inbox, &battlev1.TakeBattleMigrationRequest{BattleId: b.battleID})
	if err != nil {
		if types.IsReason(err, types.ReasonNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("actor: 取迁移状态失败: %w", err)
	}
	reply, ok := raw.(*battlev1.TakeBattleMigrationReply)
	if !ok || !reply.GetFound() {
		return nil, nil
	}
	return reply.GetState(), nil
}

// restoreIfMigrated 在建立会话 actor 之前恢复迁移状态；无待恢复状态时 no-op
// （正常启动路径零差异）。恢复后立刻给打点玩家重新起算掉线窗口——迁移窗口在
// 新属主拉起这一刻即告结束（规格 §9.6）。
func (b *BattleActor) restoreIfMigrated(ctx core.ActorContext) error {
	state, err := b.takeStagedState(ctx)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if err := b.applyStagedState(ctx, state); err != nil {
		return err
	}
	b.restored = true
	if !b.settled {
		b.rearmOfflineTimers(ctx)
	}
	ctx.Logger().Info("battle: 已按迁移状态恢复",
		"battle", b.battleID, "node", ctx.Node(), "snapshot_frame", state.GetSnapshotFrame(),
		"players", len(state.GetPlayers()), "marked", len(state.GetOfflineMarked()))
	return nil
}

// applyStagedState 在建立会话 actor 之前把状态写进会话存储并还原战斗侧状态。
// 会话 actor 的 OnStart 会读 LatestSnapshot 恢复模拟器（帧号即 snapshot_frame），
// 因此这里的写入顺序是恢复正确性的前提，写失败即 fail-closed（启动失败，不静默丢局）。
func (b *BattleActor) applyStagedState(ctx core.ActorContext, st *battlev1.BattleMigrationState) error {
	if st.GetBattleId() != b.battleID {
		return fmt.Errorf("actor: 迁移状态归属 %q 与实例 %q 不符", st.GetBattleId(), b.battleID)
	}
	if st.GetSourceEpoch() > ctx.Epoch() {
		// 栅栏：只拒绝**严格更旧**的代际（陈旧状态恢复会造成状态回退）。
		// 不用「必须更大」：目录 Claim 在记录被 drain 释放后从 1 重新起算，
		// 正常迁移后的 epoch 可以与导出时相等（框架 rollout 的校验同样只看返回的 epoch）。
		return fmt.Errorf("actor: 迁移状态来自更旧的代际（source epoch %d > 本实例 %d），拒绝恢复",
			st.GetSourceEpoch(), ctx.Epoch())
	}
	if err := b.restoreSessionState(ctx, st); err != nil {
		return err
	}
	b.matchID = st.GetMatchId()
	b.lastFrame = st.GetLastFrame()
	// 已结算的局不会被迁移（actor 结算后自停）；但仍如实还原，避免状态被静默改写。
	b.settled = st.GetSettled()
	for _, p := range st.GetPlayers() {
		b.players[p] = struct{}{}
	}
	for _, p := range st.GetOfflineMarked() {
		b.offline[p] = nil // 只打点不计时：掉线窗口由迁移窗口延续，恢复后由 exitMigrationWindow 起算
	}
	return nil
}

// restoreSessionState 把快照与帧输入日志写进会话存储（会话 actor 建立之前调用）。
func (b *BattleActor) restoreSessionState(ctx core.ActorContext, st *battlev1.BattleMigrationState) error {
	if st.GetSnapshotFrame() > 0 || len(st.GetSnapshotState()) > 0 {
		snap := lockstep.Snapshot{
			Frame: lockstep.FrameID(st.GetSnapshotFrame()),
			State: st.GetSnapshotState(),
			Hash:  st.GetSnapshotHash(),
		}
		if err := b.storage.SaveSnapshot(ctx.Context(), b.battleID, snap); err != nil {
			return fmt.Errorf("actor: 恢复会话快照失败: %w", err)
		}
	}
	byFrame := map[lockstep.FrameID][]lockstep.Input{}
	for _, in := range st.GetInputs() {
		frame := lockstep.FrameID(in.GetFrameId())
		byFrame[frame] = append(byFrame[frame], lockstep.Input{
			Frame: frame, Player: lockstep.PlayerID(in.GetPlayerId()), Payload: in.GetPayload(),
		})
	}
	for _, frame := range sortedFrames(byFrame) {
		if err := b.storage.AppendInputs(ctx.Context(), b.battleID, frame, byFrame[frame]); err != nil {
			return fmt.Errorf("actor: 恢复帧输入日志失败（帧 %d）: %w", frame, err)
		}
	}
	return nil
}

// rejoinSession 把参战名单重新登记进（迁移后重建的）lockstep 会话。
// 会话的 PlayerInput 校验依赖它自己的成员表，漏登记会让迁移后的帧输入全部被拒。
func (b *BattleActor) rejoinSession(ctx core.ActorContext) error {
	for _, player := range sortedKeys(b.players) {
		if err := b.rt.Tell(ctx.Context(), b.sessionPID,
			lockstep.JoinSession{Player: lockstep.PlayerID(player)}); err != nil {
			return fmt.Errorf("actor: 恢复会话成员 %s 失败: %w", player, err)
		}
	}
	return nil
}

// sortedKeys 返回集合的字典序切片（导出状态可复现，便于断言与比对）。
func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// markedPlayers 返回已打点掉线的玩家（字典序）。
func markedPlayers(offline map[string]core.CancelFunc) []string {
	out := make([]string, 0, len(offline))
	for k := range offline {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sortedFrames 返回帧号升序切片（按帧落盘，顺序稳定）。
func sortedFrames(byFrame map[lockstep.FrameID][]lockstep.Input) []lockstep.FrameID {
	out := make([]lockstep.FrameID, 0, len(byFrame))
	for f := range byFrame {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
