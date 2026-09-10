// 帧同步/结算域：FrameInput（输入转发）+ onFrameResult/checkSettle（帧广播处理与
// 结算）+ 帧协议辅助（主题/键/元信息编码）。实现 BattleActorServer 接口的帧部分。

package actor

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/lockstep"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/simulator"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/models"
)

// FrameInput 实现 battlev1.BattleActorServer：帧输入转发 lockstep 会话
// （非参战玩家输入静默丢弃，与现状一致）。
func (b *BattleActor) FrameInput(ctx core.ActorContext, req *battlev1.FrameInputReq) (*battlev1.FrameInputReply, error) {
	if _, ok := b.players[req.GetPlayerId()]; !ok {
		return &battlev1.FrameInputReply{}, nil
	}
	in := req.GetInput()
	err := b.rt.Tell(ctx.Context(), b.sessionPID, lockstep.PlayerInput{
		Player:  lockstep.PlayerID(req.GetPlayerId()),
		Frame:   lockstep.FrameID(in.GetFrameId()),
		Payload: in.GetPayload(),
	})
	if err != nil {
		return nil, fmt.Errorf("actor: 帧输入失败: %w", err)
	}
	return &battlev1.FrameInputReply{}, nil
}

// onFrameResult 帧广播到达（本地类型路由注册项）：下发客户端 + 胜负检查与结算。
func (b *BattleActor) onFrameResult(ctx core.ActorContext, result lockstep.FrameResult) error {
	// 帧广播下发全部参战玩家（v1 链路：nats → gateway → 客户端）。
	frame := &locksteppb.LockstepFrame{
		FrameId:    uint64(result.Frame),
		ServerTime: timestamppb.Now(), // 广播时间戳：客户端对时与压测延迟度量
		Snapshot:   snapshotMeta(result.Snapshot),
	}
	for _, in := range result.Inputs {
		frame.Inputs = append(frame.Inputs, toPBInput(in))
	}
	for player := range b.players {
		_ = b.notifier.PublishFrame(ctx.Context(), player, b.battleID, frame)
	}
	// 快照携带胜负状态：分出胜负即结算。
	if result.Snapshot != nil && !b.settled {
		b.checkSettle(ctx, result)
	}
	return nil
}

// checkSettle 从快照解出胜负：有胜者即结算；
// 帧数耗尽即使平局也结算（防无限对局泄漏帧广播与 actor 实例）。
func (b *BattleActor) checkSettle(ctx core.ActorContext, result lockstep.FrameResult) {
	var st simulator.State
	if err := json.Unmarshal(result.Snapshot.State, &st); err != nil {
		return
	}
	if st.Winner == "" && uint64(result.Frame) < b.cfg.MaxFrames {
		return
	}
	b.settled = true
	res := &models.BattleResult{
		BattleID:    b.battleID,
		MatchID:     b.matchID,
		TotalFrames: uint64(result.Frame),
		SettledAt:   time.Now().UnixMilli(),
	}
	for player := range b.players {
		res.Players = append(res.Players, models.PlayerResult{
			PlayerID: player,
			Win:      player == st.Winner,
			Score:    st.Scores[player],
		})
	}
	if err := b.resultRepo.Save(ctx.Context(), res); err != nil {
		ctx.Logger().Error("battle: 结算落库失败", "battle", b.battleID, "err", err)
	}
	ev := &battlev1.BattleSettledEvent{BattleId: b.battleID, MatchId: b.matchID}
	for _, p := range res.Players {
		ev.Players = append(ev.Players, &battlev1.PlayerResult{PlayerId: p.PlayerID, Win: p.Win, Score: p.Score})
	}
	_ = b.publisher.PublishSettled(ctx.Context(), ev)
	// 战斗结束通知下发全部参战玩家（含胜者），随后自停。
	for player := range b.players {
		_ = b.notifier.PublishEnd(ctx.Context(), player, b.battleID, st.Winner)
	}
	ctx.Stop(types.ExitNormal())
}

// stateName 映射战斗状态描述。
func stateName(settled bool) string {
	if settled {
		return "settled"
	}
	return "running"
}

// frameTopic 是 lockstep 帧广播主题。
func frameTopic(battleID string) string { return "frame." + battleID }

// snapshotKey 是快照存储键（内存存储仅标识用）。
func snapshotKey(battleID string) string { return "snap/" + battleID }

// snapshotMeta 组装快照元信息。
func snapshotMeta(snap *lockstep.Snapshot) *locksteppb.SnapshotMeta {
	if snap == nil {
		return nil
	}
	return &locksteppb.SnapshotMeta{
		FrameId:   uint64(snap.Frame),
		StateHash: hashBytes(snap.Hash),
		SizeBytes: uint64(len(snap.State)),
	}
}

// hashBytes 把状态哈希编码为 8 字节（大端）。
func hashBytes(h uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, h)
	return buf
}

// toPBInput 转换 lockstep 输入为协议帧输入（帧广播与补帧共用）。
func toPBInput(in lockstep.Input) *locksteppb.LockstepInput {
	return &locksteppb.LockstepInput{
		FrameId:  uint64(in.Frame),
		PlayerId: string(in.Player),
		Payload:  in.Payload,
	}
}

// sessionMeta 组装会话元信息。
func sessionMeta(battleID string, tickNanos int64) *locksteppb.SessionMeta {
	return &locksteppb.SessionMeta{
		SessionId:        battleID,
		MaxPlayers:       2,
		TickMillis:       uint32(tickNanos / 1e6),
		InputDelayFrames: 0,
		Mode:             locksteppb.LockstepMode_LOCKSTEP_MODE_SERVER_AUTHORITATIVE,
	}
}
