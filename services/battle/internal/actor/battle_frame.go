// 帧同步/结算域：SendFrameInput（输入转发）+ onFrameResult/checkSettle（帧广播处理与
// 结算）+ 帧协议辅助（主题/键/元信息编码）。实现 battlev1actor.BattleService 接口的帧部分。

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

// SendFrameInput 实现 battlev1actor.BattleService：帧输入转发 lockstep 会话。
// 输入者身份由投递 sender 注入（消息体无身份字段）；非参战玩家输入静默丢弃
// （与现状一致）。
func (b *BattleActor) SendFrameInput(ctx core.ActorContext, req *battlev1.FrameInputReq) error {
	playerID, err := b.senderPlayer(ctx, "帧输入")
	if err != nil {
		return err
	}
	if _, ok := b.players[playerID]; !ok {
		return nil
	}
	in := req.GetInput()
	err = b.rt.Tell(ctx.Context(), b.sessionPID, lockstep.PlayerInput{
		Player:  lockstep.PlayerID(playerID),
		Frame:   lockstep.FrameID(in.GetFrameId()),
		Payload: in.GetPayload(),
	})
	if err != nil {
		return fmt.Errorf("actor: 帧输入失败: %w", err)
	}
	return nil
}

// onFrameResult 帧广播到达（本地类型路由注册项）：直连下发客户端 + 胜负检查与结算。
func (b *BattleActor) onFrameResult(ctx core.ActorContext, result lockstep.FrameResult) error {
	b.lastFrame = uint64(result.Frame) // 掉线判负的结算复用同一帧号口径
	// 帧广播下发全部参战玩家（阶段 3 批次 5 起只走直连：帧引擎推送原语 → 客户端）。
	frame := &locksteppb.LockstepFrame{
		FrameId:    uint64(result.Frame),
		ServerTime: timestamppb.Now(), // 广播时间戳：客户端对时与压测延迟度量
		Snapshot:   snapshotMeta(result.Snapshot),
	}
	for _, in := range result.Inputs {
		frame.Inputs = append(frame.Inputs, toPBInput(in))
	}
	for player := range b.players {
		_ = b.pusher.PublishFrame(ctx.Context(), player, b.battleID, frame)
	}
	// 快照携带胜负状态：分出胜负即结算。
	if result.Snapshot != nil && !b.settled {
		b.checkSettle(ctx, result)
	}
	return nil
}

// settleOutcome 是一次结算的胜负与帧数（快照判定与掉线判负共用同一结算路径）。
type settleOutcome struct {
	// Winner 是胜者玩家 ID（空 = 平局）。
	Winner string
	// Frames 是结算时的总帧数。
	Frames uint64
	// Scores 是各玩家赛道得分（掉线判负不产生赛道得分，留空即全 0）。
	Scores map[string]int32
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
	b.settle(ctx, settleOutcome{Winner: st.Winner, Frames: uint64(result.Frame), Scores: st.Scores})
}

// settle 走既有结算路径：落库 + 结算事件 + 结束广播（直连）+ 关闭本局直连 + 自停
// （规格 §9.3 判胜与 §9.8 连接回收共用）。名单取当前参战名单——掉线判负时仍含掉线者
// （记 Win=false），故 eliminate 在移出名单前调用本方法；重复调用幂等。
func (b *BattleActor) settle(ctx core.ActorContext, out settleOutcome) {
	if b.settled {
		return
	}
	b.settled = true
	res := &models.BattleResult{
		BattleID:    b.battleID,
		MatchID:     b.matchID,
		TotalFrames: out.Frames,
		SettledAt:   time.Now().UnixMilli(),
	}
	for player := range b.players {
		res.Players = append(res.Players, models.PlayerResult{
			PlayerID: player,
			Win:      player == out.Winner,
			Score:    out.Scores[player],
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
	// 战斗结束通知直连下发全部参战玩家（含胜者），随后关闭本局直连并自停。
	for player := range b.players {
		_ = b.pusher.PublishEnd(ctx.Context(), player, b.battleID, out.Winner)
	}
	b.pusher.CloseBattle(b.battleID) // 规格 §9.8：结算后回收直连，禁止悬挂连接
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
func sessionMeta(battleID string, tickNanos int64, maxPlayers int) *locksteppb.SessionMeta {
	return &locksteppb.SessionMeta{
		SessionId:        battleID,
		MaxPlayers:       uint32(maxPlayers),
		TickMillis:       uint32(tickNanos / 1e6),
		InputDelayFrames: 0,
		Mode:             locksteppb.LockstepMode_LOCKSTEP_MODE_SERVER_AUTHORITATIVE,
	}
}
