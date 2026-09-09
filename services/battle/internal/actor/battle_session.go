// 会话域业务方法：Create/Join/Reconnect/GetState（实现 BattleActorServer 接口的
// 会话部分）。管理一局战斗的玩家进出、状态查询与补帧；帧输入/结算见 battle_frame.go。
package actor

import (
	"fmt"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/lockstep"
)

// Create 实现 battlev1.BattleActorServer：开局登记参战玩家（matcher 成局后调用）。
func (b *BattleActor) Create(ctx core.ActorContext, req *battlev1.CreateBattleRequest) (*battlev1.CreateBattleReply, error) {
	b.matchID = req.GetMatchId()
	for _, p := range req.GetPlayerIds() {
		b.players[p] = struct{}{}
	}
	return &battlev1.CreateBattleReply{BattleId: b.battleID}, nil
}

// Join 实现 battlev1.BattleActorServer：玩家加入 + lockstep JoinSession + 快照回执。
func (b *BattleActor) Join(ctx core.ActorContext, req *battlev1.JoinBattleReq) (*battlev1.JoinBattleReply, error) {
	playerID := req.GetPlayerId()
	if _, ok := b.players[playerID]; !ok {
		// 错误语义上移到产生点：gateway 直接透传，对外 reason/code 与现状一致。
		return nil, errorv1.ErrBattleNotFound("玩家不在该战斗参战名单")
	}
	// lockstep 成员加入（幂等）。
	if err := b.rt.Tell(ctx.Context(), b.sessionPID, lockstep.JoinSession{Player: lockstep.PlayerID(playerID)}); err != nil {
		return nil, fmt.Errorf("actor: 加入会话失败: %w", err)
	}
	reconnect, err := b.askReconnect(ctx, 0)
	if err != nil {
		return nil, err
	}
	return &battlev1.JoinBattleReply{
		Meta:         sessionMeta(b.battleID, b.cfg.TickInterval),
		CurrentFrame: reconnect.GetCurrentFrame(),
		Snapshot:     reconnect.GetSnapshot(),
	}, nil
}

// Reconnect 实现 battlev1.BattleActorServer：按参战名单复核玩家后回执补帧。
func (b *BattleActor) Reconnect(ctx core.ActorContext, req *battlev1.ReconnectReq) (*battlev1.ReconnectReply, error) {
	if _, ok := b.players[req.GetPlayerId()]; !ok {
		return nil, errorv1.ErrInvalidToken("补帧被拒绝：不在参战名单")
	}
	reconnect, err := b.askReconnect(ctx, req.GetLastSeenFrame())
	if err != nil {
		return nil, err
	}
	return &battlev1.ReconnectReply{
		CurrentFrame: reconnect.GetCurrentFrame(),
		Snapshot:     reconnect.GetSnapshot(),
		Missed:       reconnect.GetMissed(),
	}, nil
}

// GetState 实现 battlev1.BattleActorServer：状态查询（grpc 管理接口转发）。
func (b *BattleActor) GetState(ctx core.ActorContext, _ *battlev1.GetStateReq) (*battlev1.GetStateReply, error) {
	// 同节点会话：lockstep 消息为对象直传（非跨节点 proto 信封）。
	reply, err := b.rt.Ask(ctx.Context(), b.sessionPID, lockstep.QueryStats{})
	if err != nil {
		return nil, fmt.Errorf("actor: 状态查询失败: %w", err)
	}
	stats, ok := reply.(lockstep.SessionStats)
	if !ok {
		return nil, fmt.Errorf("actor: 状态回执类型 %T 不符", reply)
	}
	return &battlev1.GetStateReply{
		State:        stateName(b.settled),
		CurrentFrame: uint64(stats.CurrentFrame),
		PlayerCount:  uint32(stats.PlayerCount),
	}, nil
}

// askReconnect 向 lockstep 会话查询补帧信息（同节点对象直传）。
func (b *BattleActor) askReconnect(ctx core.ActorContext, lastSeen uint64) (*battlev1.ReconnectReply, error) {
	raw, err := b.rt.Ask(ctx.Context(), b.sessionPID, lockstep.ReconnectRequest{
		LastSeenFrame: lockstep.FrameID(lastSeen),
	})
	if err != nil {
		return nil, fmt.Errorf("actor: 补帧查询失败: %w", err)
	}
	reply, ok := raw.(lockstep.ReconnectResponse)
	if !ok {
		return nil, fmt.Errorf("actor: 补帧回执类型 %T 不符", raw)
	}
	out := &battlev1.ReconnectReply{CurrentFrame: uint64(reply.CurrentFrame)}
	if reply.Snapshot != nil {
		out.Snapshot = &locksteppb.SnapshotMeta{
			FrameId:    uint64(reply.Snapshot.Frame),
			StateHash:  hashBytes(reply.Snapshot.Hash),
			SizeBytes:  uint64(len(reply.Snapshot.State)),
			StorageKey: snapshotKey(b.battleID),
		}
	}
	for _, ins := range reply.MissedInputs {
		group := &locksteppb.FrameInputs{}
		for _, in := range ins {
			group.Inputs = append(group.Inputs, toPBInput(in))
		}
		out.Missed = append(out.Missed, group)
	}
	return out, nil
}
