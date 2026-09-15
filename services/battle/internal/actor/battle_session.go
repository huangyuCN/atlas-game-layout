// 会话域业务方法：Create/JoinBattle/SyncFrames/GetState（实现 BattleServiceServer 接口的
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

// Create 实现 battlev1.BattleServiceServer：开局登记参战玩家（matcher 成局后调用）。
func (b *BattleActor) Create(ctx core.ActorContext, req *battlev1.CreateBattleRequest) (*battlev1.CreateBattleReply, error) {
	b.matchID = req.GetMatchId()
	for _, p := range req.GetPlayerIds() {
		b.players[p] = struct{}{}
	}
	return &battlev1.CreateBattleReply{BattleId: b.battleID}, nil
}

// JoinBattle 实现 battlev1.BattleServiceServer：玩家加入 + lockstep JoinSession + 快照回执。
// 发起者身份由投递 sender 注入（消息体无身份字段）：不在参战名单则拒绝。
func (b *BattleActor) JoinBattle(ctx core.ActorContext, req *battlev1.JoinBattleReq) (*battlev1.JoinBattleReply, error) {
	playerID, err := b.senderPlayer(ctx, "加入战斗")
	if err != nil {
		return nil, err
	}
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

// SyncFrames 实现 battlev1.BattleServiceServer：按参战名单复核发起者后回执补帧
// （断线重连按帧区间拉取缺失帧；发起者由 sender 注入）。
func (b *BattleActor) SyncFrames(ctx core.ActorContext, req *battlev1.SyncFramesReq) (*battlev1.SyncFramesReply, error) {
	playerID, err := b.senderPlayer(ctx, "补帧")
	if err != nil {
		return nil, err
	}
	if _, ok := b.players[playerID]; !ok {
		return nil, errorv1.ErrInvalidToken("补帧被拒绝：不在参战名单")
	}
	reconnect, err := b.askReconnect(ctx, req.GetLastSeenFrame())
	if err != nil {
		return nil, err
	}
	return &battlev1.SyncFramesReply{
		CurrentFrame: reconnect.GetCurrentFrame(),
		Snapshot:     reconnect.GetSnapshot(),
		Missed:       reconnect.GetMissed(),
	}, nil
}

// GetState 实现 battlev1.BattleServiceServer：状态查询（grpc 管理接口转发）。
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
func (b *BattleActor) askReconnect(ctx core.ActorContext, lastSeen uint64) (*battlev1.SyncFramesReply, error) {
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
	out := &battlev1.SyncFramesReply{CurrentFrame: uint64(reply.CurrentFrame)}
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

// senderPlayer 返回投递 sender 承载的发起者玩家 ID（无 sender 视为协议错误）。
// 战斗域身份统一从 sender 取（消息体无身份字段），caller 传入用途名供错误定位。
func (b *BattleActor) senderPlayer(ctx core.ActorContext, action string) (string, error) {
	sender, ok := ctx.Sender()
	if !ok || sender.UID() == "" {
		return "", errorv1.ErrInvalidParams("%s失败：缺少发起者身份", action)
	}
	return sender.UID(), nil
}
