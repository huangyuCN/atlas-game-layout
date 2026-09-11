package server

import (
	"context"
	"fmt"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// mockActorRuntime 模拟 game PlayerActor 与 battle 战斗 actor（gateway 单测装置）：
// 消息为具体对象直传（生成的桩 switch 直接命中），回执直接返回对象（同节点形态）；
// 注册/登录恒成功（player_id 取 PID uid）；加入战斗按 joinOK 裁决；帧输入/补帧记录投递。
type mockActorRuntime struct {
	logoutMsgs    []*gamev1.LogoutActorMsg             // 登出投递记录
	matchEnters   []*gamev1.EnterMatchQueueActorReq    // 入队投递记录
	matchCanceled bool                                 // 取消投递记录
	joinOK        bool                                 // 加入战斗裁决（默认放行）
	frameInputs   map[string][]*battlev1.FrameInputReq // battleID → 帧输入序列
	reconnects    map[string][]*battlev1.ReconnectReq  // battleID → 补帧请求序列
}

func newMockActorRuntime() *mockActorRuntime {
	return &mockActorRuntime{
		joinOK:      true,
		frameInputs: make(map[string][]*battlev1.FrameInputReq),
		reconnects:  make(map[string][]*battlev1.ReconnectReq),
	}
}

// Tell 实现 actorclient.Runtime（记录登出投递）。
func (m *mockActorRuntime) Tell(_ context.Context, _ types.PID, msg any) error {
	if lg, ok := msg.(*gamev1.LogoutActorMsg); ok {
		m.logoutMsgs = append(m.logoutMsgs, lg)
	}
	return nil
}

// Ask 实现 actorclient.Runtime：按具体消息类型返回对象回执（同节点直传形态）。
func (m *mockActorRuntime) Ask(_ context.Context, pid types.PID, req any, _ ...core.SendOption) (any, error) {
	switch r := req.(type) {
	case *gamev1.RegisterActorReq:
		return &gamev1.RegisterActorReply{
			PlayerId: pid.UID(),
			Player:   &commonv1.PlayerSummary{PlayerId: pid.UID(), Nickname: r.GetAccount()},
		}, nil
	case *gamev1.LoginActorReq:
		return &gamev1.LoginActorReply{
			Player: &commonv1.PlayerSummary{PlayerId: pid.UID(), Nickname: r.GetPlayerId()},
		}, nil
	case *battlev1.JoinBattleReq:
		if !m.joinOK {
			return nil, errorv1.ErrBattleNotFound("玩家不在该战斗参战名单")
		}
		return &battlev1.JoinBattleReply{
			Meta: &locksteppb.SessionMeta{
				SessionId:        pid.UID(),
				MaxPlayers:       2,
				TickMillis:       100,
				InputDelayFrames: 0,
				Mode:             locksteppb.LockstepMode_LOCKSTEP_MODE_SERVER_AUTHORITATIVE,
			},
			CurrentFrame: 3,
			Snapshot: &locksteppb.SnapshotMeta{
				FrameId:   2,
				StateHash: []byte("hash"),
				SizeBytes: 8,
			},
		}, nil
	case *battlev1.FrameInputReq:
		m.frameInputs[pid.UID()] = append(m.frameInputs[pid.UID()], r)
		return &battlev1.FrameInputReply{}, nil
	case *battlev1.ReconnectReq:
		m.reconnects[pid.UID()] = append(m.reconnects[pid.UID()], r)
		return &battlev1.ReconnectReply{
			CurrentFrame: 5,
			Missed: []*locksteppb.FrameInputs{
				{FrameId: 4, Inputs: []*locksteppb.LockstepInput{
					{FrameId: 4, PlayerId: "p-1", Payload: []byte("x")},
				}},
			},
		}, nil
	case *gamev1.EnterMatchQueueActorReq:
		m.matchEnters = append(m.matchEnters, r)
		return &gamev1.EnterMatchQueueActorReply{}, nil
	case *gamev1.CancelMatchActorReq:
		m.matchCanceled = true
		return &gamev1.CancelMatchActorReply{Canceled: true}, nil
	case *gamev1.GetMatchStatusActorReq:
		return &gamev1.MatchStatusActorReply{
			State: matcherv1.MatchState_MATCH_STATE_WAITING, TicketId: "t-1", MatchId: "m-1",
		}, nil
	case *battlev1.CreateBattleRequest:
		return &battlev1.CreateBattleReply{BattleId: pid.UID()}, nil
	default:
		return nil, fmt.Errorf("mock: 未知请求类型 %T", req)
	}
}
