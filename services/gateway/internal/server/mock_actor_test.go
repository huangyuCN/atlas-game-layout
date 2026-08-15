package server

import (
	"context"
	"fmt"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"google.golang.org/protobuf/proto"
)

// mockActorRuntime 模拟 game PlayerActor 与 battle 战斗 actor（gateway 单测装置）：
// 注册/登录恒成功回执（player_id 取 PID uid）；加入战斗按 joinOK 裁决；帧输入/补帧记录投递。
type mockActorRuntime struct {
	tells       []*gamev1.PlayerActorMsg
	joinOK      bool                                 // 加入战斗裁决（默认放行）
	frameInputs map[string][]*battlev1.FrameInputReq // battleID → 帧输入序列
	reconnects  map[string][]*battlev1.ReconnectReq  // battleID → 补帧请求序列
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
	if env, ok := msg.(*gamev1.PlayerActorMsg); ok {
		m.tells = append(m.tells, env)
	}
	return nil
}

// Ask 实现 actorclient.Runtime：按信封类型返回序列化回执（跨节点形态）。
func (m *mockActorRuntime) Ask(_ context.Context, pid types.PID, req any) (any, error) {
	switch env := req.(type) {
	case *gamev1.PlayerActorMsg:
		return m.askPlayer(pid, env)
	case *battlev1.BattleActorMsg:
		return m.askBattle(pid, env)
	default:
		return nil, fmt.Errorf("mock: 未知请求类型 %T", req)
	}
}

// askPlayer 模拟 game PlayerActor 的注册/登录裁决。
func (m *mockActorRuntime) askPlayer(pid types.PID, env *gamev1.PlayerActorMsg) (any, error) {
	switch k := env.GetKind().(type) {
	case *gamev1.PlayerActorMsg_Register:
		return proto.Marshal(&gamev1.RegisterActorReply{
			Ok:       true,
			PlayerId: pid.UID(),
			Player:   &commonv1.PlayerSummary{PlayerId: pid.UID(), Nickname: k.Register.GetAccount()},
		})
	case *gamev1.PlayerActorMsg_Login:
		return proto.Marshal(&gamev1.LoginActorReply{
			Ok:     true,
			Player: &commonv1.PlayerSummary{PlayerId: pid.UID(), Nickname: k.Login.GetPlayerId()},
		})
	default:
		return proto.Marshal(&gamev1.LoginActorReply{Ok: false, ErrorReason: "UNKNOWN"})
	}
}

// askBattle 模拟 battle 战斗 actor：加入按 joinOK 裁决、帧输入/补帧记录。
func (m *mockActorRuntime) askBattle(pid types.PID, env *battlev1.BattleActorMsg) (any, error) {
	switch k := env.GetKind().(type) {
	case *battlev1.BattleActorMsg_Join:
		if !m.joinOK {
			return proto.Marshal(&battlev1.JoinBattleReply{Ok: false, ErrorReason: "PLAYER_NOT_IN_BATTLE"})
		}
		return proto.Marshal(&battlev1.JoinBattleReply{
			Ok: true,
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
		})
	case *battlev1.BattleActorMsg_FrameInput:
		m.frameInputs[pid.UID()] = append(m.frameInputs[pid.UID()], k.FrameInput)
		return proto.Marshal(&battlev1.FrameInputReply{})
	case *battlev1.BattleActorMsg_Reconnect:
		m.reconnects[pid.UID()] = append(m.reconnects[pid.UID()], k.Reconnect)
		return proto.Marshal(&battlev1.ReconnectReply{
			Ok:           true,
			CurrentFrame: 5,
			Missed: []*locksteppb.FrameInputs{
				{FrameId: 4, Inputs: []*locksteppb.LockstepInput{
					{FrameId: 4, PlayerId: "p-1", Payload: []byte("x")},
				}},
			},
		})
	case *battlev1.BattleActorMsg_Create:
		return proto.Marshal(&battlev1.CreateBattleReply{BattleId: pid.UID()})
	default:
		return proto.Marshal(&battlev1.JoinBattleReply{Ok: false, ErrorReason: "UNKNOWN"})
	}
}
