package server

import (
	"context"
	"fmt"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"google.golang.org/protobuf/proto"
)

// mockActorRuntime 模拟 game PlayerActor（gateway 单测装置）：
// 注册/登录恒成功回执（player_id 取 PID uid），登出记录投递。
type mockActorRuntime struct {
	tells []*gamev1.PlayerActorMsg
}

func newMockActorRuntime() *mockActorRuntime { return &mockActorRuntime{} }

// Tell 实现 actorclient.Runtime（记录登出投递）。
func (m *mockActorRuntime) Tell(_ context.Context, _ types.PID, msg any) error {
	if env, ok := msg.(*gamev1.PlayerActorMsg); ok {
		m.tells = append(m.tells, env)
	}
	return nil
}

// Ask 实现 actorclient.Runtime：按信封类型返回序列化回执（跨节点形态）。
func (m *mockActorRuntime) Ask(_ context.Context, pid types.PID, req any) (any, error) {
	env, ok := req.(*gamev1.PlayerActorMsg)
	if !ok {
		return nil, fmt.Errorf("mock: 未知请求类型 %T", req)
	}
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
