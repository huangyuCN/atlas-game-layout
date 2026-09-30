package server

import (
	"context"
	"fmt"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"
	"google.golang.org/protobuf/proto"
)

// mockDomain 是域服务桩（gateway 单测装置），同时实现两个注入点：
//   - opcall.MethodInvoker：业务 op 透传——按 operation 一元调用、回执写入出参，
//     与生产路径（域 rpc/ 平面 Edge 接口 → actor）同形；
//   - gamev1rpc.PlayerServiceClient：网关会话联动的 internal 面类型化客户端。
//
// 桩同时记录「投递了什么、以什么身份投递」：metadata 三键（player/request/sender）
// 是跨进程链路的关键假设，投递内容对了而身份错了同样算失败。
type mockDomain struct {
	loginReqs     []*gamev1.LoginReq           // 登录投递记录（版本门槛用例断言"未触达域"）
	logoutMsgs    []*gamev1.LogoutMsg          // 登出投递记录
	matchEnters   []*gamev1.EnterMatchQueueReq // 入队投递记录
	matchCanceled bool                         // 取消投递记录
	methods       []string                     // 被调用的 operation 序列（顺序断言用）
	callInfos     map[string]opcall.CallInfo   // operation → 最后一次调用身份
}

// newMockDomain 构造域服务桩。
func newMockDomain() *mockDomain {
	return &mockDomain{callInfos: make(map[string]opcall.CallInfo)}
}

// Invoke 实现 opcall.MethodInvoker：记录方法名与调用身份，按请求类型分派并写出回执。
func (m *mockDomain) Invoke(ctx context.Context, method string, req, rep proto.Message) error {
	info, _ := opcall.CallInfoFrom(ctx)
	m.record(method, info)
	out, err := m.handle(info.PlayerID, req)
	if err != nil {
		return err
	}
	if out != nil {
		proto.Merge(rep, out)
	}
	return nil
}

// record 记录一次调用（方法名 + 身份）。
func (m *mockDomain) record(method string, info opcall.CallInfo) {
	m.methods = append(m.methods, method)
	m.callInfos[method] = info
}

// handle 按请求类型分派到域桩：Tell 类（无回执）记为投递并返回 nil 回执。
func (m *mockDomain) handle(playerID string, req any) (proto.Message, error) {
	switch r := req.(type) {
	case *gamev1.LogoutMsg:
		m.logoutMsgs = append(m.logoutMsgs, r)
		return nil, nil
	case *gamev1.EnterMatchQueueReq, *gamev1.CancelMatchReq, *gamev1.GetMatchStatusReq:
		return m.askMatch(playerID, req)
	case *gamev1.CreatePartyReq, *gamev1.JoinPartyReq, *gamev1.LeavePartyReq,
		*gamev1.GetPartyReq, *gamev1.QueuePartyReq:
		return m.askParty(playerID, req)
	default:
		return m.askGame(playerID, req)
	}
}

// askGame 返回玩家数据类桩回执（注册/登录恒成功，资料/背包直传）。
func (m *mockDomain) askGame(playerID string, req any) (proto.Message, error) {
	switch r := req.(type) {
	case *gamev1.RegisterReq:
		return &gamev1.RegisterReply{
			PlayerId: playerID,
			Player:   &commonv1.PlayerSummary{PlayerId: playerID, Nickname: r.GetAccount()},
		}, nil
	case *gamev1.LoginReq:
		m.loginReqs = append(m.loginReqs, r)
		return &gamev1.LoginReply{
			Player: &commonv1.PlayerSummary{PlayerId: playerID, Nickname: r.GetPlayerId()},
		}, nil
	case *gamev1.GetPlayerReq:
		return &gamev1.PlayerReply{
			Player: &commonv1.PlayerSummary{PlayerId: playerID, Nickname: "mock", Level: 7},
		}, nil
	case *gamev1.GetBackpackReq:
		return &gamev1.BackpackReply{
			Items: []*gamev1.BackpackItem{{ItemId: 1001, Count: 6}},
		}, nil
	default:
		return nil, fmt.Errorf("mock: 未知请求类型 %T", req)
	}
}

// askMatch 记录匹配投递并返回桩回执（入队/取消/状态）。
func (m *mockDomain) askMatch(_ string, req any) (proto.Message, error) {
	switch r := req.(type) {
	case *gamev1.EnterMatchQueueReq:
		m.matchEnters = append(m.matchEnters, r)
		return &gamev1.EnterMatchQueueReply{}, nil
	case *gamev1.CancelMatchReq:
		m.matchCanceled = true
		return &gamev1.CancelMatchReply{Canceled: true}, nil
	case *gamev1.GetMatchStatusReq:
		return &gamev1.MatchStatusReply{
			State: matcherv1.MatchState_MATCH_STATE_WAITING, TicketId: "t-1", MatchId: "m-1",
		}, nil
	default:
		return nil, fmt.Errorf("mock: 未知请求类型 %T", req)
	}
}

// askParty 返回组队类桩回执（建队/加入/离开/名册/整队入队）。
func (m *mockDomain) askParty(playerID string, req any) (proto.Message, error) {
	switch r := req.(type) {
	case *gamev1.CreatePartyReq:
		return &gamev1.PartyReply{
			PartyId: "party-" + playerID, LeaderId: playerID,
			Members: []*commonv1.PlayerSummary{{PlayerId: playerID, Level: 10}},
		}, nil
	case *gamev1.JoinPartyReq:
		return &gamev1.PartyReply{
			PartyId: r.GetPartyId(), LeaderId: "leader",
			Members: []*commonv1.PlayerSummary{{PlayerId: "leader"}, {PlayerId: playerID}},
		}, nil
	case *gamev1.LeavePartyReq:
		return &gamev1.PartyReply{}, nil
	case *gamev1.GetPartyReq:
		return &gamev1.PartyReply{
			PartyId: "party-x", LeaderId: "leader",
			Members: []*commonv1.PlayerSummary{{PlayerId: "leader", Level: 10}},
		}, nil
	case *gamev1.QueuePartyReq:
		return &gamev1.PartyQueueReply{TicketId: "t-party-1"}, nil
	default:
		return nil, fmt.Errorf("mock: 未知请求类型 %T", req)
	}
}
