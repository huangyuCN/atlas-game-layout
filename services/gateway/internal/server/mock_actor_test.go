package server

import (
	"context"
	"fmt"
	"sync"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
)

// mockActorRuntime 模拟 game PlayerActor 与 battle 战斗 actor（gateway 单测装置）：
// 消息为具体对象直传（生成的桩 switch 直接命中），回执直接返回对象（同节点形态）；
// 注册/登录恒成功（player_id 取 PID uid）；加入战斗按 joinOK 裁决；帧输入/补帧记录投递。
type mockActorRuntime struct {
	logoutMsgs    []*gamev1.LogoutMsg                  // 登出投递记录
	matchEnters   []*gamev1.EnterMatchQueueReq         // 入队投递记录
	matchCanceled bool                                 // 取消投递记录
	joinOK        bool                                 // 加入战斗裁决（默认放行）
	frameInputs   map[string][]*battlev1.FrameInputReq // battleID → 帧输入序列（Tell 投递）
	reconnects    map[string][]*battlev1.SyncFramesReq // battleID → 补帧请求序列
}

func newMockActorRuntime() *mockActorRuntime {
	return &mockActorRuntime{
		joinOK:      true,
		frameInputs: make(map[string][]*battlev1.FrameInputReq),
		reconnects:  make(map[string][]*battlev1.SyncFramesReq),
	}
}

// Tell 实现 actorclient.Runtime（记录登出与帧输入投递：SendFrameInput 路由注解为
// 单向 Tell；其余 Tell 消息静默成功）。
func (m *mockActorRuntime) Tell(_ context.Context, pid types.PID, msg any, _ ...core.SendOption) error {
	switch r := msg.(type) {
	case *gamev1.LogoutMsg:
		m.logoutMsgs = append(m.logoutMsgs, r)
	case *battlev1.FrameInputReq:
		m.frameInputs[pid.UID()] = append(m.frameInputs[pid.UID()], r)
	}
	return nil
}

// Ask 实现 actorclient.Runtime：按域分派到对应桩处理（同节点直传形态）。
func (m *mockActorRuntime) Ask(_ context.Context, pid types.PID, req any, _ ...core.SendOption) (any, error) {
	switch req.(type) {
	case *battlev1.JoinBattleReq, *battlev1.SyncFramesReq, *battlev1.CreateBattleRequest:
		return m.askBattle(pid, req)
	case *gamev1.EnterMatchQueueReq, *gamev1.CancelMatchReq, *gamev1.GetMatchStatusReq:
		return m.askMatch(pid, req)
	case *gamev1.CreatePartyReq, *gamev1.JoinPartyReq, *gamev1.LeavePartyReq,
		*gamev1.GetPartyReq, *gamev1.QueuePartyReq:
		return m.askParty(pid, req)
	default:
		return m.askGame(pid, req)
	}
}

// askGame 返回玩家数据类桩回执（注册/登录恒成功，资料/背包直传）。
func (m *mockActorRuntime) askGame(pid types.PID, req any) (any, error) {
	switch r := req.(type) {
	case *gamev1.RegisterReq:
		return &gamev1.RegisterReply{
			PlayerId: pid.UID(),
			Player:   &commonv1.PlayerSummary{PlayerId: pid.UID(), Nickname: r.GetAccount()},
		}, nil
	case *gamev1.LoginReq:
		return &gamev1.LoginReply{
			Player: &commonv1.PlayerSummary{PlayerId: pid.UID(), Nickname: r.GetPlayerId()},
		}, nil
	case *gamev1.GetPlayerReq:
		return &gamev1.PlayerReply{
			Player: &commonv1.PlayerSummary{PlayerId: pid.UID(), Nickname: "mock", Level: 7},
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
func (m *mockActorRuntime) askMatch(pid types.PID, req any) (any, error) {
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
func (m *mockActorRuntime) askParty(pid types.PID, req any) (any, error) {
	switch r := req.(type) {
	case *gamev1.CreatePartyReq:
		return &gamev1.PartyReply{
			PartyId: "party-" + pid.UID(), LeaderId: pid.UID(),
			Members: []*commonv1.PlayerSummary{{PlayerId: pid.UID(), Level: 10}},
		}, nil
	case *gamev1.JoinPartyReq:
		return &gamev1.PartyReply{
			PartyId: r.GetPartyId(), LeaderId: "leader",
			Members: []*commonv1.PlayerSummary{{PlayerId: "leader"}, {PlayerId: pid.UID()}},
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

// askBattle 返回战斗类桩回执（加入按 joinOK 裁决；补帧记录投递并回执缺失帧）。
func (m *mockActorRuntime) askBattle(pid types.PID, req any) (any, error) {
	switch r := req.(type) {
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
	case *battlev1.SyncFramesReq:
		m.reconnects[pid.UID()] = append(m.reconnects[pid.UID()], r)
		return &battlev1.SyncFramesReply{
			CurrentFrame: 5,
			Missed: []*locksteppb.FrameInputs{
				{FrameId: 4, Inputs: []*locksteppb.LockstepInput{
					{FrameId: 4, PlayerId: "p-1", Payload: []byte("x")},
				}},
			},
		}, nil
	case *battlev1.CreateBattleRequest:
		return &battlev1.CreateBattleReply{BattleId: pid.UID()}, nil
	default:
		return nil, fmt.Errorf("mock: 未知请求类型 %T", req)
	}
}

// fakeTransport 是测试用流式传输上下文（模拟 TCP/WS/KCP 请求侧环境：连接 ID + 帧头），
// 供会话接口直调与透传测试构造身份来源（连接绑定 / 帧会话槽）。
type fakeTransport struct {
	kind transport.Kind // 模拟的传输种类（决定回写 pusher 的映射）
	op   string         // 模拟的请求 operation
	conn uint64         // 模拟的连接 ID（connFrom 按 connID 寻址）
	hdr  fakeHeader     // 模拟的帧请求头（帧会话槽等）
}

// Kind 返回模拟的传输种类。
func (f *fakeTransport) Kind() transport.Kind { return f.kind }

// Endpoint 返回测试端点占位。
func (f *fakeTransport) Endpoint() string { return "test://gateway" }

// Operation 返回模拟的请求 operation。
func (f *fakeTransport) Operation() string { return f.op }

// RequestHeader 返回帧请求头。
func (f *fakeTransport) RequestHeader() transport.Header { return f.hdr }

// ReplyHeader 返回 nil（帧传输无独立响应头语义）。
func (f *fakeTransport) ReplyHeader() transport.Header { return nil }

// ConnID 返回模拟的连接 ID（服务端推送寻址）。
func (f *fakeTransport) ConnID() uint64 { return f.conn }

// fakeHeader 是测试用 transport.Header（map 存取，模拟帧头载体）。
type fakeHeader map[string]string

// Get 读取键值。
func (h fakeHeader) Get(key string) string { return h[key] }

// Set 写入键值。
func (h fakeHeader) Set(key, value string) { h[key] = value }

// Add 追加键值（单值形态与 Set 同义）。
func (h fakeHeader) Add(key, value string) { h[key] = value }

// Delete 删除键。
func (h fakeHeader) Delete(key string) { delete(h, key) }

// Keys 返回全部键名。
func (h fakeHeader) Keys() []string {
	out := make([]string, 0, len(h))
	for k := range h {
		out = append(out, k)
	}
	return out
}

// Values 返回键的全部取值（单值形态）。
func (h fakeHeader) Values(key string) []string {
	if v, ok := h[key]; ok {
		return []string{v}
	}
	return nil
}

// fakePush 记录一次服务端下行推送。
type fakePush struct {
	connID  uint64 // 目标连接 ID
	op      string // 推送 operation（消息完整名）
	payload []byte // 推送载荷
}

// fakePusher 实现 pushServer：记录 PushRaw 调用（会话回写与挤下线断言用，
// 不写真实网络）。
type fakePusher struct {
	mu     sync.Mutex
	pushes []fakePush
}

// PushRaw 记录一次推送。
func (p *fakePusher) PushRaw(connID uint64, operation string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pushes = append(p.pushes, fakePush{
		connID:  connID,
		op:      operation,
		payload: append([]byte(nil), payload...),
	})
	return nil
}

// snapshot 返回已记录推送的副本（并发安全）。
func (p *fakePusher) snapshot() []fakePush {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]fakePush(nil), p.pushes...)
}

// connCtx 构造携带流式连接上下文的测试请求 ctx：kind 标识传输种类（TCP 业务 /
// KCP 战斗），frameToken 非空时注入帧会话槽（UDP/KCP 每帧验证身份的形态）。
func connCtx(kind transport.Kind, op string, connID uint64, frameToken string) context.Context {
	hdr := fakeHeader{}
	if frameToken != "" {
		hdr[frame.RequestHeaderKeySession] = frameToken
	}
	return transport.NewServerContext(context.Background(), &fakeTransport{
		kind: kind, op: op, conn: connID, hdr: hdr,
	})
}
