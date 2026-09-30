package server

import (
	"context"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"google.golang.org/protobuf/proto"
)

// forwardParty 是组队 op 的断言包装（回执固定为名册快照类型）。
func forwardParty(t *testing.T, env *gwEnv, ctx context.Context, op string, req proto.Message) *gamev1.PartyReply {
	t.Helper()
	rep, ok := env.forward(t, ctx, op, req).(*gamev1.PartyReply)
	if !ok {
		t.Fatalf("%s 回执类型异常: %T", op, rep)
	}
	return rep
}

// TestBattleOpsNotRoutedByGateway 是阶段 3 批次 5 的破坏性断言（单测层）：
// 网关的路由表只合并 **game** 域（装配白名单收紧），battle 的 CLIENT op 一个都不在表里。
// 经网关发战斗 op 因此在帧引擎层**明确失败**（TRANSPORT_NOT_FOUND，不是静默丢弃），
// 客户端只能凭 battle_ticket 直连接入层 → battle 帧面。
func TestBattleOpsNotRoutedByGateway(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)

	clientOps := 0
	for op, entry := range battlev1.BattleServiceRouteTable {
		if entry.Access != relay.AccessClient {
			continue
		}
		clientOps++
		if _, ok := env.g.Relay().Lookup(op); ok {
			t.Errorf("battle CLIENT op %s 不应出现在网关路由表（白名单只收 game）", op)
		}
	}
	if clientOps == 0 {
		t.Fatal("battle 路由表没有 CLIENT op：本断言失去意义（表形态变了？）")
	}
	// 正对照：game 域 op 仍在表里——白名单只收紧了 battle，不是把透传整个关掉。
	if _, ok := env.g.Relay().Lookup(opEnterMatch); !ok {
		t.Fatal("game 域 op 应在网关路由表中（透传引擎仍服务业务 op）")
	}
}

// TestMatchQueueForwardsToPlayerActor 验证匹配 op 经透传引擎转发到 game
// PlayerActor：无效帧会话槽拒绝（INVALID_TOKEN），业务错误透传。
func TestMatchQueueForwardsToPlayerActor(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	env.login(t, 1, "p-1")

	// 无效帧会话槽凭据拒绝（凭据即身份，不再来自请求 payload）。
	badCtx := connCtx(transport.KindTCP, opEnterMatch, 9, "bad-token")
	entry := env.relayEntry(t, opEnterMatch)
	if _, err := env.g.Relay().Forward(badCtx, entry, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}); atlaserrors.Reason(err) != "INVALID_TOKEN" {
		t.Fatalf("无效凭据应拒绝, got %v", err)
	}

	// 业务通道连接绑定身份：入队转发到 actor。
	env.forward(t, connCtx(transport.KindTCP, opEnterMatch, 1, ""), opEnterMatch, &gamev1.EnterMatchQueueReq{Ruleset: "casual"})
	if len(env.mock.matchEnters) != 1 || env.mock.matchEnters[0].GetRuleset() != "casual" {
		t.Fatalf("入队投递不符: %+v", env.mock.matchEnters)
	}

	// 状态查询回执。
	sctx := connCtx(transport.KindTCP, opMatchStatus, 1, "")
	st, ok := env.forward(t, sctx, opMatchStatus, &gamev1.GetMatchStatusReq{}).(*gamev1.MatchStatusReply)
	if !ok {
		t.Fatal("状态回执类型异常")
	}
	if st.GetState() != matcherv1.MatchState_MATCH_STATE_WAITING || st.GetTicketId() != "t-1" {
		t.Fatalf("状态回执不符: %+v", st)
	}

	// 取消转发。
	ca, ok := env.forward(t, connCtx(transport.KindTCP, opCancelMatch, 1, ""), opCancelMatch, &gamev1.CancelMatchReq{}).(*gamev1.CancelMatchReply)
	if !ok {
		t.Fatal("取消回执类型异常")
	}
	if !ca.GetCanceled() || !env.mock.matchCanceled {
		t.Fatalf("取消不符: reply=%+v mock=%v", ca, env.mock.matchCanceled)
	}
}

// TestPartyProjectionForwardsToPlayerActor 验证组队 op：建队/加入/名册/整队入队/离开
// 经透传引擎转发到 PlayerActor；离开回执空快照。
func TestPartyProjectionForwardsToPlayerActor(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	env.login(t, 1, "p-1")
	tcpCtx := func(op string) context.Context { return connCtx(transport.KindTCP, op, 1, "") }

	// 建队：回执快照（队长自身）。
	cp := forwardParty(t, env, tcpCtx(opCreateParty), opCreateParty, &gamev1.CreatePartyReq{})
	if cp.GetPartyId() != "party-p-1" || cp.GetLeaderId() != "p-1" || len(cp.GetMembers()) != 1 {
		t.Fatalf("建队回执不符: %+v", cp)
	}

	// 加入：回执携带客体队伍名册。
	jp := forwardParty(t, env, tcpCtx(opJoinParty), opJoinParty, &gamev1.JoinPartyReq{PartyId: "party-p-1"})
	if jp.GetPartyId() != "party-p-1" || len(jp.GetMembers()) != 2 {
		t.Fatalf("加入回执不符: %+v", jp)
	}

	// 名册快照查询。
	gp := forwardParty(t, env, tcpCtx(opGetParty), opGetParty, &gamev1.GetPartyReq{})
	if gp.GetPartyId() != "party-x" {
		t.Fatalf("名册回执不符: %+v", gp)
	}

	// 整队入队（QueuePartyReq 携带规则集）。
	qr, ok := env.forward(t, tcpCtx(opQueueParty), opQueueParty, &gamev1.QueuePartyReq{Ruleset: "casual"}).(*gamev1.PartyQueueReply)
	if !ok {
		t.Fatal("整队入队回执类型异常")
	}
	if qr.GetTicketId() != "t-party-1" {
		t.Fatalf("整队入队回执不符: %+v", qr)
	}

	// 离开回执空快照（mock LeaveParty 返回空）。
	if lv := forwardParty(t, env, tcpCtx(opLeaveParty), opLeaveParty, &gamev1.LeavePartyReq{}); lv.GetPartyId() != "" {
		t.Fatalf("离开回执应为空快照: %+v", lv)
	}
}

// TestRelayTCPWireForward 验证透传 handler 在真实 TCP 帧链路上的端到端行为：
// 会话登录（生成桩）后业务 op 帧 → frameops handler 解码 → 身份解析 → 远端投递 → 编码回执；
// 未登录连接被拒（INVALID_TOKEN）。
func TestRelayTCPWireForward(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli := env.newTCPWireClient(t)
	auth := gatewayv1.NewSessionTCPClient(cli)
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if login.GetToken() == "" || login.GetPlayerId() != "p-1" {
		t.Fatalf("登录回执不符: %+v", login)
	}

	var q gamev1.EnterMatchQueueReply
	if err := cli.Invoke(ctx, opEnterMatch, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}, &q); err != nil {
		t.Fatalf("EnterMatchQueue: %v", err)
	}
	if len(env.mock.matchEnters) != 1 || env.mock.matchEnters[0].GetRuleset() != "casual" {
		t.Fatalf("入队投递不符: %+v", env.mock.matchEnters)
	}
	// 身份随投递下发（metadata 三键）：真实帧链路上玩家身份与发起者都必须是 p-1。
	info := env.mock.callInfos[opEnterMatch]
	if info.PlayerID != "p-1" || info.SenderPID != "player:p-1" {
		t.Fatalf("投递身份不符: %+v", info)
	}

	// 未登录连接：业务 op 被拒。
	other := env.newTCPWireClient(t)
	var q2 gamev1.EnterMatchQueueReply
	if err := other.Invoke(ctx, opEnterMatch, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}, &q2); !errorv1.IsInvalidToken(err) {
		t.Fatalf("未登录应 INVALID_TOKEN, got %v", err)
	}
}

// TestCallInfoOf 验证网关投递身份的组装口径（跨进程链路的关键假设）：
//   - 客户端 op：player 身份 = 会话身份，发起者 = 该玩家（客户端不可影响），
//     请求 ID 取帧头（观测头；**去重键的取舍不在这里**——接收侧按路由条目的
//     Idempotency 声明决定，见 opcall.PlanFromMetadata）；
//   - 网关自身的会话联动：不下发发起者（网关不是玩家，凭空造一个会让同源校验失效）；
//   - 未携带请求 ID / 身份非法：不注入（不臆造）。
func TestCallInfoOf(t *testing.T) {
	ctx := requestIDCtx(transport.KindTCP, opEnterMatch, 1, "", "req-1")
	info := callInfoOf(ctx, "p-1", true)
	if info.PlayerID != "p-1" || info.SenderPID != "player:p-1" || info.RequestID != "req-1" {
		t.Fatalf("客户端 op 身份不符: %+v", info)
	}

	sess := callInfoOf(ctx, "p-1", false)
	if sess.PlayerID != "p-1" || sess.RequestID != "req-1" || sess.SenderPID != "" {
		t.Fatalf("会话联动不应下发发起者: %+v", sess)
	}

	if got := callInfoOf(connCtx(transport.KindTCP, opEnterMatch, 1, ""), "p-1", true); got.RequestID != "" {
		t.Fatalf("客户端未携带请求 ID 不应注入: %+v", got)
	}
	if got := callInfoOf(context.Background(), "", true); got.SenderPID != "" {
		t.Fatalf("身份非法不应下发发起者: %+v", got)
	}
}

// requestIDCtx 在 connCtx 基础上注入帧请求 ID（Atlas-Frame-Request-Id），
// 用于验证「帧头 → ctx → 投递计划」单向链的取值。
func requestIDCtx(kind transport.Kind, op string, connID uint64, frameToken, requestID string) context.Context {
	ctx := connCtx(kind, op, connID, frameToken)
	if tr, ok := transport.FromServerContext(ctx); ok && tr.RequestHeader() != nil {
		tr.RequestHeader().Set(frame.RequestHeaderKeyRequestID, requestID)
	}
	return ctx
}
