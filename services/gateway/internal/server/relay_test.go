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
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/transport"
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

// battleToken 登录玩家并返回其会话凭据（战斗透传用例共用装置）。
func battleToken(t *testing.T, env *gwEnv, playerID string) string {
	t.Helper()
	return env.login(t, 1, playerID).GetToken()
}

// TestJoinBattleBindsBattleChannel 验证 JoinBattle 透传成功后绑定战斗通道：
// 帧会话槽承载身份（KCP 形态），推送经战斗通道优先下发。
func TestJoinBattleBindsBattleChannel(t *testing.T) {
	mr, natsURL, pub := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)

	token := battleToken(t, env, "p-1")
	ctx := connCtx(transport.KindKCP, opJoinBattle, 77, token)
	rep := env.forward(t, ctx, opJoinBattle, &battlev1.JoinBattleReq{BattleId: "b-1"})
	join, ok := rep.(*battlev1.JoinBattleReply)
	if !ok {
		t.Fatalf("回执类型异常: %T", rep)
	}
	if join.GetMeta().GetSessionId() != "b-1" || join.GetCurrentFrame() != 3 || join.GetSnapshot() == nil {
		t.Fatalf("JoinBattle 元信息回执不符: %+v", join)
	}
	// 战斗通道已绑定 KCP 连接，业务通道不受影响。
	sess, ok := env.sess.LocalSession("p-1")
	if !ok || sess.Biz == nil || sess.Biz.ID != 1 {
		t.Fatalf("业务通道绑定不符: sess=%+v ok=%v", sess, ok)
	}
	if sess.Battle == nil || sess.Battle.ID != 77 {
		t.Fatalf("战斗通道未绑定 KCP 连接: %+v", sess.Battle)
	}
	// 绑定后推送经战斗通道下发（业务通道不重复下发）。
	pctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := PublishPush(pctx, pub, "p-1", consts.PushOpMatchStarted, []byte(`{"match_id":"m-1"}`)); err != nil {
		t.Fatalf("PublishPush: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return hasPush(env.kcp.snapshot(), 77, consts.PushOpMatchStarted) }) {
		t.Fatalf("战斗通道未收到推送: %+v", env.kcp.snapshot())
	}
	time.Sleep(200 * time.Millisecond)
	if hasPush(env.push.snapshot(), 1, consts.PushOpMatchStarted) {
		t.Fatal("推送应仅经战斗通道下发")
	}
}

// TestJoinBattleRejectsNonMember 验证 battle actor 拒绝非参战玩家且不绑定战斗通道。
func TestJoinBattleRejectsNonMember(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	env.mock.joinOK = false

	token := battleToken(t, env, "p-1")
	ctx := connCtx(transport.KindKCP, opJoinBattle, 77, token)
	entry := env.relayEntry(t, opJoinBattle)
	if _, err := env.g.Relay().Forward(ctx, entry, &battlev1.JoinBattleReq{BattleId: "b-1"}); err == nil {
		t.Fatal("非参战玩家加入战斗应报错")
	}
	// 业务通道会话保留；战斗通道未绑定。
	sess, ok := env.sess.LocalSession("p-1")
	if !ok || sess.Biz == nil {
		t.Fatalf("业务通道会话应存在: sess=%+v ok=%v", sess, ok)
	}
	if sess.Battle != nil {
		t.Fatalf("被拒加入后不应绑定战斗通道: %+v", sess.Battle)
	}
}

// TestSendFrameInputForwardsToBattle 验证帧输入 Tell 透传 battle actor（载荷原样，
// 身份经 sender 注入；伪造载荷字段不再由 gateway 改写）。
func TestSendFrameInputForwardsToBattle(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)

	token := battleToken(t, env, "p-1")
	joinCtx := connCtx(transport.KindKCP, opJoinBattle, 77, token)
	env.forward(t, joinCtx, opJoinBattle, &battlev1.JoinBattleReq{BattleId: "b-1"})

	// 帧输入上行（含伪造的载荷身份 p-9：透传引擎不改写业务载荷）。
	inCtx := connCtx(transport.KindKCP, opSendFrameInput, 77, token)
	env.forwardTell(t, inCtx, opSendFrameInput, &battlev1.FrameInputReq{
		BattleId: "b-1",
		Input:    &locksteppb.LockstepInput{FrameId: 1, PlayerId: "p-9", Payload: []byte("up")},
	})
	ins := env.mock.frameInputs["b-1"]
	if len(ins) != 1 || ins[0].GetBattleId() != "b-1" ||
		string(ins[0].GetInput().GetPayload()) != "up" || ins[0].GetInput().GetPlayerId() != "p-9" {
		t.Fatalf("帧输入投递不符（载荷应原样透传）: %+v", ins)
	}
}

// TestSendFrameInputRequiresBinding 验证未绑定身份的连接发帧输入被拒。
func TestSendFrameInputRequiresBinding(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)

	ctx := connCtx(transport.KindKCP, opSendFrameInput, 5, "")
	entry := env.relayEntry(t, opSendFrameInput)
	_, err := env.g.Relay().Forward(ctx, entry, &battlev1.FrameInputReq{
		BattleId: "b-1",
		Input:    &locksteppb.LockstepInput{FrameId: 1, PlayerId: "p-1", Payload: []byte("up")},
	})
	if !errorv1.IsInvalidToken(err) {
		t.Fatalf("未绑定身份应 INVALID_TOKEN, got %v", err)
	}
}

// TestSyncFramesForwardsToBattle 验证补帧请求透传 battle actor 并回执缺失帧。
func TestSyncFramesForwardsToBattle(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)

	token := battleToken(t, env, "p-1")
	joinCtx := connCtx(transport.KindKCP, opJoinBattle, 77, token)
	env.forward(t, joinCtx, opJoinBattle, &battlev1.JoinBattleReq{BattleId: "b-1"})

	syncCtx := connCtx(transport.KindKCP, opSyncFrames, 77, token)
	rep := env.forward(t, syncCtx, opSyncFrames, &battlev1.SyncFramesReq{BattleId: "b-1", LastSeenFrame: 3})
	sr, ok := rep.(*battlev1.SyncFramesReply)
	if !ok {
		t.Fatalf("回执类型异常: %T", rep)
	}
	if sr.GetCurrentFrame() != 5 || len(sr.GetMissed()) != 1 || sr.GetMissed()[0].GetFrameId() != 4 {
		t.Fatalf("补帧回执不符: %+v", sr)
	}
	// 补帧请求已按战斗 ID 路由投递（携带断点帧号）。
	recs := env.mock.reconnects["b-1"]
	if len(recs) != 1 || recs[0].GetLastSeenFrame() != 3 {
		t.Fatalf("补帧投递不符: %+v", recs)
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
// 会话登录（生成桩）后业务 op 帧 → relayHandler 解码 → Relay.Forward → 编码回执；
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

	// 未登录连接：业务 op 被拒。
	other := env.newTCPWireClient(t)
	var q2 gamev1.EnterMatchQueueReply
	if err := other.Invoke(ctx, opEnterMatch, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}, &q2); !errorv1.IsInvalidToken(err) {
		t.Fatalf("未登录应 INVALID_TOKEN, got %v", err)
	}
}

// TestIdempotencyID 验证投递去重键的注入决策：
// 注解声明 IDEMPOTENT 且客户端携带 ID → 注入；缺注解或缺 ID → 不注入（零开销原路径）。
func TestIdempotencyID(t *testing.T) {
	idempotent := relay.RouteEntry{Idempotency: relay.Idempotent}
	none := relay.RouteEntry{Idempotency: relay.IdempotencyNone}

	if got := idempotencyID(idempotent, "req-1"); got != "req-1" {
		t.Fatalf("声明幂等且携带 ID 应注入, got %q", got)
	}
	if got := idempotencyID(idempotent, ""); got != "" {
		t.Fatalf("客户端未携带 ID 不应注入, got %q", got)
	}
	if got := idempotencyID(none, "req-1"); got != "" {
		t.Fatalf("注解未声明不应注入, got %q", got)
	}
}
