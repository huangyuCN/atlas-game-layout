package e2e

import (
	"context"
	"fmt"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gamev1opclient "github.com/huangyuCN/atlas-game-layout/api/game/v1/opclient"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/scripts/sdksession"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
)

// TestE2ETCPAuth 验证业务通道（tcp，SDK 会话驱动）：注册 → 登录 → 心跳 → 登出。
func TestE2ETCPAuth(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	newGame(t)
	gw := newGateway(t, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, _ := dialBizSession(t, gw.TCPURL)
	loginFlow(t, ctx, sess)
	logoutFlow(t, ctx, sess)
}

// TestE2EWSAuth 验证业务通道（ws）的认证与心跳；并断言**战斗 op 经同一连接明确失败**：
// 阶段 3 批次 5 起战斗 op 不再经网关（battle 在线也不可达），失败是 op 级
// （帧引擎层 TRANSPORT_NOT_FOUND）——同一连接上的业务 op 仍照常可达。
func TestE2EWSAuth(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	newGame(t)
	gw := newGateway(t, "a")
	battleSvc := newBattle(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, cli := dialWSSession(t, gw.WSURL)
	loginFlow(t, ctx, sess)
	seedBattle(t, ctx, battleSvc, "b-1", sess.PlayerID())

	// 破坏性断言（WS 面）：战斗 op 明确失败——不是静默丢弃、不是超时、不是回执为空。
	op := battlev1opclient.BattleServiceProtocolOps.JoinBattle
	var join battlev1.JoinBattleReply
	err := cli.Invoke(ctx, op, &battlev1.JoinBattleReq{BattleId: "b-1"}, &join)
	assertGatewayRejectsBattleOp(t, err, op)
	// 同一连接上业务 op 不受影响（拒绝是 op 级，不是连接坏了）。
	var data gamev1.PlayerDataReply
	if err := cli.Invoke(ctx, gamev1opclient.PlayerServiceProtocolOps.GetPlayerData, &gamev1.GetPlayerDataReq{}, &data); err != nil {
		t.Fatalf("同一连接上业务 op 应可达: %v", err)
	}
	if data.GetPlayer().GetPlayerId() != sess.PlayerID() {
		t.Fatalf("业务 op 回执不符: %+v", data.GetPlayer())
	}
}

// TestE2EKCPBattle 验证 KCP 直连帧面（阶段 3 批次 5 后战斗帧的唯一承载路径之一）：
// 客户端凭 battle 出的票直连帧端口，入局/帧输入经本地 actor 投递走通。
func TestE2EKCPBattle(t *testing.T) { assertDirectFrameFace(t, frameKCP) }

// TestE2EUDPBattle 验证 UDP 直连帧面（同上，裸 UDP 面）。
func TestE2EUDPBattle(t *testing.T) { assertDirectFrameFace(t, frameUDP) }

// assertDirectFrameFace 跑一个直连帧面闭环：出票 → 直连 → 入局 → 帧输入（Tell）→ 无票负例。
func assertDirectFrameFace(t *testing.T, kind frameKind) {
	t.Helper()
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	newGame(t)
	battle := newBattle(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	battleID, playerID := "b-"+string(kind), "p-"+string(kind)
	seedBattle(t, ctx, battle, battleID, playerID)
	ticket := issueTicket(t, ctx, battle, battleID, playerID)
	addr := frameAddrOf(battle, kind)

	frame := dialDirectFrame(t, ctx, kind, addr, func() []byte { return ticket })
	var join battlev1.JoinBattleReply
	if err := frame.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
		&battlev1.JoinBattleReq{BattleId: battleID}, &join); err != nil {
		t.Fatalf("%s 直连 JoinBattle: %v", kind, err)
	}
	if join.GetMeta().GetSessionId() != battleID {
		t.Fatalf("%s 直连入局回执不符: %+v", kind, join.GetMeta())
	}
	// 帧输入（Tell，无回执）走同一连接与同一张票：载荷不带 player_id，身份来自票。
	if err := frame.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SendFrameInput, &battlev1.FrameInputReq{
		BattleId: battleID,
		Input:    &locksteppb.LockstepInput{FrameId: 1, Payload: []byte{1}},
	}, nil); err != nil {
		t.Fatalf("%s 直连帧输入: %v", kind, err)
	}

	// 负例：匿名帧（无票）直连被**明确拒绝**（BATTLE_TICKET_INVALID），不是静默。
	anon := dialDirectFrame(t, ctx, kind, addr, func() []byte { return nil })
	var denied battlev1.JoinBattleReply
	err := anon.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
		&battlev1.JoinBattleReq{BattleId: battleID}, &denied)
	if err == nil || !errorv1.IsBattleTicketInvalid(err) {
		t.Fatalf("%s 无票直连应被拒 BATTLE_TICKET_INVALID, got %v", kind, err)
	}
}

// TestE2EHTTPHealth 验证 http 健康检查（gateway 与 game）与 game 管理面的玩家查询。
func TestE2EHTTPHealth(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	game := newGame(t)
	gw := newGateway(t, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// gateway 健康检查。
	if body, err := httpGet(ctx, "http://"+gw.HTTPURL+"/health"); err != nil || !strings.Contains(body, `"ok"`) {
		t.Fatalf("gateway 健康检查: body=%q err=%v", body, err)
	}
	// game 健康检查。
	if body, err := httpGet(ctx, "http://"+game.HTTPURL+"/health"); err != nil || !strings.Contains(body, `"ok"`) {
		t.Fatalf("game 健康检查: body=%q err=%v", body, err)
	}

	// 玩家查询（P7 接管）：旧 service Player 的 REST 入口已删除，改由管理面 QueryPlayer 覆盖。
	sess, _ := dialBizSession(t, gw.TCPURL)
	loginFlow(t, ctx, sess)
	rep, err := dialAdmin(t, game.GRPCURL).QueryPlayer(ctx, &admingamev1.QueryPlayerRequest{
		Context: adminCtx("e2e-health", "", false), PlayerId: sess.PlayerID(),
	})
	if err != nil {
		t.Fatalf("管理面 QueryPlayer: %v", err)
	}
	if rep.GetPlayerId() != sess.PlayerID() || rep.GetLevel() != 1 || rep.GetNickname() == "" {
		t.Fatalf("管理面玩家投影不符: %+v", rep)
	}
}

// dialBizSession 拨号 TCP 业务通道并绑定 SDK 会话（返回会话与底层客户端，推送订阅用）。
func dialBizSession(t *testing.T, tcpURL string, opts ...sdkclient.Option) (*sdkclient.Session, *sdkclient.Client) {
	t.Helper()
	return dialBizSessionWith(t, tcpURL, nil, opts...)
}

// dialBizSessionWith 在 dialBizSession 基础上追加**会话级**选项（如 WithOnKicked 接缝回调：
// 推送原因经 SessionProtocol 按帧头版本解码，见 SDK 接缝 S0.5 修订 1）。
func dialBizSessionWith(t *testing.T, tcpURL string, sessOpts []sdkclient.SessionOption, opts ...sdkclient.Option) (*sdkclient.Session, *sdkclient.Client) {
	t.Helper()
	sess := sdksession.NewSession(append([]sdkclient.SessionOption{
		sdkclient.WithSessionHeartbeatInterval(0),
	}, sessOpts...)...)
	cli, err := sdkclient.Dial(tcpURL, append(testDialOpts(sess), opts...)...)
	if err != nil {
		t.Fatalf("tcp 拨号: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	if err := sess.Bind(cli); err != nil {
		t.Fatalf("会话绑定: %v", err)
	}
	return sess, cli
}

// dialWSSession 拨号 WS 单通道并绑定 SDK 会话（业务与战斗共用连接）。
func dialWSSession(t *testing.T, wsURL string, opts ...sdkclient.Option) (*sdkclient.Session, *sdkclient.Client) {
	t.Helper()
	sess := sdksession.NewSession(sdkclient.WithSessionHeartbeatInterval(0))
	cli, err := sdkclient.DialWS(wsURL, "", append(testDialOpts(sess), opts...)...)
	if err != nil {
		t.Fatalf("ws 拨号: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	if err := sess.Bind(cli); err != nil {
		t.Fatalf("会话绑定: %v", err)
	}
	return sess, cli
}

// testDialOpts 测试客户端公共参数：传输保活 + 会话凭据装配（会话心跳关闭，
// 心跳语义由用例显式 Invoke 验证）。
func testDialOpts(sess *sdkclient.Session) []sdkclient.Option {
	return append([]sdkclient.Option{sdkclient.WithHeartbeatInterval(5 * time.Second)}, sess.ChannelOptions()...)
}

// replyOf 断言 SDK 会话回执的具体生成类型（any → 模板生成的会话 DTO）。回执类型由接缝 op
// 与生成物共同决定，类型不符即契约漂移，直接失败而不是静默取零值。
func replyOf[T any](t *testing.T, raw any) T {
	t.Helper()
	typed, ok := raw.(T)
	if !ok {
		var zero T
		t.Fatalf("会话回执类型不符：期望 %T", zero)
	}
	return typed
}

// loginFlow 跑注册 → 登录 → 心跳（不登出）；凭据存入会话（Token/PlayerID 取用）。
func loginFlow(t *testing.T, ctx context.Context, sess *sdkclient.Session) {
	t.Helper()
	account := testAccount(t, "auth")
	rawReg, err := sess.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: "集成"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	reg := replyOf[*gatewayv1.RegisterReply](t, rawReg)
	if reg.GetPlayerId() == "" {
		t.Fatal("注册回执缺少 player_id")
	}
	if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: "pw"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	rawHb, err := sess.Heartbeat(ctx)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	hb := replyOf[*gatewayv1.HeartbeatReply](t, rawHb)
	if hb.GetServerTimeUnixMs() == 0 {
		t.Fatal("心跳回执缺 server_time（生成 DTO 的 server_time_unix_ms 为 0）")
	}
}

// logoutFlow 登出并断言成功（登出闭环验证）。
func logoutFlow(t *testing.T, ctx context.Context, sess *sdkclient.Session) {
	t.Helper()
	if err := sess.Logout(ctx); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if sess.Token() != "" {
		t.Fatal("登出后会话凭据应清空")
	}
}

// httpGet 发起 GET 请求并返回响应体（超时受 ctx 约束）。
func httpGet(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return string(body), fmt.Errorf("状态码 %d", resp.StatusCode)
	}
	return string(body), nil
}
