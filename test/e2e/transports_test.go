package e2e

import (
	"context"
	"fmt"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
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

// TestE2EWSAuth 验证单通道形态（SDK ws 单通道：认证与战斗共用一条连接）。
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

	// 战斗 op 经同一连接透传（连接绑定身份）。
	battle := battlev1opclient.NewBattleService(cli)
	join, err := battle.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: "b-1"})
	if err != nil {
		t.Fatalf("JoinBattle: %v", err)
	}
	if join == nil {
		t.Fatal("ws 战斗绑定回执为空")
	}
}

// TestE2EKCPBattle 验证战斗通道（kcp）：tcp 登录 + kcp 帧会话槽凭据绑定。
func TestE2EKCPBattle(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	newGame(t)
	gw := newGateway(t, "a")
	battle := newBattle(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	token, playerID := loginViaTCP(t, ctx, gw.TCPURL)
	seedBattle(t, ctx, battle, "b-1", playerID)

	cli := dialBattleChannel(t, sdkclient.DialKCP, gw.KCPURL, token)
	bcli := battlev1opclient.NewBattleService(cli)
	join, err := bcli.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: "b-1"})
	if err != nil {
		t.Fatalf("kcp JoinBattle: %v", err)
	}
	if join == nil {
		t.Fatal("kcp 战斗绑定回执为空")
	}
}

// TestE2EUDPBattle 验证战斗通道（udp）：tcp 登录 + udp 帧会话槽凭据绑定。
func TestE2EUDPBattle(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	newGame(t)
	gw := newGateway(t, "a")
	battle := newBattle(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	token, playerID := loginViaTCP(t, ctx, gw.TCPURL)
	seedBattle(t, ctx, battle, "b-1", playerID)

	cli := dialBattleChannel(t, sdkclient.DialUDP, gw.UDPURL, token)
	bcli := battlev1opclient.NewBattleService(cli)
	join, err := bcli.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: "b-1"})
	if err != nil {
		t.Fatalf("udp JoinBattle: %v", err)
	}
	if join == nil {
		t.Fatal("udp 战斗绑定回执为空")
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

// dialBattleChannel 拨号战斗通道（KCP/UDP）并注入帧会话槽凭据提供者。
func dialBattleChannel(t *testing.T, dial func(string, ...sdkclient.Option) (*sdkclient.Client, error), addr, token string) *sdkclient.Client {
	t.Helper()
	cli, err := dial(addr, sdkclient.WithSessionTokenProvider(func() string { return token }))
	if err != nil {
		t.Fatalf("战斗通道拨号: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
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

// loginViaTCP 经 tcp 完成注册登录（战斗通道测试的令牌来源），返回令牌与玩家 ID。
func loginViaTCP(t *testing.T, ctx context.Context, tcpURL string) (string, string) {
	t.Helper()
	sess, _ := dialBizSession(t, tcpURL)
	loginFlow(t, ctx, sess)
	return sess.Token(), sess.PlayerID()
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
