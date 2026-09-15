package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
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
	battle := battlev1.NewBattleServiceClient(cli)
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
	bcli := battlev1.NewBattleServiceClient(cli)
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
	bcli := battlev1.NewBattleServiceClient(cli)
	join, err := bcli.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: "b-1"})
	if err != nil {
		t.Fatalf("udp JoinBattle: %v", err)
	}
	if join == nil {
		t.Fatal("udp 战斗绑定回执为空")
	}
}

// TestE2EHTTPHealth 验证 http 健康检查（gateway 与 game）与 game 玩家 REST 管理接口。
func TestE2EHTTPHealth(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	game := newGame(t)
	gw := newGateway(t, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// gateway 健康检查。
	if body, err := httpGet(ctx, "http://"+gw.HTTPURL+"/health"); err != nil || !strings.Contains(body, `"ok"`) {
		t.Fatalf("gateway 健康检查: body=%q err=%v", body, err)
	}
	// game 健康检查。
	if body, err := httpGet(ctx, "http://"+game.HTTPURL+"/health"); err != nil || !strings.Contains(body, `"ok"`) {
		t.Fatalf("game 健康检查: body=%q err=%v", body, err)
	}

	// game 玩家 REST 管理接口：注册建档后查询。
	token, playerID := loginViaTCP(t, ctx, gw.TCPURL)
	_ = token
	body, err := httpGet(ctx, "http://"+game.HTTPURL+"/v1/players/"+playerID)
	if err != nil || !strings.Contains(body, `"playerId"`) {
		t.Fatalf("GetPlayer REST: body=%q err=%v", body, err)
	}
}

// dialBizSession 拨号 TCP 业务通道并绑定 SDK 会话（返回会话与底层客户端，推送订阅用）。
func dialBizSession(t *testing.T, tcpURL string, opts ...sdkclient.Option) (*sdkclient.Session, *sdkclient.Client) {
	t.Helper()
	sess := sdkclient.NewSession(sdkclient.WithSessionHeartbeatInterval(0))
	cli, err := sdkclient.Dial(tcpURL, append(testDialOpts(sess), opts...)...)
	if err != nil {
		t.Fatalf("tcp 拨号: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	sess.Bind(cli)
	return sess, cli
}

// dialWSSession 拨号 WS 单通道并绑定 SDK 会话（业务与战斗共用连接）。
func dialWSSession(t *testing.T, wsURL string, opts ...sdkclient.Option) (*sdkclient.Session, *sdkclient.Client) {
	t.Helper()
	sess := sdkclient.NewSession(sdkclient.WithSessionHeartbeatInterval(0))
	cli, err := sdkclient.DialWS(wsURL, "", append(testDialOpts(sess), opts...)...)
	if err != nil {
		t.Fatalf("ws 拨号: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	sess.Bind(cli)
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

// loginFlow 跑注册 → 登录 → 心跳（不登出）；凭据存入会话（Token/PlayerID 取用）。
func loginFlow(t *testing.T, ctx context.Context, sess *sdkclient.Session) {
	t.Helper()
	account := testAccount(t, "auth")
	reg, err := sess.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: "集成"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if reg.PlayerID == "" {
		t.Fatal("注册回执缺少 player_id")
	}
	if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.PlayerID, Password: "pw"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	var hb gatewayv1.HeartbeatReply
	if err := sess.Invoke(ctx, sdkclient.OpSessionHeartbeat, &gatewayv1.HeartbeatRequest{Ts: 1}, &hb); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if hb.GetServerTimeUnixMs() == 0 {
		t.Fatal("心跳回执缺 server_time")
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
