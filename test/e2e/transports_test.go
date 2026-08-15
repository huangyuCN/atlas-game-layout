package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
)

// TestE2ETCPAuth 验证业务通道（tcp）：注册 → 登录 → 心跳 → 登出。
func TestE2ETCPAuth(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	newGame(t)
	gw := newGateway(t, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := tcpt.NewClient(gw.TCPURL)
	if err != nil {
		t.Fatalf("tcp client: %v", err)
	}
	defer cli.Close()
	auth := gatewayv1.NewGatewayAuthTCPClient(cli)
	token, playerID := loginFlow(t, ctx, auth)
	logoutFlow(t, ctx, auth, playerID, token)
}

// TestE2EWSAuth 验证单通道形态（ws）：认证与战斗共用一条连接。
func TestE2EWSAuth(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	newGame(t)
	gw := newGateway(t, "a")
	battleSvc := newBattle(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := wst.NewClient(ctx, gw.WSURL)
	if err != nil {
		t.Fatalf("ws client: %v", err)
	}
	defer cli.Close()
	auth := gatewayv1.NewGatewayAuthWSClient(cli)
	token, playerID := loginFlow(t, ctx, auth)
	seedBattle(t, ctx, battleSvc, "b-1", playerID)

	// 战斗协议绑定（单通道回退：同一 ws 连接）。
	battle := gatewayv1.NewGatewayBattleWSClient(cli)
	join, err := battle.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{Token: token, PlayerId: playerID, BattleId: "b-1"})
	if err != nil {
		t.Fatalf("JoinBattle: %v", err)
	}
	if !join.GetOk() {
		t.Fatal("ws 战斗绑定回执 ok=false")
	}
}

// TestE2EKCPBattle 验证战斗通道（kcp）：tcp 登录 + kcp 令牌快速绑定。
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

	cli, err := kcpt.NewClient(gw.KCPURL)
	if err != nil {
		t.Fatalf("kcp client: %v", err)
	}
	defer cli.Close()
	bcli := gatewayv1.NewGatewayBattleKCPClient(cli)
	join, err := bcli.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{Token: token, PlayerId: playerID, BattleId: "b-1"})
	if err != nil {
		t.Fatalf("kcp JoinBattle: %v", err)
	}
	if !join.GetOk() {
		t.Fatal("kcp 战斗绑定回执 ok=false")
	}
}

// TestE2EUDPBattle 验证战斗通道（udp）：tcp 登录 + udp 令牌快速绑定。
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

	cli, err := udpt.NewClient(gw.UDPURL)
	if err != nil {
		t.Fatalf("udp client: %v", err)
	}
	defer cli.Close()
	bcli := gatewayv1.NewGatewayBattleUDPClient(cli)
	join, err := bcli.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{Token: token, PlayerId: playerID, BattleId: "b-1"})
	if err != nil {
		t.Fatalf("udp JoinBattle: %v", err)
	}
	if !join.GetOk() {
		t.Fatal("udp 战斗绑定回执 ok=false")
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

// authClient 是认证客户端的最小公共接口（tcp/ws 生成客户端满足）。
type authClient interface {
	Register(context.Context, *gatewayv1.RegisterRequest) (*gatewayv1.RegisterReply, error)
	Login(context.Context, *gatewayv1.LoginRequest) (*gatewayv1.LoginReply, error)
	Heartbeat(context.Context, *gatewayv1.HeartbeatRequest) (*gatewayv1.HeartbeatReply, error)
	Logout(context.Context, *gatewayv1.LogoutRequest) (*gatewayv1.LogoutReply, error)
}

// loginFlow 跑注册 → 登录 → 心跳（不登出），返回令牌与玩家 ID（战斗通道绑定用）。
func loginFlow(t *testing.T, ctx context.Context, auth authClient) (string, string) {
	t.Helper()
	account := testAccount(t, "auth")
	reg, err := auth.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: "集成"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: "pw"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if _, err := auth.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: login.GetPlayerId(), Token: login.GetToken(), Ts: 1}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	return login.GetToken(), login.GetPlayerId()
}

// logoutFlow 登出并断言成功（登出闭环验证）。
func logoutFlow(t *testing.T, ctx context.Context, auth authClient, playerID, token string) {
	t.Helper()
	if _, err := auth.Logout(ctx, &gatewayv1.LogoutRequest{PlayerId: playerID, Token: token}); err != nil {
		t.Fatalf("Logout: %v", err)
	}
}

// loginViaTCP 经 tcp 完成注册登录（战斗通道测试的令牌来源），返回令牌与玩家 ID。
func loginViaTCP(t *testing.T, ctx context.Context, tcpURL string) (string, string) {
	t.Helper()
	cli, err := tcpt.NewClient(tcpURL)
	if err != nil {
		t.Fatalf("tcp client: %v", err)
	}
	defer cli.Close()
	token, playerID := loginFlow(t, ctx, gatewayv1.NewGatewayAuthTCPClient(cli))
	return token, playerID
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
