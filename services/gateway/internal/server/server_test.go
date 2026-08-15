package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natss "github.com/nats-io/nats.go"
)

// newSharedBackends 起共享中间件：miniredis 与内嵌 nats，返回发布用连接。
func newSharedBackends(t *testing.T) (*miniredis.Miniredis, string, *natss.Conn) {
	t.Helper()
	mr := miniredis.RunT(t)
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1})
	if err != nil {
		t.Fatalf("启动 nats-server 失败: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server 未就绪")
	}
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(nats.Options{URL: ns.ClientURL(), Name: "publisher"})
	if err != nil {
		t.Fatalf("连接 nats 失败: %v", err)
	}
	t.Cleanup(nc.Close)
	return mr, ns.ClientURL(), nc
}

// gwEnv 是一个 gateway 实例的完整本地装配（双实例专项用）。
type gwEnv struct {
	id     string
	sess   *session.Manager
	g      *Gateway
	tcpSrv *tcpt.Server
	wsSrv  *wst.Server
	wsHTTP *httptest.Server
	tcpURL string
	wsURL  string
}

// newGWEnv 构造一个 gateway 实例（tcp/ws 真启动，kcp/udp 仅构造注册）。
func newGWEnv(t *testing.T, id string, mr *miniredis.Miniredis, natsURL string) *gwEnv {
	return newGWEnvWithRedis(t, id, mr.Addr(), natsURL)
}

// newGWEnvWithRedis 以真实/内存 redis 地址构造 gateway 实例（集成与单测共用装置）。
func newGWEnvWithRedis(t *testing.T, id, redisAddr, natsURL string) *gwEnv {
	t.Helper()
	cli, err := pkredis.NewClient(pkredis.Options{Addr: redisAddr})
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	nc, err := nats.Connect(nats.Options{URL: natsURL, Name: "gw-" + id})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)

	tcpSrv, err := tcpt.NewServer(tcpt.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("tcp server: %v", err)
	}
	wsSrv, err := wst.NewServer(wst.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("ws server: %v", err)
	}
	kcpSrv, err := kcpt.NewServer(kcpt.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("kcp server: %v", err)
	}
	udpSrv, err := udpt.NewServer(udpt.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("udp server: %v", err)
	}

	sess := session.NewManager(session.NewRedisStore(cli), id, 30*time.Second)
	g := NewGateway(id, sess, actorclient.NewClient(noopRuntime{}), nc, tcpSrv, wsSrv, kcpSrv, udpSrv)
	if err := registerHandlers(tcpSrv, wsSrv, kcpSrv, udpSrv, g); err != nil {
		t.Fatalf("registerHandlers: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := g.StartRelay(ctx); err != nil {
		t.Fatalf("StartRelay: %v", err)
	}
	sess.Start(ctx)

	sctx, scancel := context.WithCancel(context.Background())
	if err := tcpSrv.Start(sctx); err != nil {
		t.Fatalf("tcp Start: %v", err)
	}
	t.Cleanup(func() {
		scancel()
		_ = tcpSrv.Stop(context.Background())
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsSrv.Handler())
	wsHTTP := httptest.NewServer(mux)
	t.Cleanup(wsHTTP.Close)

	tcpEP, err := tcpSrv.Endpoint()
	if err != nil {
		t.Fatalf("tcp Endpoint: %v", err)
	}

	return &gwEnv{
		id:     id,
		sess:   sess,
		g:      g,
		tcpSrv: tcpSrv,
		wsSrv:  wsSrv,
		wsHTTP: wsHTTP,
		tcpURL: tcpEP.Host,
		wsURL:  "ws" + strings.TrimPrefix(wsHTTP.URL, "http") + "/ws",
	}
}

// newTCPAuthClient 建立 TCP 业务连接与认证客户端。
func (e *gwEnv) newTCPAuthClient(t *testing.T) (*tcpt.Client, gatewayv1.GatewayAuthTCPClient) {
	t.Helper()
	cli, err := tcpt.NewClient(e.tcpURL)
	if err != nil {
		t.Fatalf("tcp client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli, gatewayv1.NewGatewayAuthTCPClient(cli)
}

// newWSClients 建立 WS 连接（单通道形态：认证 + 战斗共用）。
func (e *gwEnv) newWSClients(t *testing.T) (*wst.Client, gatewayv1.GatewayAuthWSClient, gatewayv1.GatewayBattleWSClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	cli, err := wst.NewClient(ctx, e.wsURL)
	if err != nil {
		t.Fatalf("ws client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli, gatewayv1.NewGatewayAuthWSClient(cli), gatewayv1.NewGatewayBattleWSClient(cli)
}

// TestLoginHeartbeatLogout 验证登录 → 心跳 → 登出的会话闭环。
func TestLoginHeartbeatLogout(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	_, auth := env.newTCPAuthClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{Account: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if login.GetToken() == "" || login.GetPlayerId() != "p-1" {
		t.Fatalf("登录回执不符: %+v", login)
	}
	if _, err := auth.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: "p-1", Token: login.GetToken(), Ts: 1}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if _, err := auth.Logout(ctx, &gatewayv1.LogoutRequest{PlayerId: "p-1", Token: login.GetToken()}); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	// 登出后路由清除。
	if r, err := env.sess.Route(ctx, "p-1"); err != nil || r != nil {
		t.Fatalf("登出后路由 = %+v, err = %v, want nil", r, err)
	}
	// 登出后令牌失效。
	if _, err := auth.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: "p-1", Token: login.GetToken()}); err == nil {
		t.Fatal("登出后心跳应失败")
	}
}

// TestKickSameInstance 验证同实例二次登录挤下线旧连接。
func TestKickSameInstance(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	oldCli, oldAuth := env.newTCPAuthClient(t)
	login1, err := oldAuth.Login(ctx, &gatewayv1.LoginRequest{Account: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("首次登录: %v", err)
	}
	// 注册挤下线监听，然后发起二次登录。
	kicked := make(chan struct{}, 1)
	oldCli.OnNotify(func(operation string, payload []byte) {
		if operation == PushOpKickedOffline {
			kicked <- struct{}{}
		}
	})
	_, newAuth := env.newTCPAuthClient(t)
	if _, err := newAuth.Login(ctx, &gatewayv1.LoginRequest{Account: "p-1", Password: "x"}); err != nil {
		t.Fatalf("二次登录: %v", err)
	}
	select {
	case <-kicked:
	case <-time.After(2 * time.Second):
		t.Fatal("旧连接未收到被挤下线通知")
	}
	// 旧会话令牌已被覆盖：旧 token 心跳失败。
	if _, err := oldAuth.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: "p-1", Token: login1.GetToken(), Ts: 1}); err == nil {
		t.Fatal("旧会话心跳应失败")
	}
}

// TestJoinBattleBindsChannel 验证 WS 单通道形态：登录后战斗绑定走同一连接。
func TestJoinBattleBindsChannel(t *testing.T) {
	mr, natsURL, pub := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, auth, battle := env.newWSClients(t)
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{Account: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	join, err := battle.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{Token: login.GetToken(), PlayerId: "p-1", BattleId: "b-1"})
	if err != nil {
		t.Fatalf("JoinBattle: %v", err)
	}
	if !join.GetOk() {
		t.Fatal("JoinBattle 回执 ok=false")
	}

	// 战斗通道绑定后推送仍可达（同连接回退）。
	got := make(chan struct{}, 1)
	cli.OnNotify(func(operation string, payload []byte) {
		if operation == "gateway.v1.MatchStartedNotify" {
			got <- struct{}{}
		}
	})
	if err := PublishPush(ctx, pub, "p-1", "gateway.v1.MatchStartedNotify", []byte(`{"match_id":"m-1"}`)); err != nil {
		t.Fatalf("PublishPush: %v", err)
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("未收到推送")
	}
}
