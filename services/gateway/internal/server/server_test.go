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
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	atlaserrors "github.com/huangyuCN/atlas/errors"
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
	mock   *mockActorRuntime // game/battle actor 桩（断言投递用）
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
	mock := newMockActorRuntime()
	g := NewGateway(id, sess, actorclient.NewClient(mock), nc, tcpSrv, wsSrv, kcpSrv, udpSrv)
	if err := RegisterGatewayHandlers(tcpSrv, wsSrv, kcpSrv, udpSrv, g); err != nil {
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
		mock:   mock,
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

	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
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
	login1, err := oldAuth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("首次登录: %v", err)
	}
	// 注册挤下线监听，然后发起二次登录。
	kicked := make(chan struct{}, 1)
	oldCli.OnNotify(func(operation string, payload []byte) {
		if operation == consts.PushOpKickedOffline {
			kicked <- struct{}{}
		}
	})
	_, newAuth := env.newTCPAuthClient(t)
	if _, err := newAuth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"}); err != nil {
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
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
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
	// 会话元信息/当前帧/快照由 battle actor 回执（M7）。
	if join.GetMeta().GetSessionId() != "b-1" || join.GetCurrentFrame() != 3 || join.GetSnapshot() == nil {
		t.Fatalf("JoinBattle 元信息回执不符: %+v", join)
	}

	// 战斗通道绑定后推送仍可达（同连接回退）。
	got := make(chan struct{}, 1)
	cli.OnNotify(func(operation string, payload []byte) {
		if operation == consts.PushOpMatchStarted {
			got <- struct{}{}
		}
	})
	if err := PublishPush(ctx, pub, "p-1", consts.PushOpMatchStarted, []byte(`{"match_id":"m-1"}`)); err != nil {
		t.Fatalf("PublishPush: %v", err)
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("未收到推送")
	}
}

// TestJoinBattleRejectsNonMember 验证 battle actor 拒绝非参战玩家且不绑定战斗通道。
func TestJoinBattleRejectsNonMember(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	// 注入拒绝加入的 battle 裁决。
	env.mock.joinOK = false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, auth, battle := env.newWSClients(t)
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if _, err := battle.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{Token: login.GetToken(), PlayerId: "p-1", BattleId: "b-1"}); err == nil {
		t.Fatal("非参战玩家加入战斗应报错")
	}
	// 未绑定战斗通道：本地会话无 Battle 连接。
	sess, ok := env.sess.LocalSession("p-1")
	if !ok {
		t.Fatal("本地会话应存在（业务通道绑定）")
	}
	if sess.Battle != nil {
		t.Fatal("被拒加入后不应绑定战斗通道")
	}
}

// TestSendFrameInputForwardsToBattle 验证帧输入透传 battle actor（信封 + 路由）。
func TestSendFrameInputForwardsToBattle(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, auth, battle := env.newWSClients(t)
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if _, err := battle.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{Token: login.GetToken(), PlayerId: "p-1", BattleId: "b-1"}); err != nil {
		t.Fatalf("JoinBattle: %v", err)
	}
	_, err = battle.SendFrameInput(ctx, &gatewayv1.SendFrameInputRequest{
		BattleId: "b-1",
		// 伪造他人身份：gateway 应以连接会话绑定为准改写为 p-1。
		Input: &locksteppb.LockstepInput{FrameId: 1, PlayerId: "p-9", Payload: []byte("up")},
	})
	if err != nil {
		t.Fatalf("SendFrameInput: %v", err)
	}
	// 帧输入已按战斗 ID 路由投递，且身份被改回会话绑定玩家（防伪造）。
	ins := env.mock.frameInputs["b-1"]
	if len(ins) != 1 || ins[0].GetPlayerId() != "p-1" ||
		ins[0].GetInput().GetPlayerId() != "p-1" || string(ins[0].GetInput().GetPayload()) != "up" {
		t.Fatalf("帧输入投递不符: %+v", ins)
	}
}

// TestSendFrameInputRequiresBinding 验证未绑定战斗通道的连接发帧输入被拒。
func TestSendFrameInputRequiresBinding(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, battle := env.newWSClients(t)
	if _, err := battle.SendFrameInput(ctx, &gatewayv1.SendFrameInputRequest{
		BattleId: "b-1",
		Input:    &locksteppb.LockstepInput{FrameId: 1, PlayerId: "p-1", Payload: []byte("up")},
	}); err == nil {
		t.Fatal("未绑定身份的连接发帧输入应被拒")
	}
}

// TestSyncFramesForwardsToBattle 验证补帧请求透传 battle actor 并回执缺失帧。
func TestSyncFramesForwardsToBattle(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, auth, battle := env.newWSClients(t)
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if _, err := battle.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{Token: login.GetToken(), PlayerId: "p-1", BattleId: "b-1"}); err != nil {
		t.Fatalf("JoinBattle: %v", err)
	}
	reply, err := battle.SyncFrames(ctx, &locksteppb.SyncFrameRequest{SessionId: "b-1", FromFrameId: 3, Limit: 10})
	if err != nil {
		t.Fatalf("SyncFrames: %v", err)
	}
	if reply.GetConfirmedFrameId() != 5 || len(reply.GetFrames()) != 1 || reply.GetFrames()[0].GetFrameId() != 4 {
		t.Fatalf("补帧回执不符: %+v", reply)
	}
	// 补帧请求已携带断点帧号与会话绑定玩家投递。
	recs := env.mock.reconnects["b-1"]
	if len(recs) != 1 || recs[0].GetLastSeenFrame() != 3 || recs[0].GetPlayerId() != "p-1" {
		t.Fatalf("补帧投递不符: %+v", recs)
	}
}

// TestMatchQueueForwardsToPlayerActor 验证匹配端点：入队/取消/状态经生成桩转发到
// game PlayerActor；无效令牌拒绝（INVALID_TOKEN），业务错误透传。
func TestMatchQueueForwardsToPlayerActor(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	cli, auth := env.newTCPAuthClient(t)
	matchCli := gatewayv1.NewGatewayMatchTCPClient(cli)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	// 无效令牌拒绝。
	if _, err := matchCli.QueueMatch(ctx, &gatewayv1.MatchQueueRequest{Token: "bad", PlayerId: "p-1", Ruleset: "casual"}); atlaserrors.Reason(err) != "INVALID_TOKEN" {
		t.Fatalf("无效令牌应拒绝, got %v", err)
	}

	// 有效令牌：入队成功并投递到 actor。
	if _, err := matchCli.QueueMatch(ctx, &gatewayv1.MatchQueueRequest{Token: login.GetToken(), PlayerId: "p-1", Ruleset: "casual"}); err != nil {
		t.Fatalf("QueueMatch: %v", err)
	}
	if len(env.mock.matchEnters) != 1 || env.mock.matchEnters[0].GetRuleset() != "casual" {
		t.Fatalf("入队投递不符: %+v", env.mock.matchEnters)
	}

	// 状态查询转发回执。
	st, err := matchCli.MatchStatus(ctx, &gatewayv1.MatchStatusRequest{Token: login.GetToken(), PlayerId: "p-1"})
	if err != nil {
		t.Fatalf("MatchStatus: %v", err)
	}
	if st.GetState() != matcherv1.MatchState_MATCH_STATE_WAITING || st.GetTicketId() != "t-1" {
		t.Fatalf("状态回执不符: %+v", st)
	}

	// 取消转发。
	ca, err := matchCli.CancelMatch(ctx, &gatewayv1.MatchCancelRequest{Token: login.GetToken(), PlayerId: "p-1"})
	if err != nil {
		t.Fatalf("CancelMatch: %v", err)
	}
	if !ca.GetCanceled() || !env.mock.matchCanceled {
		t.Fatalf("取消不符: reply=%+v mock=%v", ca, env.mock.matchCanceled)
	}
}
