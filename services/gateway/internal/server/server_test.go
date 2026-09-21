package server

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natss "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// 测试引用的透传 op（注解生成路由表的寻址键，纯协议寻址字符串）。
const (
	opJoinBattle     = "/battle.v1.BattleService/JoinBattle"
	opSendFrameInput = "/battle.v1.BattleService/SendFrameInput"
	opSyncFrames     = "/battle.v1.BattleService/SyncFrames"
	opEnterMatch     = "/game.v1.PlayerService/EnterMatchQueue"
	opCancelMatch    = "/game.v1.PlayerService/CancelMatch"
	opMatchStatus    = "/game.v1.PlayerService/GetMatchStatus"
	opCreateParty    = "/game.v1.PlayerService/CreateParty"
	opJoinParty      = "/game.v1.PlayerService/JoinParty"
	opLeaveParty     = "/game.v1.PlayerService/LeaveParty"
	opGetParty       = "/game.v1.PlayerService/GetParty"
	opQueueParty     = "/game.v1.PlayerService/QueueParty"
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

// gwEnv 是一个 gateway 实例的完整本地装配（会话直调 + 透传 + 推送测试共用）。
type gwEnv struct {
	id     string
	sess   *session.Manager
	g      *Gateway
	mock   *mockActorRuntime // game/battle actor 桩（断言投递用）
	push   *fakePusher       // TCP 业务通道推送记录（会话回写断言用）
	kcp    *fakePusher       // KCP 战斗通道推送记录（通道绑定优先级断言用）
	tcpSrv *tcpt.Server
	tcpURL string
}

// newGWEnv 构造一个 gateway 实例（miniredis + 内嵌 nats 装置）。
func newGWEnv(t *testing.T, id string, mr *miniredis.Miniredis, natsURL string) *gwEnv {
	return newGWEnvWithRedis(t, id, mr.Addr(), natsURL)
}

// newGWEnvWithRedis 以真实/内存 redis 地址构造 gateway 实例（单测与集成共用装置）。
// 四协议 Server 全量构造并完成会话 + 透传注册（验证装配路径），仅 TCP 启动
// （帧链路端到端用）；会话回写注入推送记录桩，断言不依赖真实客户端连接。
func newGWEnvWithRedis(t *testing.T, id, redisAddr, natsURL string) *gwEnv {
	return newGWEnvMeter(t, id, redisAddr, natsURL, metrics.Noop())
}

// newGWEnvMeter 同 newGWEnvWithRedis，但注入指定指标采集器（业务打点断言用）。
func newGWEnvMeter(t *testing.T, id, redisAddr, natsURL string, meter metrics.Collector) *gwEnv {
	t.Helper()
	cli, err := pkredis.NewClient(pkredis.Options{Addrs: []string{redisAddr}})
	if err != nil {
		t.Fatalf("redis client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	nc, err := nats.Connect(nats.Options{URL: natsURL, Name: "gw-" + id})
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)

	tcpSrv, wsSrv, kcpSrv, udpSrv := newTransportServers(t)
	push, kcpPush := new(fakePusher), new(fakePusher)

	sess := session.NewManager(session.NewRedisStore(cli), id, 30*time.Second)
	mock := newMockActorRuntime()
	g := NewGateway(id, newRouteTable(t), sess, actorclient.NewClient(mock), meter, nc,
		push, new(fakePusher), kcpPush, udpSrv)
	// 通道绑定副作用按生产装配声明（与 graph.go 一致）。
	g.Relay().WithChannelBinding(opJoinBattle, relay.Slot(session.ChannelBattle))
	cfg := &conf.Bootstrap{
		Tcp:       &conf.Bootstrap_Net{Addr: "127.0.0.1:0"},
		Websocket: &conf.Bootstrap_Net{Addr: "127.0.0.1:0"},
		Kcp:       &conf.Bootstrap_Net{Addr: "127.0.0.1:0"},
		Udp:       &conf.Bootstrap_Net{Addr: "127.0.0.1:0"},
	}
	if err := RegisterGatewayHandlers(cfg, tcpSrv, wsSrv, kcpSrv, udpSrv, g); err != nil {
		t.Fatalf("registerHandlers: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := g.StartRelay(ctx); err != nil {
		t.Fatalf("StartRelay: %v", err)
	}
	sess.Start(ctx)

	startTCPServer(t, tcpSrv)
	return &gwEnv{
		id:     id,
		sess:   sess,
		g:      g,
		mock:   mock,
		push:   push,
		kcp:    kcpPush,
		tcpSrv: tcpSrv,
		tcpURL: tcpAddr(t, tcpSrv),
	}
}

// newTransportServers 构造四协议传输服务端（仅构造不启动；注册在构造期即可完成）。
func newTransportServers(t *testing.T) (*tcpt.Server, *wst.Server, *kcpt.Server, *udpt.Server) {
	t.Helper()
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
	return tcpSrv, wsSrv, kcpSrv, udpSrv
}

// newRouteTable 合并注解生成的两张域路由表（与生产 graph.go 装配一致）。
func newRouteTable(t *testing.T) relay.Table {
	t.Helper()
	table, err := relay.Merge(gamev1.PlayerServiceRouteTable, battlev1.BattleServiceRouteTable)
	if err != nil {
		t.Fatalf("relay.Merge: %v", err)
	}
	// 通道绑定副作用按生产装配声明（WithChannelBinding，与 graph.go 一致）：
	// JoinBattle 转发成功后把当前连接绑定到玩家战斗通道。
	return table
}

// startTCPServer 启动 TCP 服务端并注册停止清理（帧链路端到端测试用）。
func startTCPServer(t *testing.T, srv *tcpt.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("tcp Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = srv.Stop(context.Background())
	})
}

// tcpAddr 返回 TCP 服务端监听地址。
func tcpAddr(t *testing.T, srv *tcpt.Server) string {
	t.Helper()
	ep, err := srv.Endpoint()
	if err != nil {
		t.Fatalf("tcp Endpoint: %v", err)
	}
	return ep.Host
}

// newTCPWireClient 建立真实 TCP 连接客户端（验证生成桩与透传 handler 的帧链路行为）。
func (e *gwEnv) newTCPWireClient(t *testing.T) *tcpt.Client {
	t.Helper()
	cli, err := tcpt.NewClient(e.tcpURL)
	if err != nil {
		t.Fatalf("tcp client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// login 以 TCP 业务连接形态直调 Gateway.Login（connID 标识连接，身份由连接承载）。
func (e *gwEnv) login(t *testing.T, connID uint64, playerID string) *gatewayv1.LoginReply {
	t.Helper()
	ctx := connCtx(transport.KindTCP, gatewayv1.OperationSessionLoginTCP, connID, "")
	rep, err := e.g.Login(ctx, &gatewayv1.LoginRequest{PlayerId: playerID, Password: "x"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	return rep
}

// relayEntry 查询透传路由条目（缺失即失败，防路由表漂移静默跳过）。
func (e *gwEnv) relayEntry(t *testing.T, operation string) relay.RouteEntry {
	t.Helper()
	entry, ok := e.g.Relay().Lookup(operation)
	if !ok {
		t.Fatalf("透传路由缺失: %s", operation)
	}
	return entry
}

// forward 是请求-响应型透传的断言包装（失败即终止，返回业务回执）。
func (e *gwEnv) forward(t *testing.T, ctx context.Context, operation string, req proto.Message) proto.Message {
	t.Helper()
	rep, err := e.g.Relay().Forward(ctx, e.relayEntry(t, operation), req)
	if err != nil {
		t.Fatalf("Forward %s: %v", operation, err)
	}
	return rep
}

// forwardTell 是单向 Tell 型透传的断言包装（不应有回执）。
func (e *gwEnv) forwardTell(t *testing.T, ctx context.Context, operation string, req proto.Message) {
	t.Helper()
	rep, err := e.g.Relay().Forward(ctx, e.relayEntry(t, operation), req)
	if err != nil {
		t.Fatalf("Forward %s: %v", operation, err)
	}
	if rep != nil {
		t.Fatalf("Tell 型透传不应有回执: %T", rep)
	}
}

// waitFor 轮询等待异步链路条件成立（nats 订阅/清扫回调），超时返回 false。
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// hasPush 判断推送记录中是否存在指定连接与 operation 的下行。
func hasPush(pushes []fakePush, connID uint64, op string) bool {
	for _, p := range pushes {
		if p.connID == connID && p.op == op {
			return true
		}
	}
	return false
}

// TestLoginLogoutClosesSession 验证登录 → 登出的会话闭环（身份由连接承载）。
func TestLoginLogoutClosesSession(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)

	login := env.login(t, 1, "p-1")
	if login.GetToken() == "" || login.GetPlayerId() != "p-1" || login.GetPlayer().GetNickname() != "p-1" {
		t.Fatalf("登录回执不符: %+v", login)
	}
	// 登录即建立会话（业务通道绑定连接 1）。
	if sess, ok := env.sess.LocalSession("p-1"); !ok || sess.Biz == nil || sess.Biz.ID != 1 {
		t.Fatalf("登录会话绑定不符: sess=%+v ok=%v", sess, ok)
	}
	// 心跳续租（身份由连接承载，以会话自身凭据续租）。
	hctx := connCtx(transport.KindTCP, gatewayv1.OperationSessionHeartbeatTCP, 1, "")
	hb, err := env.g.Heartbeat(hctx, &gatewayv1.HeartbeatRequest{Ts: 1})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if hb.GetServerTimeUnixMs() == 0 {
		t.Fatalf("心跳回执缺服务端时间: %+v", hb)
	}
	// 登出（身份从连接反查）后本地会话与路由清除。
	if _, err := env.g.Logout(connCtx(transport.KindTCP, gatewayv1.OperationSessionLogoutTCP, 1, ""), &gatewayv1.LogoutRequest{}); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if _, ok := env.sess.LocalSession("p-1"); ok {
		t.Fatal("登出后本地会话应清除")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if r, err := env.sess.Route(ctx, "p-1"); err != nil || r != nil {
		t.Fatalf("登出后路由 = %+v, err = %v, want nil", r, err)
	}
}

// TestHeartbeatRejectsUnboundConn 验证未绑定玩家身份的连接心跳被拒。
func TestHeartbeatRejectsUnboundConn(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)

	ctx := connCtx(transport.KindTCP, gatewayv1.OperationSessionHeartbeatTCP, 5, "")
	if _, err := env.g.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{Ts: 1}); !errorv1.IsInvalidToken(err) {
		t.Fatalf("未绑定连接心跳应 INVALID_TOKEN, got %v", err)
	}
}

// TestResumeRestoresSession 验证断线重连：凭据校验通过后重绑新连接（免密，凭据沿用）。
func TestResumeRestoresSession(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)

	token := env.login(t, 1, "p-1").GetToken()
	// 原连接断开（等价于直接用新连接请求 Resume：身份尚未绑定）。
	rep, err := env.g.Resume(connCtx(transport.KindTCP, gatewayv1.OperationSessionResumeTCP, 2, ""),
		&gatewayv1.ResumeRequest{PlayerId: "p-1", Token: token})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if rep.GetPlayerId() != "p-1" {
		t.Fatalf("恢复回执不符: %+v", rep)
	}
	// 恢复后业务通道重绑新连接，旧连接反向索引注销。
	if sess, ok := env.sess.LocalSession("p-1"); !ok || sess.Biz == nil || sess.Biz.ID != 2 {
		t.Fatalf("恢复后业务通道应绑定新连接: sess=%+v", sess)
	}
	// 凭据不匹配拒绝恢复（旧令牌/被接管）。
	if _, err := env.g.Resume(connCtx(transport.KindTCP, gatewayv1.OperationSessionResumeTCP, 3, ""),
		&gatewayv1.ResumeRequest{PlayerId: "p-1", Token: "bad"}); !errorv1.IsInvalidToken(err) {
		t.Fatalf("错误凭据恢复应拒绝, got %v", err)
	}
}

// TestKickSameInstance 验证同实例二次登录挤下线旧连接（旧连接收到被挤通知 + 联动清理）。
func TestKickSameInstance(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)

	login1 := env.login(t, 1, "p-1")
	login2 := env.login(t, 2, "p-1")
	if login2.GetToken() == login1.GetToken() {
		t.Fatal("二次登录应签发新令牌")
	}
	// 旧连接（connID=1）收到被挤下线推送。
	if !hasPush(env.push.snapshot(), 1, consts.PushOpKickedOffline) {
		t.Fatalf("旧连接未收到挤下线推送: %+v", env.push.snapshot())
	}
	// 会话令牌已被新登录覆盖（旧凭据失效）。
	if sess, ok := env.sess.LocalSession("p-1"); !ok || sess.Token != login2.GetToken() {
		t.Fatalf("会话令牌应被覆盖: sess=%+v", sess)
	}
	// 挤下线联动撮合域清理（kickOld：取消匹配 + 离队）。
	if !env.mock.matchCanceled {
		t.Fatal("挤下线应联动向 PlayerActor 取消匹配")
	}
}
