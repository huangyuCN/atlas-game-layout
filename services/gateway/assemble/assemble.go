// Package assemble 提供 gateway 服务的可编程装配入口：
// 供集成测试、工具链与将来「嵌入式部署」复用（fx 模块化装配的进程内形态，D2）。
package assemble

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	gwserver "github.com/huangyuCN/atlas-game-layout/services/gateway/internal/server"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
)

// Options 是 gateway 实例的装配参数。
type Options struct {
	ID            string
	EtcdEndpoints []string
	NatsURL       string
	RedisAddr     string
	TCPAddr       string // 空则 127.0.0.1:0（随机端口）
	SessionTTL    time.Duration
}

// Gateway 是装配完成的 gateway 实例句柄。
type Gateway struct {
	TCPURL  string              // 业务通道（tcp）
	WSURL   string              // 单通道形态（ws，业务+战斗共用）
	KCPURL  string              // 战斗通道（kcp）
	UDPURL  string              // 战斗通道（udp）
	HTTPURL string              // 健康检查（http）
	Actors  *actorclient.Client // 远程 actor 客户端（测试/观测用）
	stop    func(ctx context.Context) error
}

// New 装配并启动一个 gateway 实例（五协议 + 分布式会话 + actor 集群客户端）。
func New(ctx context.Context, opts Options) (*Gateway, error) {
	if opts.SessionTTL <= 0 {
		opts.SessionTTL = 30 * time.Second
	}
	if opts.TCPAddr == "" {
		opts.TCPAddr = "127.0.0.1:0"
	}

	cli, err := pkredis.NewClient(pkredis.Options{Addr: opts.RedisAddr})
	if err != nil {
		return nil, fmt.Errorf("assemble: redis: %w", err)
	}
	nc, err := pkgnats.Connect(pkgnats.Options{URL: opts.NatsURL, Name: "gw-" + opts.ID})
	if err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: nats: %w", err)
	}
	ec, err := etcd.NewClient(etcd.Options{Endpoints: opts.EtcdEndpoints})
	if err != nil {
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: etcd: %w", err)
	}
	disc, err := pkgregistry.NewEtcdDiscovery(ec, pkgregistry.Options{})
	if err != nil {
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: 服务发现: %w", err)
	}
	rt, err := pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        "gw-" + opts.ID,
		ServiceName:   "game", // 懒激活在 game 节点执行（PlayerActor 宿主）
		EtcdEndpoints: opts.EtcdEndpoints,
		NatsURL:       opts.NatsURL,
		Discovery:     disc,
	})
	if err != nil {
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: actor 运行时: %w", err)
	}
	if err := rt.Start(ctx); err != nil {
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: actor 启动: %w", err)
	}
	if err := pkgactor.RegisterPlayerReplica(rt); err != nil {
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: 懒激活副本: %w", err)
	}
	actors := actorclient.NewClient(rt)

	tcpSrv, err := tcpt.NewServer(tcpt.WithAddress(opts.TCPAddr))
	if err != nil {
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: tcp: %w", err)
	}
	wsSrv, err := wst.NewServer(wst.WithAddress("127.0.0.1:0"))
	if err != nil {
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: ws: %w", err)
	}
	kcpSrv, err := kcpt.NewServer(kcpt.WithAddress("127.0.0.1:0"))
	if err != nil {
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: kcp: %w", err)
	}
	udpSrv, err := udpt.NewServer(udpt.WithAddress("127.0.0.1:0"))
	if err != nil {
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: udp: %w", err)
	}
	sess := session.NewManager(session.NewRedisStore(cli), "gw-"+opts.ID, opts.SessionTTL)
	g := gwserver.NewGateway("gw-"+opts.ID, sess, actors, nc, tcpSrv, wsSrv, kcpSrv, udpSrv)
	if err := gwserver.RegisterGatewayHandlers(tcpSrv, wsSrv, kcpSrv, udpSrv, g); err != nil {
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: 注册协议: %w", err)
	}
	relayCtx, relayCancel := context.WithCancel(context.Background())
	if err := g.StartRelay(relayCtx); err != nil {
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: 推送订阅: %w", err)
	}
	sess.Start(relayCtx)

	sctx, scancel := context.WithCancel(context.Background())
	if err := tcpSrv.Start(sctx); err != nil {
		scancel()
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: tcp 启动: %w", err)
	}
	ep, err := tcpSrv.Endpoint()
	if err != nil {
		scancel()
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: tcp 端点: %w", err)
	}

	// WebSocket：httptest 包装（单通道形态，业务与战斗共用）。
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsSrv.Handler())
	wsHTTP := httptest.NewServer(mux)

	// KCP/UDP 战斗通道启动并取地址。
	if err := kcpSrv.Start(sctx); err != nil {
		wsHTTP.Close()
		scancel()
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: kcp 启动: %w", err)
	}
	kcpEP, err := kcpSrv.Endpoint()
	if err != nil {
		wsHTTP.Close()
		scancel()
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: kcp 端点: %w", err)
	}
	if err := udpSrv.Start(sctx); err != nil {
		wsHTTP.Close()
		scancel()
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: udp 启动: %w", err)
	}
	udpEP, err := udpSrv.Endpoint()
	if err != nil {
		wsHTTP.Close()
		scancel()
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: udp 端点: %w", err)
	}

	// HTTP 健康检查。
	httpSrv, err := atlashttp.NewServer(atlashttp.WithAddress("127.0.0.1:0"))
	if err != nil {
		wsHTTP.Close()
		scancel()
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: http: %w", err)
	}
	httpSrv.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"gateway"}`))
	})
	hctx, hcancel := context.WithCancel(context.Background())
	go func() { _ = httpSrv.Start(hctx) }()
	httpEP, err := serverutil.WaitEndpoint(httpSrv, 2*time.Second)
	if err != nil {
		hcancel()
		wsHTTP.Close()
		scancel()
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: http 启动: %w", err)
	}

	stop := func(ctx context.Context) error {
		hcancel()
		_ = httpSrv.Stop(ctx)
		wsHTTP.Close()
		scancel()
		_ = tcpSrv.Stop(ctx)
		_ = kcpSrv.Stop(ctx)
		_ = udpSrv.Stop(ctx)
		relayCancel()
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		return cli.Close()
	}
	return &Gateway{
		TCPURL:  ep.Host,
		WSURL:   "ws" + strings.TrimPrefix(wsHTTP.URL, "http") + "/ws",
		KCPURL:  kcpEP.Host,
		UDPURL:  udpEP.Host,
		HTTPURL: httpEP.Host,
		Actors:  actors,
		stop:    stop,
	}, nil
}

// Stop 停止 gateway 实例并释放全部资源。
func (g *Gateway) Stop(ctx context.Context) error {
	if g.stop == nil {
		return nil
	}
	return g.stop(ctx)
}
