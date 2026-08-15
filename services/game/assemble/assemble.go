// Package assemble 提供 game 服务的可编程装配入口：
// 供集成测试、工具链与将来「嵌入式部署」复用（fx 模块化装配的进程内形态，D2）。
package assemble

import (
	"context"
	"fmt"
	"net/http"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	gameactor "github.com/huangyuCN/atlas-game-layout/services/game/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas/registry"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
)

// 快照参数（与 server 装配对齐）。
const (
	snapshotTTL  = 5 * time.Minute
	snapshotTick = 10 * time.Second
)

// Options 是 game 服务的装配参数。
type Options struct {
	NodeID        string
	EtcdEndpoints []string
	NatsURL       string
	RedisAddr     string
	MongoURI      string
	MongoDB       string
	GRPCAddr      string        // 空则 127.0.0.1:0（随机端口）
	SessionTTL    time.Duration // 0 则默认 30s
}

// Game 是装配完成的 game 服务句柄。
type Game struct {
	Runtime *pkgactor.Runtime
	GRPCURL string
	HTTPURL string // 健康检查 + 玩家业务 REST 管理接口
	stop    func(ctx context.Context) error
}

// New 装配并启动 game 服务（actor 集群运行时 + 业务 + gRPC）。
func New(ctx context.Context, opts Options) (*Game, error) {
	if opts.SessionTTL <= 0 {
		opts.SessionTTL = 30 * time.Second
	}
	if opts.GRPCAddr == "" {
		opts.GRPCAddr = "127.0.0.1:0"
	}

	ec, err := etcd.NewClient(etcd.Options{Endpoints: opts.EtcdEndpoints})
	if err != nil {
		return nil, fmt.Errorf("assemble: etcd: %w", err)
	}
	disc, err := pkgregistry.NewEtcdDiscovery(ec, pkgregistry.Options{})
	if err != nil {
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: 服务发现: %w", err)
	}
	rt, err := pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        opts.NodeID,
		ServiceName:   "game",
		EtcdEndpoints: opts.EtcdEndpoints,
		NatsURL:       opts.NatsURL,
		Discovery:     disc,
	})
	if err != nil {
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: actor 运行时: %w", err)
	}

	mc, err := pkgmongo.NewClient(ctx, pkgmongo.Options{URI: opts.MongoURI, Database: opts.MongoDB})
	if err != nil {
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: mongo: %w", err)
	}
	persist, err := repo.NewMongoPlayerRepo(ctx, mc)
	if err != nil {
		_ = mc.Close(ctx)
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: 玩家仓储: %w", err)
	}
	rcli, err := pkredis.NewClient(pkredis.Options{Addr: opts.RedisAddr})
	if err != nil {
		_ = mc.Close(ctx)
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: redis: %w", err)
	}
	store := repo.NewPlayerStore(repo.NewRedisPlayerCache(rcli), persist)
	svc := handler.NewPlayerHandler(store, repo.NewRedisSessionStore(rcli), biz.PlayerServiceOptions{
		SessionTTL: opts.SessionTTL,
	})
	if err := rt.Register(gameactor.NewProps(svc, store, snapshotTTL, snapshotTick)); err != nil {
		_ = rcli.Close()
		_ = mc.Close(ctx)
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: 注册 PlayerActor: %w", err)
	}
	if err := rt.Start(ctx); err != nil {
		_ = rcli.Close()
		_ = mc.Close(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: actor 启动: %w", err)
	}

	grpcSrv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(opts.GRPCAddr))
	if err != nil {
		_ = rt.Shutdown(ctx)
		_ = rcli.Close()
		_ = mc.Close(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: grpc: %w", err)
	}
	gameSvc := handler.NewGameHandler(gameactor.NewPlayerClient(rt))
	gamev1.RegisterPlayerServer(grpcSrv, gameSvc)
	gctx, gcancel := context.WithCancel(context.Background())
	// Atlas gRPC Server.Start 为阻塞式 serve（生命周期由 App.Run 管理），
	// 进程内装配需在后台启动并轮询端点就绪。
	go func() { _ = grpcSrv.Start(gctx) }()
	ep, err := serverutil.WaitEndpoint(grpcSrv, 2*time.Second)
	if err != nil {
		gcancel()
		_ = rt.Shutdown(ctx)
		_ = rcli.Close()
		_ = mc.Close(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: grpc 启动: %w", err)
	}

	// 注册 game 服务实例：actor 集群懒激活按服务发现选节点，
	// 实例 ID 必须与 actor NodeID 一致（见 cluster.placement.serviceInstanceToNode）。
	reg, err := pkgregistry.NewEtcd(ec, pkgregistry.Options{})
	if err != nil {
		gcancel()
		_ = rt.Shutdown(ctx)
		_ = rcli.Close()
		_ = mc.Close(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: 注册中心: %w", err)
	}
	instance := &registry.ServiceInstance{
		ID:        opts.NodeID,
		Name:      "game",
		Version:   "0.1.0",
		Endpoints: []string{"grpc://" + ep.Host},
	}
	if err := reg.Register(ctx, instance); err != nil {
		gcancel()
		_ = rt.Shutdown(ctx)
		_ = rcli.Close()
		_ = mc.Close(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: 注册服务: %w", err)
	}

	// HTTP：健康检查 + 玩家业务 REST 管理接口（grpc/http 双形态接入验证）。
	httpSrv, err := atlashttp.NewServer(atlashttp.WithAddress("127.0.0.1:0"))
	if err != nil {
		gcancel()
		_ = rt.Shutdown(ctx)
		_ = rcli.Close()
		_ = mc.Close(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: http: %w", err)
	}
	httpSrv.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"game"}`))
	})
	gamev1.RegisterPlayerHTTPServer(httpSrv, gameSvc)
	hctx, hcancel := context.WithCancel(context.Background())
	go func() { _ = httpSrv.Start(hctx) }()
	httpEP, err := serverutil.WaitEndpoint(httpSrv, 2*time.Second)
	if err != nil {
		hcancel()
		gcancel()
		_ = rt.Shutdown(ctx)
		_ = rcli.Close()
		_ = mc.Close(ctx)
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: http 启动: %w", err)
	}

	stop := func(ctx context.Context) error {
		_ = reg.Deregister(ctx, instance)
		hcancel()
		_ = httpSrv.Stop(ctx)
		gcancel()
		_ = grpcSrv.Stop(ctx)
		_ = rt.Shutdown(ctx)
		_ = rcli.Close()
		_ = mc.Close(ctx)
		return ec.Close()
	}
	return &Game{Runtime: rt, GRPCURL: ep.Host, HTTPURL: httpEP.Host, stop: stop}, nil
}

// Stop 停止 game 服务并释放全部资源。
func (g *Game) Stop(ctx context.Context) error {
	if g.stop == nil {
		return nil
	}
	return g.stop(ctx)
}
