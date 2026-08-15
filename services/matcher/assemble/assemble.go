// Package assemble 提供 matcher 服务的可编程装配入口：
// 供集成测试、工具链与将来「嵌入式部署」复用（fx 模块化装配的进程内形态，D2）。
package assemble

import (
	"context"
	"fmt"
	"net/url"
	"time"

	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/infra"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/matchmaker"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Options 是 matcher 服务的装配参数。
type Options struct {
	NodeID        string
	EtcdEndpoints []string
	NatsURL       string
	RedisAddr     string
	GRPCAddr      string // 空则 127.0.0.1:0（随机端口）
	// SinkOverride 是成局观察方的测试注入点（nil 时用默认组合：nats 发布 + actor 开局）。
	SinkOverride biz.MatchEventSink
}

// Matcher 是装配完成的 matcher 服务句柄。
type Matcher struct {
	GRPCURL string
	Service matchmaker.Service // 撮合运行时 API（测试/观测用）
	stop    func(ctx context.Context) error
}

// New 装配并启动 matcher 服务（撮合运行时 + grpc + actor 集群客户端）。
func New(ctx context.Context, opts Options) (*Matcher, error) {
	if opts.GRPCAddr == "" {
		opts.GRPCAddr = "127.0.0.1:0"
	}
	backends, err := newBackends(ctx, opts)
	if err != nil {
		return nil, err
	}
	mm, err := startMatchmaker(ctx, backends.cli)
	if err != nil {
		backends.close()
		return nil, err
	}
	sink := opts.SinkOverride
	if sink == nil {
		sink = infra.NewSink(backends.nc, backends.rt)
	}
	svc := handler.NewMatcherHandler(
		mm.Service,
		infra.NewRedisPlayerTicketMapper(backends.cli),
		sink,
		infra.NewRedisSettleDeduper(backends.cli),
	)
	grpcSrv, ep, grpcStop, err := startGRPC(ctx, opts.GRPCAddr, svc)
	if err != nil {
		_ = mm.Stop(ctx)
		backends.close()
		return nil, err
	}
	stop := func(ctx context.Context) error {
		grpcStop()
		_ = grpcSrv.Stop(ctx)
		_ = mm.Stop(ctx)
		return backends.close()
	}
	return &Matcher{GRPCURL: ep.Host, Service: mm.Service, stop: stop}, nil
}

// Stop 停止 matcher 服务并释放全部资源。
func (m *Matcher) Stop(ctx context.Context) error {
	if m.stop == nil {
		return nil
	}
	return m.stop(ctx)
}

// backends 是底层资源句柄集合（关闭顺序：actor → etcd → nats → redis）。
type backends struct {
	cli *pkredis.Client
	nc  *natsgo.Conn
	ec  *clientv3.Client
	rt  *pkgactor.Runtime
}

// close 释放全部底层资源。
func (b *backends) close() error {
	if b.rt != nil {
		_ = b.rt.Shutdown(context.Background())
	}
	if b.ec != nil {
		_ = b.ec.Close()
	}
	if b.nc != nil {
		b.nc.Close()
	}
	if b.cli != nil {
		return b.cli.Close()
	}
	return nil
}

// newBackends 建立 redis/nats/etcd 与 actor 集群客户端。
func newBackends(ctx context.Context, opts Options) (*backends, error) {
	cli, err := pkredis.NewClient(pkredis.Options{Addr: opts.RedisAddr})
	if err != nil {
		return nil, fmt.Errorf("assemble: redis: %w", err)
	}
	nc, err := nats.Connect(nats.Options{URL: opts.NatsURL, Name: "matcher"})
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
		NodeID:        opts.NodeID,
		ServiceName:   consts.ServiceBattle, // 懒激活在 battle 节点执行（M7）
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
	if err := pkgactor.RegisterBattleReplica(rt); err != nil {
		_ = rt.Shutdown(ctx)
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: 懒激活副本: %w", err)
	}
	if err := rt.Start(ctx); err != nil {
		_ = ec.Close()
		nc.Close()
		_ = cli.Close()
		return nil, fmt.Errorf("assemble: actor 启动: %w", err)
	}
	return &backends{cli: cli, nc: nc, ec: ec, rt: rt}, nil
}

// startMatchmaker 装配并启动撮合运行时。
func startMatchmaker(ctx context.Context, cli *pkredis.Client) (*matchredis.Runtime, error) {
	mm, err := infra.NewMatchmakerRuntime(cli)
	if err != nil {
		return nil, fmt.Errorf("assemble: 撮合运行时: %w", err)
	}
	if err := mm.Start(ctx); err != nil {
		return nil, fmt.Errorf("assemble: 撮合启动: %w", err)
	}
	return mm, nil
}

// startGRPC 装配并启动 grpc 服务，返回停止函数与端点。
func startGRPC(ctx context.Context, addr string, svc *handler.MatcherHandler) (*atlasgrpc.Server, *url.URL, func(), error) {
	grpcSrv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(addr))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("assemble: grpc: %w", err)
	}
	matcherv1.RegisterMatcherServer(grpcSrv, svc)
	gctx, gcancel := context.WithCancel(context.Background())
	go func() { _ = grpcSrv.Start(gctx) }()
	ep, err := serverutil.WaitEndpoint(grpcSrv, 2*time.Second)
	if err != nil {
		gcancel()
		return nil, nil, nil, fmt.Errorf("assemble: grpc 启动: %w", err)
	}
	return grpcSrv, ep, gcancel, nil
}
