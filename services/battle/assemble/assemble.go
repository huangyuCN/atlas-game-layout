// Package assemble 提供 battle 服务的可编程装配入口：
// 供集成测试、工具链与将来「嵌入式部署」复用（fx 模块化装配的进程内形态，D2）。
package assemble

import (
	"context"
	"fmt"
	"net/url"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	battleactor "github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/repo"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/infra"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	"github.com/huangyuCN/atlas/registry"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// BattleConfig 是战斗参数覆盖（供 e2e 等外部包注入；镜像 internal/actor.Config——
// Go internal 规则禁止外部包直接引用 actor.Config，故以本包类型暴露，toActor 单点映射）。
type BattleConfig struct {
	TickInterval  int64  // 帧间隔（纳秒）
	TrackLen      int32  // 赛道长度（胜负判定）
	MaxFrames     uint64 // 帧数上限
	SnapshotEvery uint64 // 快照周期（帧）
}

// toActor 映射为 actor.Config（字段镜像的唯一转换点）。
func (c BattleConfig) toActor() battleactor.Config {
	return battleactor.Config{
		TickInterval:  c.TickInterval,
		TrackLen:      c.TrackLen,
		MaxFrames:     c.MaxFrames,
		SnapshotEvery: c.SnapshotEvery,
	}
}

// DefaultBattleConfig 返回默认战斗参数（完整填充，可局部覆盖）。
func DefaultBattleConfig() BattleConfig {
	cfg := battleactor.DefaultConfig()
	return BattleConfig{
		TickInterval:  cfg.TickInterval,
		TrackLen:      cfg.TrackLen,
		MaxFrames:     cfg.MaxFrames,
		SnapshotEvery: cfg.SnapshotEvery,
	}
}

// Options 是 battle 服务的装配参数。
type Options struct {
	NodeID        string
	EtcdEndpoints []string
	NatsURL       string
	MongoURI      string
	MongoDB       string
	GRPCAddr      string // 空则 127.0.0.1:0（随机端口）
	// BattleCfg 覆盖战斗默认参数（测试注入：短帧间隔/长赛道等）；nil 用默认。
	BattleCfg *BattleConfig
}

// Battle 是装配完成的 battle 服务句柄。
type Battle struct {
	Runtime *pkgactor.Runtime // 战斗 actor 宿主（测试/观测可直达）
	GRPCURL string
	stop    func(ctx context.Context) error
}

// New 装配并启动 battle 服务（actor 集群运行时 + BattleActor + gRPC）。
func New(ctx context.Context, opts Options) (*Battle, error) {
	if opts.GRPCAddr == "" {
		opts.GRPCAddr = "127.0.0.1:0"
	}
	backends, err := newBackends(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err := registerBattleActor(backends, opts.BattleCfg); err != nil {
		backends.close()
		return nil, err
	}
	if err := backends.rt.Start(ctx); err != nil {
		backends.close()
		return nil, fmt.Errorf("assemble: actor 启动: %w", err)
	}

	svc := handler.NewBattleHandler(backends.rt)
	grpcSrv, ep, grpcStop, err := startGRPC(ctx, opts.GRPCAddr, svc)
	if err != nil {
		_ = backends.rt.Shutdown(ctx)
		backends.close()
		return nil, err
	}
	reg, instance, err := registerService(ctx, backends.ec, opts.NodeID, ep.Host)
	if err != nil {
		grpcStop()
		_ = backends.rt.Shutdown(ctx)
		backends.close()
		return nil, err
	}

	stop := func(ctx context.Context) error {
		_ = reg.Deregister(ctx, instance)
		grpcStop()
		_ = grpcSrv.Stop(ctx)
		backends.reg.Close()
		_ = backends.rt.Shutdown(ctx)
		return backends.close()
	}
	return &Battle{Runtime: backends.rt, GRPCURL: ep.Host, stop: stop}, nil
}

// Stop 停止 battle 服务并释放全部资源。
func (b *Battle) Stop(ctx context.Context) error {
	if b.stop == nil {
		return nil
	}
	return b.stop(ctx)
}

// backends 是底层资源句柄集合（关闭顺序：actor → etcd → nats → mongo）。
type backends struct {
	ec  *clientv3.Client
	nc  *natsgo.Conn
	mc  *pkgmongo.Client
	rt  *pkgactor.Runtime
	reg *pubsub.Registry
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
	if b.mc != nil {
		return b.mc.Close(context.Background())
	}
	return nil
}

// newBackends 建立 etcd/nats/mongo 与 actor 集群运行时。
func newBackends(ctx context.Context, opts Options) (*backends, error) {
	ec, err := etcd.NewClient(etcd.Options{Endpoints: opts.EtcdEndpoints})
	if err != nil {
		return nil, fmt.Errorf("assemble: etcd: %w", err)
	}
	nc, err := nats.Connect(nats.Options{URL: opts.NatsURL, Name: "battle"})
	if err != nil {
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: nats: %w", err)
	}
	mc, err := pkgmongo.NewClient(ctx, pkgmongo.Options{URI: opts.MongoURI, Database: opts.MongoDB})
	if err != nil {
		nc.Close()
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: mongo: %w", err)
	}
	disc, err := pkgregistry.NewEtcdDiscovery(ec, pkgregistry.Options{})
	if err != nil {
		_ = mc.Close(ctx)
		nc.Close()
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: 服务发现: %w", err)
	}
	rt, err := pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        opts.NodeID,
		ServiceName:   consts.ServiceBattle,
		EtcdEndpoints: opts.EtcdEndpoints,
		NatsURL:       opts.NatsURL,
		Discovery:     disc,
	})
	if err != nil {
		_ = mc.Close(ctx)
		nc.Close()
		_ = ec.Close()
		return nil, fmt.Errorf("assemble: actor 运行时: %w", err)
	}
	return &backends{ec: ec, nc: nc, mc: mc, rt: rt}, nil
}

// registerBattleActor 注册 BattleActor（下行通知/结算组件 + 战斗参数）。
func registerBattleActor(b *backends, cfgOverride *BattleConfig) error {
	resultRepo := repo.NewMongoResultRepo(b.mc)
	reg, err := pubsub.New(b.rt.Raw().Local())
	if err != nil {
		return fmt.Errorf("assemble: pubsub: %w", err)
	}
	cfg := battleactor.DefaultConfig()
	if cfgOverride != nil {
		cfg = cfgOverride.toActor()
	}
	notifier := infra.NewNatsBattleNotifier(b.nc)
	publisher := infra.NewNatsSettlePublisher(b.nc)
	err = b.rt.Register(battleactor.NewRuntimeProps(battleactor.DefaultDeps{
		Rt:         b.rt,
		Registry:   reg,
		ResultRepo: resultRepo,
		Notifier:   notifier,
		Publisher:  publisher,
	}, cfg))
	if err != nil {
		reg.Close()
		return fmt.Errorf("assemble: 注册 BattleActor: %w", err)
	}
	b.reg = reg
	return nil
}

// startGRPC 装配并启动 grpc 服务，返回停止函数与端点。
func startGRPC(ctx context.Context, addr string, svc *handler.BattleHandler) (*atlasgrpc.Server, *url.URL, func(), error) {
	grpcSrv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(addr))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("assemble: grpc: %w", err)
	}
	battlev1.RegisterBattleServer(grpcSrv, svc)
	gctx, gcancel := context.WithCancel(context.Background())
	// Atlas gRPC Server.Start 为阻塞式 serve，进程内装配需后台启动并轮询就绪。
	go func() { _ = grpcSrv.Start(gctx) }()
	ep, err := serverutil.WaitEndpoint(grpcSrv, 2*time.Second)
	if err != nil {
		gcancel()
		return nil, nil, nil, fmt.Errorf("assemble: grpc 启动: %w", err)
	}
	return grpcSrv, ep, gcancel, nil
}

// registerService 注册 battle 服务实例（实例 ID 必须与 actor NodeID 一致，
// 供 matcher 懒激活按服务发现选中本节点）。
func registerService(ctx context.Context, ec *clientv3.Client, nodeID, grpcHost string) (registry.Registrar, *registry.ServiceInstance, error) {
	reg, err := pkgregistry.NewEtcd(ec, pkgregistry.Options{})
	if err != nil {
		return nil, nil, fmt.Errorf("assemble: 注册中心: %w", err)
	}
	instance := &registry.ServiceInstance{
		ID:        nodeID,
		Name:      consts.ServiceBattle,
		Version:   "0.1.0",
		Endpoints: []string{"grpc://" + grpcHost},
	}
	if err := reg.Register(ctx, instance); err != nil {
		return nil, nil, fmt.Errorf("assemble: 注册服务: %w", err)
	}
	return reg, instance, nil
}
