// Package server 负责 battle 服务的传输层组装：
// gRPC（开局/查询）+ HTTP（健康/管理）+ 依赖装配（infra）与 BattleActor 注册。
package server

import (
	"context"
	"fmt"
	"net/http"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/repo"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/infra"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查 + 管理接口）。
func NewHTTPServer(cfg *conf.Bootstrap) (transport.Server, error) {
	srv, err := atlashttp.NewServer(atlashttp.WithAddress(cfg.GetServer().GetHttp().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 HTTP 服务端失败: %w", err)
	}
	srv.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"battle"}`))
	})
	return srv, nil
}

// NewGRPCServer 构造 gRPC 服务端并注册战斗管理服务。
func NewGRPCServer(cfg *conf.Bootstrap, svc *handler.BattleHandler) (transport.Server, error) {
	srv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(cfg.GetServer().GetGrpc().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 gRPC 服务端失败: %w", err)
	}
	battlev1.RegisterBattleServer(srv, svc)
	return srv, nil
}

// NewMongoClient 装配 MongoDB 客户端（结算落库）。
func NewMongoClient(cfg *conf.Bootstrap) (*mongo.Client, error) {
	opts := mongo.Options{}
	if d := cfg.GetData(); d != nil && d.GetMongo() != nil {
		opts.URI = d.GetMongo().GetUri()
		opts.Database = d.GetMongo().GetDatabase()
	}
	return mongo.NewClient(context.Background(), opts)
}

// NewResultRepo 装配结算结果仓储。
func NewResultRepo(cli *mongo.Client) (repo.ResultRepo, error) {
	return repo.NewMongoResultRepo(cli), nil
}

// NewActorRuntime 装配 actor 集群运行时（战斗 actor 宿主）。
func NewActorRuntime(cfg *conf.Bootstrap, ec *clientv3.Client) (*pkgactor.Runtime, error) {
	var endpoints []string
	if r := cfg.GetRegistry(); r != nil && r.GetEtcd() != nil {
		endpoints = r.GetEtcd().GetEndpoints()
	}
	natsURL := ""
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		natsURL = d.GetNats().GetUrl()
	}
	nodeID := ""
	if r := cfg.GetRuntime(); r != nil {
		nodeID = r.GetId()
	}
	discovery, err := pkgregistry.NewEtcdDiscovery(ec, pkgregistry.Options{})
	if err != nil {
		return nil, fmt.Errorf("server: 构造服务发现失败: %w", err)
	}
	return pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        nodeID,
		ServiceName:   consts.ServiceBattle,
		EtcdEndpoints: endpoints,
		NatsURL:       natsURL,
		Discovery:     discovery,
	})
}

// NewPubSub 装配帧广播 pubsub（teller 为本地运行时）。
func NewPubSub(rt *pkgactor.Runtime) (*pubsub.Registry, error) {
	reg, err := pubsub.New(rt.Raw().Local())
	if err != nil {
		return nil, fmt.Errorf("server: 构造 pubsub 失败: %w", err)
	}
	return reg, nil
}

// NewBattleHandler 装配战斗管理 grpc 实现（actor 转发）。
func NewBattleHandler(rt *pkgactor.Runtime) *handler.BattleHandler {
	return handler.NewBattleHandler(rt)
}

// registerActor 注册 BattleActor 并接入生命周期（含帧广播/结算组件）。
func registerActor(
	lc fx.Lifecycle,
	rt *pkgactor.Runtime,
	reg *pubsub.Registry,
	resultRepo repo.ResultRepo,
	nc *natsgo.Conn,
) error {
	notifier := infra.NewNatsBattleNotifier(nc)
	publisher := infra.NewNatsSettlePublisher(nc)
	err := rt.Register(actor.NewRuntimeProps(actor.DefaultDeps{
		Rt:         rt,
		Registry:   reg,
		ResultRepo: resultRepo,
		Notifier:   notifier,
		Publisher:  publisher,
	}, actor.DefaultConfig()))
	if err != nil {
		return fmt.Errorf("server: 注册 BattleActor 失败: %w", err)
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return rt.Start(ctx) },
		OnStop: func(ctx context.Context) error {
			reg.Close()
			return rt.Shutdown(ctx)
		},
	})
	return nil
}

// Module 是 battle 服务的传输层装配模块。
var Module = fx.Module("server",
	fx.Provide(
		fx.Annotate(NewHTTPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(NewGRPCServer, fx.ResultTags(`group:"servers"`)),
		infra.NewNatsConn,
		NewMongoClient,
		NewResultRepo,
		NewActorRuntime,
		NewPubSub,
		NewBattleHandler,
	),
	fx.Invoke(registerActor),
)
