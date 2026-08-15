// Package server 负责 matcher 服务的传输层组装：
// gRPC（入队/取消/查询）+ HTTP（健康/规则查询）+ 依赖装配（infra）与撮合运行时。
package server

import (
	"context"
	"fmt"
	"net/http"

	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/infra"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/matchmaker"
	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查 + 规则查询）。
func NewHTTPServer(cfg *conf.Bootstrap) (transport.Server, error) {
	srv, err := atlashttp.NewServer(atlashttp.WithAddress(cfg.GetServer().GetHttp().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 HTTP 服务端失败: %w", err)
	}
	srv.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"matcher"}`))
	})
	srv.HandleFunc("/v1/matcher/rules", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"rulesets":["%s"],"rule":%q}`, biz.DefaultMatchmakerName, infra.RuleDescription)
	})
	return srv, nil
}

// NewGRPCServer 构造 gRPC 服务端并注册撮合服务。
func NewGRPCServer(cfg *conf.Bootstrap, svc *handler.MatcherHandler) (transport.Server, error) {
	srv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(cfg.GetServer().GetGrpc().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 gRPC 服务端失败: %w", err)
	}
	matcherv1.RegisterMatcherServer(srv, svc)
	return srv, nil
}

// newRedisClient 装配 redis 客户端（映射 + matchmaker 后端）。
func newRedisClient(cfg *conf.Bootstrap) (*pkredis.Client, error) {
	opts := pkredis.Options{}
	if d := cfg.GetData(); d != nil && d.GetRedis() != nil {
		opts.Addr = d.GetRedis().GetAddr()
	}
	return pkredis.NewClient(opts)
}

// newNatsConn 装配 NATS 连接（事件总线）。
func newNatsConn(cfg *conf.Bootstrap) (*natsgo.Conn, error) {
	url := ""
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		url = d.GetNats().GetUrl()
	}
	return nats.Connect(nats.Options{URL: url, Name: "matcher"})
}

// newActorRuntime 装配 actor 集群客户端（开局调用 + 战斗 actor 懒激活副本）。
func newActorRuntime(cfg *conf.Bootstrap, ec *clientv3.Client) (*pkgactor.Runtime, error) {
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
	rt, err := pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        nodeID,
		ServiceName:   consts.ServiceBattle, // 懒激活在 battle 节点执行（M7）
		EtcdEndpoints: endpoints,
		NatsURL:       natsURL,
		Discovery:     discovery,
	})
	if err != nil {
		return nil, fmt.Errorf("server: 构造 actor 运行时失败: %w", err)
	}
	if err := pkgactor.RegisterBattleReplica(rt); err != nil {
		return nil, fmt.Errorf("server: 注册战斗 actor 懒激活副本失败: %w", err)
	}
	return rt, nil
}

// newSink 装配成局观察方（nats 发布 + 开局调用，infra 默认组合）。
func newSink(nc *natsgo.Conn, rt *pkgactor.Runtime) biz.MatchEventSink {
	return infra.NewSink(nc, rt)
}

// newMatchmakerService 暴露撮合运行时对外 API（grpc 依赖）。
func newMatchmakerService(rt *matchredis.Runtime) matchmaker.Service {
	return rt.Service
}

// newMatchmakerHandler 装配撮合 grpc 实现。
func newMatchmakerHandler(svc matchmaker.Service, mapper *infra.RedisPlayerTicketMapper, sink biz.MatchEventSink, deduper *infra.RedisSettleDeduper) *handler.MatcherHandler {
	return handler.NewMatcherHandler(svc, mapper, sink, deduper)
}

// registerActorLifecycle 把 actor 集群客户端接入生命周期。
func registerActorLifecycle(lc fx.Lifecycle, rt *pkgactor.Runtime) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return rt.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return rt.Shutdown(ctx) },
	})
}

// registerRuntime 把撮合运行时接入生命周期（Start tick 循环 / Stop 优雅停止）。
// 注意：tick 循环是长驻 goroutine，不能用 fx OnStart 的 ctx（15s 超时取消会杀掉循环），
// 故以进程级 context 启动；停止经 Matcher.Stop 的 stopCh 通道完成。
func registerRuntime(lc fx.Lifecycle, rt *matchredis.Runtime) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return rt.Start(context.Background()) },
		OnStop:  func(ctx context.Context) error { return rt.Stop(ctx) },
	})
}

// Module 是 matcher 服务的传输层装配模块。
var Module = fx.Module("server",
	fx.Provide(
		fx.Annotate(NewHTTPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(NewGRPCServer, fx.ResultTags(`group:"servers"`)),
		newRedisClient,
		newNatsConn,
		newActorRuntime,
		infra.NewRedisPlayerTicketMapper,
		infra.NewRedisSettleDeduper,
		infra.NewMatchmakerRuntime,
		newMatchmakerService,
		newSink,
		newMatchmakerHandler,
	),
	fx.Invoke(registerRuntime, registerActorLifecycle),
)
