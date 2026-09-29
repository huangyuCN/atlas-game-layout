// Package server 负责 matcher 服务的传输层构造：
// gRPC 双面（Matcher 服务注册在 internal 面）+ HTTP（健康/规则查询）。
// 启动参数来自 server.grpc / server.http 配置（字段见 protobuf/configs/server.proto，
// 缺省约定见 docs/config.md）；中间件与过滤器由 pkg/middleware 经 fx 注入（业务可用 fx.Decorate 追加）；
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态与进程内形态均由 atlas.App 驱动启停）。
package server

import (
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas/transport"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	"go.uber.org/fx"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查 + 规则查询）。
func NewHTTPServer(cfg *conf.Bootstrap,
	mws serverutil.Middlewares, filters serverutil.Filters) (*atlashttp.Server, error) {
	srv, err := serverutil.HTTPServer(cfg.GetServer().GetHttp(), mws, filters)
	if err != nil {
		return nil, err
	}
	// /health 用裸 HandleFunc 注册：探针不进中间件链（不记日志、不打点），避免噪声。
	srv.HandleFunc("/health", serverutil.HealthHandler(cfg.GetRuntime().GetName()))
	return srv, nil
}

// GRPCServerSet 是 matcher 两个 gRPC 面的服务端集合（未启用的面为 nil）：
// 以 transport.Server 进 fx 的 servers 值组，由 atlas.App 统一启停与注册。
type GRPCServerSet struct {
	fx.Out

	Edge     transport.Server `group:"servers"`
	Internal transport.Server `group:"servers"`
}

// NewGRPCServers 构造 matcher 的两个 gRPC 面并注册 Matcher 服务：
// Matcher 是**服务间**调用（game 的 PlayerActor → matcher，battle 直连测试），故注册到
// internal listener；matcher 没有带 route 注解的 service（不存在客户端 op），edge 面按配置
// 留空（不启用）——启用也只会得到一个空面，还会因 reflection 暴露服务清单。
// 两面各自独立地址与生命周期（server.grpc.edge_addr / internal_addr，空 = 不启用该面），
// 都挂 opgrpc 一元拦截器（入站 metadata→ctx、错误→status，挂载点见 serverutil.GRPCServers）。
func NewGRPCServers(cfg *conf.Bootstrap, svc *handler.MatcherHandler,
	mws serverutil.Middlewares) (GRPCServerSet, error) {
	faces, err := serverutil.GRPCServers(cfg.GetServer().GetGrpc(), mws)
	if err != nil {
		return GRPCServerSet{}, err
	}
	if faces.Internal != nil {
		matcherv1.RegisterMatcherServer(faces.Internal, svc)
	}
	return GRPCServerSet{Edge: faces.Edge.Transport(), Internal: faces.Internal.Transport()}, nil
}
