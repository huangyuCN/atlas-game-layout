// Package server 负责 matcher 服务的传输层构造：
// gRPC（入队/取消/查询）+ HTTP（健康/规则查询）。
// 启动参数来自 server.grpc / server.http 配置（字段见 protobuf/configs/server.proto）；
// 中间件与过滤器由 pkg/middleware 经 fx 注入（业务可用 fx.Decorate 追加）；
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态与进程内形态均由 atlas.App 驱动启停）。
package server

import (
	"context"
	"net/http"

	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/infra"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
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
	// 管理接口走 Route + ctx.Middleware：与生成代码同构，因此享有中间件链。
	srv.Route("/").GET("/v1/matcher/rules", rulesHandler())
	return srv, nil
}

// rulesHandler 返回撮合规则查询处理器（响应经 atlas 响应编码器）。
func rulesHandler() func(ctx atlashttp.Context) error {
	return func(ctx atlashttp.Context) error {
		h := ctx.Middleware(func(context.Context, interface{}) (interface{}, error) {
			return map[string]any{
				"rulesets": []string{biz.DefaultMatchmakerName},
				"rule":     infra.RuleDescription,
			}, nil
		})
		out, err := h(ctx, nil)
		if err != nil {
			return err
		}
		return ctx.Result(http.StatusOK, out)
	}
}

// NewGRPCServer 构造 gRPC 服务端并注册撮合服务。
func NewGRPCServer(cfg *conf.Bootstrap, svc *handler.MatcherHandler,
	mws serverutil.Middlewares) (*atlasgrpc.Server, error) {
	srv, err := serverutil.GRPCServer(cfg.GetServer().GetGrpc(), mws)
	if err != nil {
		return nil, err
	}
	matcherv1.RegisterMatcherServer(srv, svc)
	return srv, nil
}
