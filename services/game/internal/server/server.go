// Package server 负责 game 服务的传输层构造：
// gRPC（玩家业务）+ HTTP（健康检查 + 玩家 REST 管理接口）。
// 启动参数来自 server.grpc / server.http 配置（字段见 protobuf/configs/server.proto）；
// 中间件与过滤器由 pkg/middleware 经 fx 注入（业务可用 fx.Decorate 追加）；
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态与进程内形态均由 atlas.App 驱动启停）。
package server

import (
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查 + 玩家业务管理接口）。
func NewHTTPServer(cfg *conf.Bootstrap, svc *handler.GameHandler,
	mws serverutil.Middlewares, filters serverutil.Filters) (*atlashttp.Server, error) {
	srv, err := serverutil.HTTPServer(cfg.GetServer().GetHttp(), mws, filters)
	if err != nil {
		return nil, err
	}
	srv.HandleFunc("/health", serverutil.HealthHandler(cfg.GetRuntime().GetName()))
	gamev1.RegisterPlayerHTTPServer(srv, svc)
	return srv, nil
}

// NewGRPCServer 构造 gRPC 服务端并注册玩家业务服务。
func NewGRPCServer(cfg *conf.Bootstrap, svc *handler.GameHandler,
	mws serverutil.Middlewares) (*atlasgrpc.Server, error) {
	srv, err := serverutil.GRPCServer(cfg.GetServer().GetGrpc(), mws)
	if err != nil {
		return nil, err
	}
	gamev1.RegisterPlayerServer(srv, svc)
	return srv, nil
}
