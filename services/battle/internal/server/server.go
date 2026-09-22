// Package server 负责 battle 服务的传输层构造：
// gRPC（开局/查询/帧上行）+ HTTP（健康检查）。
// 启动参数来自 server.grpc / server.http 配置（字段见 protobuf/configs/server.proto）；
// 中间件与过滤器由 pkg/middleware 经 fx 注入（业务可用 fx.Decorate 追加）；
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态 atlas.App / 进程内形态 serverutil.ServeAsync）。
package server

import (
	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查）。
func NewHTTPServer(cfg *conf.Bootstrap,
	mws serverutil.Middlewares, filters serverutil.Filters) (*atlashttp.Server, error) {
	srv, err := serverutil.HTTPServer(cfg.GetServer().GetHttp(), mws, filters)
	if err != nil {
		return nil, err
	}
	srv.HandleFunc("/health", serverutil.HealthHandler(cfg.GetRuntime().GetName()))
	return srv, nil
}

// NewGRPCServer 构造 gRPC 服务端并注册战斗管理服务。
func NewGRPCServer(cfg *conf.Bootstrap, svc *handler.BattleHandler,
	mws serverutil.Middlewares) (*atlasgrpc.Server, error) {
	srv, err := serverutil.GRPCServer(cfg.GetServer().GetGrpc(), mws)
	if err != nil {
		return nil, err
	}
	battlev1.RegisterBattleServer(srv, svc)
	return srv, nil
}
