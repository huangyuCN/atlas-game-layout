// Package server 负责 edge 服务的传输层构造：HTTP 健康检查
// （接入层对外只有裸 L4 面 + 健康面，不注册 gRPC）。
// 启动参数来自 server.http 配置（字段见 protobuf/configs/server.proto，缺省约定见 docs/config.md）；
// 中间件与过滤器由 pkg/middleware 经 fx 注入；依赖装配见 internal/app；
// 服务启停归属驱动方（进程形态与进程内形态均由 atlas.App 驱动启停）。
package server

import (
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查；指标由 observability.metrics.prometheus 端点暴露）。
func NewHTTPServer(cfg *conf.Bootstrap,
	mws serverutil.Middlewares, filters serverutil.Filters) (*atlashttp.Server, error) {
	srv, err := serverutil.HTTPServer(cfg.GetServer().GetHttp(), mws, filters)
	if err != nil {
		return nil, err
	}
	srv.HandleFunc("/health", serverutil.HealthHandler(cfg.GetRuntime().GetName()))
	return srv, nil
}
