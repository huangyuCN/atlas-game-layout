// Package server 负责 gateway 服务的传输层构造与统一 handler：
// 五协议 Server（tcp/ws/kcp/udp/http）+ 会话绑定 + 挤下线 + 下行推送。
// 启动参数来自 server.http 配置（字段见 protobuf/configs/server.proto）；
// 中间件与过滤器由 pkg/middleware 经 fx 注入（业务可用 fx.Decorate 追加）；
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态 atlas.App / 进程内形态 serverutil.ServeAsync + WS 的 /ws 包装）。
package server

import (
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
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
