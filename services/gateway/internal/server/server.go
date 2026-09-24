// Package server 负责 gateway 服务的传输层构造与统一 handler：
// 五协议 Server（tcp/ws/kcp/udp/http）+ gRPC edge 面 + 会话绑定 + 挤下线 + 下行推送。
// 启动参数来自 server.http / server.grpc 配置（字段见 protobuf/configs/server.proto，
// 缺省约定见 docs/config.md）；
// 中间件与过滤器由 pkg/middleware 经 fx 注入（业务可用 fx.Decorate 追加）；
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态与进程内形态均由 atlas.App 驱动启停）。
package server

import (
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas/transport"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	"go.uber.org/fx"
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

// GRPCServerSet 是 gateway 两个 gRPC 面的服务端集合（未启用的面为 nil）：
// 以 transport.Server 进 fx 的 servers 值组，由 atlas.App 统一启停与注册。
type GRPCServerSet struct {
	fx.Out

	Edge     transport.Server `group:"servers"`
	Internal transport.Server `group:"servers"`
}

// NewGRPCServers 构造 gateway 的 gRPC 双面：本轮**只启用 edge 面**且暂不注册域服务
// （客户端 op 走 tcp/ws/kcp/udp 四协议通道与 actor 平面转发），internal 面留空（不启用）——
// 按 server.grpc.edge_addr / internal_addr 的配置判定，空 = 不启用该面（缺省约定见 docs/config.md）。
// 两面都挂 opgrpc 一元拦截器（挂载点见 serverutil.GRPCServers），P7 管理面再往 internal 面注册。
func NewGRPCServers(cfg *conf.Bootstrap, mws serverutil.Middlewares) (GRPCServerSet, error) {
	faces, err := serverutil.GRPCServers(cfg.GetServer().GetGrpc(), mws)
	if err != nil {
		return GRPCServerSet{}, err
	}
	return GRPCServerSet{Edge: faces.Edge.Transport(), Internal: faces.Internal.Transport()}, nil
}
