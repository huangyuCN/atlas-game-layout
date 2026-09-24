// Package server 负责 battle 服务的传输层构造：gRPC 双面（域 op 的 rpc/ 平面）+ HTTP（健康检查）。
// 启动参数来自 server.grpc / server.http 配置（字段见 protobuf/configs/server.proto，
// 缺省约定见 docs/config.md）；中间件与过滤器由 pkg/middleware 经 fx 注入（业务可用 fx.Decorate 追加）；
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态与进程内形态均由 atlas.App 驱动启停）。
package server

import (
	battlev1rpc "github.com/huangyuCN/atlas-game-layout/api/battle/v1/rpc"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"
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

// GRPCServerSet 是 battle 两个 gRPC 面的服务端集合（未启用的面为 nil）：
// 以 transport.Server 进 fx 的 servers 值组，由 atlas.App 统一启停与注册。
type GRPCServerSet struct {
	fx.Out

	Edge     transport.Server `group:"servers"`
	Internal transport.Server `group:"servers"`
}

// NewGRPCServers 构造 battle 的两个 gRPC 面并注册域 rpc/ 平面（注册细节见 registerBattleServices）：
// Edge 接口（access=CLIENT：加入对局/帧上行/帧同步，经网关转发）→ edge listener；
// Internal 接口（access=INTERNAL：开局/查询，服务间调用）→ internal listener。
// 两面各自独立地址与生命周期（server.grpc.edge_addr / internal_addr，空 = 不启用该面），
// 都挂 opgrpc 一元拦截器（入站 metadata→ctx、错误→status，挂载点见 serverutil.GRPCServers）；
// 注册错面在编译期即失败（Register...Edge/Internal 的形参类型即本面接口）。
func NewGRPCServers(cfg *conf.Bootstrap, rt *pkgactor.Runtime,
	mws serverutil.Middlewares) (GRPCServerSet, error) {
	faces, err := serverutil.GRPCServers(cfg.GetServer().GetGrpc(), mws)
	if err != nil {
		return GRPCServerSet{}, err
	}
	return registerBattleServices(faces, rt), nil
}

// registerBattleServices 把 BattleService 的两个面实现按面注册并汇成 servers 组的值。
// AdapterOptions 留空：入站身份与观测头已由 opgrpc 拦截器写入 ctx，
// 接入层经 opcall.PlanFromContext 自取，装配期无需再附加投递选项。
func registerBattleServices(faces serverutil.GRPCFaces, rt *pkgactor.Runtime) GRPCServerSet {
	edge, internal := serverutil.RegisterFaces(faces,
		battlev1rpc.NewBattleServiceEdge(rt, opcall.AdapterOptions{}),
		battlev1rpc.NewBattleServiceInternal(rt, opcall.AdapterOptions{}),
		battlev1rpc.RegisterBattleServiceEdge, battlev1rpc.RegisterBattleServiceInternal)
	return GRPCServerSet{Edge: edge, Internal: internal}
}
