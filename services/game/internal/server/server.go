// Package server 负责 game 服务的传输层构造：gRPC 双面（域 op 的 rpc/ 平面）+ HTTP（健康检查）。
// 启动参数来自 server.grpc / server.http 配置（字段见 protobuf/configs/server.proto，
// 缺省约定见 docs/config.md）；中间件与过滤器由 pkg/middleware 经 fx 注入（业务可用 fx.Decorate 追加）；
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态与进程内形态均由 atlas.App 驱动启停）。
package server

import (
	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	gamev1rpc "github.com/huangyuCN/atlas-game-layout/api/game/v1/rpc"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"
	"github.com/huangyuCN/atlas/transport"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	"go.uber.org/fx"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查）。
// 旧的玩家 REST 管理接口（service Player 的 google.api.http 注解）已删除，管理面归 P7。
func NewHTTPServer(cfg *conf.Bootstrap,
	mws serverutil.Middlewares, filters serverutil.Filters) (*atlashttp.Server, error) {
	srv, err := serverutil.HTTPServer(cfg.GetServer().GetHttp(), mws, filters)
	if err != nil {
		return nil, err
	}
	srv.HandleFunc("/health", serverutil.HealthHandler(cfg.GetRuntime().GetName()))
	return srv, nil
}

// GRPCServerSet 是 game 两个 gRPC 面的服务端集合（未启用的面为 nil）：
// 以 transport.Server 进 fx 的 servers 值组，由 atlas.App 统一启停与注册。
type GRPCServerSet struct {
	fx.Out

	Edge     transport.Server `group:"servers"`
	Internal transport.Server `group:"servers"`
}

// NewGRPCServers 构造 game 的两个 gRPC 面并注册域 rpc/ 平面与管理面（注册细节见 registerPlayerServices）：
// Edge 接口（access=CLIENT，客户端 op 经网关转发）→ edge listener；
// Internal 接口（access=INTERNAL，服务间调用）+ 管理面 AdminService → internal listener。
// 两面各自独立地址与生命周期（server.grpc.edge_addr / internal_addr，空 = 不启用该面），
// 都挂 opgrpc 一元拦截器（入站 metadata→ctx、错误→status，挂载点见 serverutil.GRPCServers）；
// 注册错面在编译期即失败（Register...Edge/Internal 的形参类型即本面接口）。
func NewGRPCServers(cfg *conf.Bootstrap, rt *pkgactor.Runtime,
	mws serverutil.Middlewares, admin biz.AdminService) (GRPCServerSet, error) {
	faces, err := serverutil.GRPCServers(cfg.GetServer().GetGrpc(), mws)
	if err != nil {
		return GRPCServerSet{}, err
	}
	return registerPlayerServices(faces, rt, admin), nil
}

// registerPlayerServices 把 PlayerService 的两个面实现按面注册、把管理面注册到 internal 面，
// 并汇成 servers 组的值。
//
// 安全边界（钉死）：AdminService **只在 internal listener 上注册**——管理面是内网面
// （GM/运维工具可达），edge 面（不可信区、客户端 op 经网关转发）零管理面方法。
// 管理面本轮无鉴权，端口隔离就是唯一信任边界，不得为图方便把它挂到 edge 面。
func registerPlayerServices(faces serverutil.GRPCFaces, rt *pkgactor.Runtime, admin biz.AdminService) GRPCServerSet {
	// AdapterOptions 留空：入站身份与观测头已由 opgrpc 拦截器写入 ctx，
	// 接入层经 opcall.PlanFromContext 自取，装配期无需再附加投递选项。
	edge, internal := serverutil.RegisterFaces(faces,
		gamev1rpc.NewPlayerServiceEdge(rt, opcall.AdapterOptions{}),
		gamev1rpc.NewPlayerServiceInternal(rt, opcall.AdapterOptions{}),
		gamev1rpc.RegisterPlayerServiceEdge, gamev1rpc.RegisterPlayerServiceInternal)
	if faces.Internal != nil && admin != nil {
		admingamev1.RegisterAdminServiceServer(faces.Internal, admin)
	}
	return GRPCServerSet{Edge: edge, Internal: internal}
}
