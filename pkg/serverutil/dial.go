package serverutil

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas/contrib/actor/opgrpc"
	"github.com/huangyuCN/atlas/registry"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	"google.golang.org/grpc"
)

// DialDomain 拨号域服务（按注册中心服务名 + 面 scheme 选端点），并在一处挂载
// 客户端中间件链与 opgrpc 客户端拦截器。
//
// 为什么收口成一个函数：域 `rpc/` 平面的跨服务调用必须同时满足两件事——
// ① 拨到正确的面（见下 scheme 参数）；② 身份三键写出与远端错误还原
// （`opgrpc.UnaryClientInterceptor`）。逐处手写拨号极易漏挂拦截器，而漏挂的表现是
// 「身份静默丢失、reason 变 Unknown」，排查成本极高，故**面向域 `rpc/` 平面的调用**
// 统一走本函数（非 actor 契约的拨号——如 game→matcher 的管理面、`gmctl`——仍自行拨号，
// 它们的错误还原由 `transport/grpc` 的入站接缝兜底）。
//
// scheme 取 SchemeGRPC（internal 面：服务间调用 + 管理面）或 SchemeGRPCEdge
// （edge 面：客户端 op 经网关转发到域 rpc/ 平面的 Edge 接口）。
// 两面端口隔离即信任边界：客户端 op 只能落到 edge 面，内部方法只在 internal 面。
func DialDomain(ctx context.Context, discovery registry.Discovery, service, scheme string, mws ClientMiddlewares) (*grpc.ClientConn, error) {
	opts := []atlasgrpc.ClientOption{
		atlasgrpc.WithDiscovery(discovery),
		atlasgrpc.WithEndpoint("discovery:///" + service),
		atlasgrpc.WithEndpointScheme(scheme),
		atlasgrpc.WithUnaryInterceptor(opgrpc.UnaryClientInterceptor()),
	}
	if len(mws) > 0 {
		opts = append(opts, atlasgrpc.WithMiddleware(mws...))
	}
	conn, err := atlasgrpc.DialInsecure(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("serverutil: 拨号域服务 %s（%s 面）失败: %w", service, scheme, err)
	}
	return conn, nil
}
