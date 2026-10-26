package serverutil

import (
	"context"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas/contrib/actor/opgrpc"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	"google.golang.org/grpc"
)

// DialOptions 汇总 DialDomain 的装配项（跨服务调用的共性关注点：预算与选点）。
type dialOptions struct {
	// timeout 是单次调用预算；0 = 沿用框架缺省（不覆盖）。
	timeout time.Duration
	// filters 是节点选择过滤器（如本机优先，见 framework selector/filter）。
	filters []selector.NodeFilter
}

// DialOption 配置 DialDomain 的装配项。
type DialOption func(*dialOptions)

// WithRPCTimeout 设置单次调用预算（只放大不缩小：框架缺省已对齐 5s 链路预算）。
func WithRPCTimeout(timeout time.Duration) DialOption {
	return func(o *dialOptions) { o.timeout = timeout }
}

// WithNodeFilters 挂载节点选择过滤器（按调用生效，用于本机优先等选点策略）。
func WithNodeFilters(filters ...selector.NodeFilter) DialOption {
	return func(o *dialOptions) { o.filters = append(o.filters, filters...) }
}

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
func DialDomain(ctx context.Context, discovery registry.Discovery, service, scheme string, mws ClientMiddlewares, dialOpts ...DialOption) (*grpc.ClientConn, error) {
	o := dialOptions{}
	for _, opt := range dialOpts {
		opt(&o)
	}
	opts := []atlasgrpc.ClientOption{
		atlasgrpc.WithDiscovery(discovery),
		atlasgrpc.WithEndpoint("discovery:///" + service),
		atlasgrpc.WithEndpointScheme(scheme),
		atlasgrpc.WithUnaryInterceptor(opgrpc.UnaryClientInterceptor()),
	}
	// 预算只显式传：不传即沿用框架缺省（DefaultClientTimeout），由框架一处维护口径。
	if o.timeout > 0 {
		opts = append(opts, atlasgrpc.WithTimeout(o.timeout))
	}
	if len(o.filters) > 0 {
		opts = append(opts, atlasgrpc.WithNodeFilter(o.filters...))
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
