// Package app 是 edge 服务的唯一装配之家：
// Module 列出全部组件清单（infra → resolver → server 分层），
// 进程形态（cmd/main + atlas App 驱动启停）与进程内形态
// （assemble 经 bootstrap.Boot 驱动启停）共用同一张依赖图。
//
// 与四业务服务的差别：接入层是**裸 L4 转发器**，不注册 gRPC 双面、不持有 actor 运行时，
// 只启「各接入面监听（tcp/udp）+ HTTP 健康」；实例无状态，可水平扩多实例（规格 §2.1）。
package app

import (
	"context"
	"errors"
	"net/url"

	"github.com/huangyuCN/atlas-game-layout/pkg/fxkit"
	"github.com/huangyuCN/atlas-game-layout/pkg/middleware"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/guard"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/server"
	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	"go.uber.org/fx"
)

// Module 是 edge 服务的完整装配清单。
// 依赖来源（*conf.Bootstrap / *clientv3.Client / registry.Discovery）由驱动方供给；
// 接入层实例与 HTTP 服务端以 transport.Server 进 servers 值组，由 atlas.App 统一启停
// （停机顺序即「先停新连接、再拆流」：监听器先关，随后接入层自身 drain）。
var Module = fx.Module("edge",
	// 中间件/过滤器默认链（业务可用 fx.Decorate 追加自己的）。
	middleware.Module,
	fx.Provide(
		// ── infra：注册中心 + actor 目录（只读）+ 帧面实例发现 ──
		fxkit.NewEtcdClient[*conf.Bootstrap],
		fxkit.NewEtcdDiscovery[*conf.Bootstrap],
		newActorDirectory,
		newFrameRegistry,
		directoryOf, // *resolver.LocatorDirectory → resolver.Directory
		framesOf,    // *resolver.DiscoveryRegistry → resolver.FrameRegistry
		newResolver,
		resolverOf,     // *resolver.Resolver → edge.Resolver
		newStreamGuard, // 属主变更守卫（目录 watch → 拆流，规格 §8）
		guardOf,        // *guard.Guard → edge.BackendGuard
		// ── server：接入层（L4，多面监听）+ HTTP 健康 ──
		newProxyOut,
		fx.Annotate(server.NewHTTPServer, fx.As(new(transport.Server)), fx.ResultTags(`group:"servers"`)),
	),
	fx.Invoke(closeGuard),
)

// closeGuard 把守卫的目录监听接入生命周期：停机时统一取消（不留后台 watch goroutine）。
func closeGuard(lc fx.Lifecycle, g *guard.OwnerGuard) {
	if g == nil {
		return
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		g.Close()
		return nil
	}})
}

// ProxyOut 把接入层实例同时暴露为具体类型（驱动方读监听地址）与 servers 组成员。
type ProxyOut struct {
	fx.Out

	// Server 是进 fx servers 值组的传输层服务端（由 atlas.App 启停）。
	Server transport.Server `group:"servers"`
	// Proxy 是具体类型句柄（进程内形态读取各面实际监听地址）。
	Proxy *edge.Proxy
}

// newProxyOut 装配接入层并以其两种形态对外暴露。
func newProxyOut(cfg *conf.Bootstrap, res edge.Resolver, collector metrics.Collector,
	g edge.BackendGuard) (ProxyOut, error) {
	proxy, err := newProxy(cfg, res, collector, g)
	if err != nil {
		return ProxyOut{}, err
	}
	return ProxyOut{
		Server: newProxyServer(proxy, cfg.GetEdge().GetListeners()),
		Proxy:  proxy,
	}, nil
}

// newProxyServer 把接入层包装为「传输层服务端 + 端点」：主面取首个 tcp 面（WS 面）。
func newProxyServer(proxy *edge.Proxy, faces []*conf.Edge_Listener) *proxyServer {
	primary := primaryFaceIndex(faces)
	configured := ""
	if primary < len(faces) {
		configured = faces[primary].GetAddr()
	}
	return &proxyServer{Proxy: proxy, primary: primary, configured: configured}
}

// proxyServer 把接入层适配为传输层服务端：除 Start/Stop 外还实现 transport.Endpointer，
// 供进程内形态归集端点（接入层不注册实例，该端点只作驱动方/测试的对外地址）。
type proxyServer struct {
	*edge.Proxy

	primary    int    // 客户端主面（首个 tcp 面）在接入面列表中的下标
	configured string // 主面在配置里的监听地址（尚未开始监听时的兜底）
}

// Endpoint 返回接入层对外端点（客户端主面，形如 tcp://127.0.0.1:7100）。
// 归集动作可能发生在监听之前（App 组装实例时），此时回落到配置地址。
func (s *proxyServer) Endpoint() (*url.URL, error) {
	addr := s.configured
	if addrs := s.ListenerAddrs(); len(addrs) > 0 {
		addr = addrs[min(s.primary, len(addrs)-1)]
	}
	if addr == "" {
		return nil, errors.New("edge: 接入面没有可用监听地址")
	}
	return url.Parse("tcp://" + addr)
}

// primaryFaceIndex 返回客户端主面（首个 tcp 面 = WS 面）的下标；没有 tcp 面时取第 0 个。
func primaryFaceIndex(list []*conf.Edge_Listener) int {
	for i, l := range list {
		if l.GetNetwork() == conf.Edge_NETWORK_TCP {
			return i
		}
	}
	return 0
}
