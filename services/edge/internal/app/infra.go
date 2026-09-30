package app

import (
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/guard"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/resolver"
	"github.com/huangyuCN/atlas/contrib/edge"
	etcdlocator "github.com/huangyuCN/atlas/contrib/locator/etcd"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/namespace"
	"github.com/huangyuCN/atlas/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// newActorDirectory 装配 actor 目录的**只读**适配器：
// 前缀取 namespace.Derive(actor_namespace 或 runtime.namespace).EtcdDirectory
// （/atlas/actors/<ns>），与 battle 侧托管方写入目录时同源——前缀不一致会表现为「查不到属主」。
func newActorDirectory(cfg *conf.Bootstrap, ec *clientv3.Client) (*resolver.LocatorDirectory, error) {
	if ec == nil {
		return nil, fmt.Errorf("app: etcd 客户端为空（actor 目录不可用）")
	}
	ns := cfg.GetEdge().GetActorNamespace()
	if ns == "" {
		ns = cfg.GetRuntime().GetNamespace()
	}
	derived, err := namespace.Derive(ns)
	if err != nil {
		return nil, fmt.Errorf("app: actor 目录命名空间非法（edge.actor_namespace 或 runtime.namespace）: %w", err)
	}
	loc, err := etcdlocator.NewLocator(ec, etcdlocator.WithPrefix(derived.EtcdDirectory))
	if err != nil {
		return nil, fmt.Errorf("app: 构造 actor 目录 locator 失败: %w", err)
	}
	return resolver.NewLocatorDirectory(loc)
}

// newFrameRegistry 装配帧面实例发现（battle 注册的 battle-frame 实例）。
// 服务发现由装配层提供（fxkit.NewEtcdDiscovery），键前缀与注册端同源。
func newFrameRegistry(discovery registry.Discovery) (*resolver.DiscoveryRegistry, error) {
	return resolver.NewDiscoveryRegistry(discovery)
}

// newResolver 装配接入层后端解析器（目录属主 + 帧面实例端口 → 后端地址）。
func newResolver(dir resolver.Directory, frames resolver.FrameRegistry,
	cfg *conf.Bootstrap) (*resolver.Resolver, error) {
	return resolver.New(dir, frames, resolver.Options{
		FrameService: cfg.GetEdge().GetFrameService(),
	})
}

// newStreamGuard 装配属主变更守卫（规格 §8）：目录属主换人即拆该局的直连流。
func newStreamGuard(dir *resolver.LocatorDirectory) (*guard.OwnerGuard, error) {
	return guard.New(dir, nil)
}

// guardOf 把守卫绑定为 edge.BackendGuard（nil 允许：未装配时不拆流，行为与批次 6 一致）。
func guardOf(g *guard.OwnerGuard) edge.BackendGuard {
	if g == nil {
		return nil
	}
	return g
}

// newProxy 装配接入层实例（实现 transport.Server：Start 起监听、Stop 优雅停机含拆流）。
func newProxy(cfg *conf.Bootstrap, res edge.Resolver, collector metrics.Collector,
	g edge.BackendGuard) (*edge.Proxy, error) {
	ecfg, err := proxyConfig(cfg, res, collector, g)
	if err != nil {
		return nil, err
	}
	proxy, err := edge.New(ecfg)
	if err != nil {
		return nil, fmt.Errorf("app: 构造接入层失败: %w", err)
	}
	return proxy, nil
}

// directoryOf 把目录实现绑定为 resolver.Directory 接口（供 fx 按接口注入）。
func directoryOf(d *resolver.LocatorDirectory) resolver.Directory { return d }

// framesOf 把帧面发现实现绑定为 resolver.FrameRegistry 接口。
func framesOf(f *resolver.DiscoveryRegistry) resolver.FrameRegistry { return f }

// resolverOf 把解析器绑定为 edge.Resolver 接口。
func resolverOf(r *resolver.Resolver) edge.Resolver { return r }
