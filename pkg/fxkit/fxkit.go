// Package fxkit 提供跨服务复用的 fx 泛型提供器：
// 各服务 internal/conf 的 *Bootstrap 均引用公共配置消息（protobuf/configs），
// 借助泛型把「从配置提取参数构造底层客户端」的实现收敛为一份，
// 供各服务的 fx 模块以 `fxkit.NewEtcdClient[*conf.Bootstrap]`、
// `fxkit.NewRedisClient[*conf.Bootstrap]` 形式直接提供。
package fxkit

import (
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/pkg/enumconv"
	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	etcdreg "github.com/huangyuCN/atlas/contrib/registry/etcd"
	"github.com/huangyuCN/atlas/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// WithRegistry 是提取注册中心配置段所需的最小接口
// （各服务生成的 *Bootstrap 自动满足）。
type WithRegistry interface {
	GetRegistry() *configspb.Registry
}

// WithRuntime 是提取服务运行时身份所需的最小接口
// （各服务生成的 *Bootstrap 自动满足）。
type WithRuntime interface {
	GetRuntime() *configspb.Runtime
}

// RegistryConfig 是装配注册中心所需的最小配置接口：注册中心配置段 + 服务身份。
type RegistryConfig interface {
	WithRegistry
	WithRuntime
}

// EtcdEndpoints 提取 registry.etcd.endpoints（缺失时返回 nil）。
func EtcdEndpoints[B WithRegistry](cfg B) []string {
	if r := cfg.GetRegistry(); r != nil && r.GetEtcd() != nil {
		return r.GetEtcd().GetEndpoints()
	}
	return nil
}

// NewEtcdClient 从配置装配 etcd 客户端（惰性连接，不阻塞建连）。
// 未配置 registry.etcd.endpoints 时快速失败：
// 所有服务都依赖 actor 集群与注册中心，缺配置应尽早暴露而非静默降级。
func NewEtcdClient[B WithRegistry](cfg B) (*clientv3.Client, error) {
	endpoints := EtcdEndpoints(cfg)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("fxkit: 配置缺少 registry.etcd.endpoints（actor 集群与注册中心必需）")
	}
	cli, err := etcd.NewClient(etcd.Options{Endpoints: endpoints})
	if err != nil {
		return nil, fmt.Errorf("fxkit: 构造 etcd 客户端失败: %w", err)
	}
	return cli, nil
}

// RegistryOptions 把 registry 配置段与服务身份映射为注册器构造选项：
// namespace 缺省按 runtime.env 隔离（多套部署共用同一 etcd 时不串台），
// ttl 未配置时留零值交给底层默认值。
func RegistryOptions[B RegistryConfig](cfg B) (pkgregistry.Options, error) {
	r := cfg.GetRegistry()
	ttl, err := config.ParseDuration(r.GetTtl())
	if err != nil {
		return pkgregistry.Options{}, fmt.Errorf("fxkit: registry.ttl 无效: %w", err)
	}
	return pkgregistry.Options{
		Namespace: pkgregistry.NamespaceOf(r.GetNamespace(), cfg.GetRuntime().GetEnv()),
		TTL:       ttl,
	}, nil
}

// NewRegistrar 基于 etcd 客户端与注册配置装配 Atlas 注册器。
func NewRegistrar[B RegistryConfig](cfg B, ec *clientv3.Client) (registry.Registrar, error) {
	reg, err := newEtcdRegistry(cfg, ec)
	if err != nil {
		return nil, err
	}
	return reg, nil
}

// NewEtcdDiscovery 基于 etcd 客户端与注册配置装配服务发现（actor 集群懒激活选节点用）：
// 与 NewRegistrar 共用同一条构造路径，键前缀因此必然一致——前缀不一致会表现为
// 「注册成功却发现不到」。
func NewEtcdDiscovery[B RegistryConfig](cfg B, ec *clientv3.Client) (registry.Discovery, error) {
	reg, err := newEtcdRegistry(cfg, ec)
	if err != nil {
		return nil, err
	}
	return reg, nil
}

// newEtcdRegistry 按注册配置构造 etcd 注册中心
// （同一对象同时实现 registry.Registrar 与 registry.Discovery）。
func newEtcdRegistry[B RegistryConfig](cfg B, ec *clientv3.Client) (*etcdreg.Registry, error) {
	opts, err := RegistryOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("fxkit: %w", err)
	}
	reg, err := pkgregistry.NewEtcd(ec, opts)
	if err != nil {
		return nil, fmt.Errorf("fxkit: 构造注册中心失败: %w", err)
	}
	return reg, nil
}

// WithData 是提取数据中间件配置段所需的最小接口
// （各服务生成的 *Bootstrap 自动满足）。
type WithData interface {
	GetData() *configspb.Data
}

// RedisOptions 把 data.redis 配置映射为 redis.Options：proto 枚举 → Go 枚举
// 只此一份，避免各服务重复映射。配置段缺失时返回零值（单点、无地址），
// 由 NewRedisClient 统一校验后快速失败。
func RedisOptions[B WithData](cfg B) (pkredis.Options, error) {
	var r *configspb.Data_Redis
	if d := cfg.GetData(); d != nil {
		r = d.GetRedis()
	}
	if r == nil {
		return pkredis.Options{}, nil
	}
	mode, err := redisModeOf(r.GetMode())
	if err != nil {
		return pkredis.Options{}, err
	}
	return pkredis.Options{
		Addrs:      r.GetAddrs(),
		Mode:       mode,
		MasterName: r.GetMasterName(),
		Password:   r.GetPassword(),
		DB:         int(r.GetDb()),
	}, nil
}

// NewRedisClient 从配置装配 redis 客户端（惰性连接，不建连）；
// 配置缺失或形态非法时快速失败——依赖 redis 的服务应尽早暴露而非静默降级。
func NewRedisClient[B WithData](cfg B) (*pkredis.Client, error) {
	opts, err := RedisOptions(cfg)
	if err != nil {
		return nil, fmt.Errorf("fxkit: %w", err)
	}
	cli, err := pkredis.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("fxkit: 构造 redis 客户端失败: %w", err)
	}
	return cli, nil
}

// redisModeOf 把 proto 形态枚举映射为 redis.Mode；未知取值快速失败。
func redisModeOf(m configspb.Data_RedisMode) (pkredis.Mode, error) {
	return enumconv.Map(m, map[configspb.Data_RedisMode]pkredis.Mode{
		configspb.Data_REDIS_MODE_SINGLE:   pkredis.ModeSingle,
		configspb.Data_REDIS_MODE_SENTINEL: pkredis.ModeSentinel,
		configspb.Data_REDIS_MODE_CLUSTER:  pkredis.ModeCluster,
	}, "redis 配置形态")
}
