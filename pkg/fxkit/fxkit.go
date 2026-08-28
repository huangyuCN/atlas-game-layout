// Package fxkit 提供跨服务复用的 fx 泛型提供器：
// 各服务 internal/conf 的 *Bootstrap 均引用公共配置消息（protobuf/configs），
// 借助泛型把「从配置提取参数构造底层客户端」的实现收敛为一份，
// 供各服务的 fx 模块以 `fxkit.NewEtcdClient[*conf.Bootstrap]` 形式直接提供。
package fxkit

import (
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// WithRegistry 是提取注册中心配置段所需的最小接口
// （各服务生成的 *Bootstrap 自动满足）。
type WithRegistry interface {
	GetRegistry() *configspb.Registry
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

// NewRegistrar 基于 etcd 客户端装配 Atlas 注册器（Registrar + Discovery 同一实现）。
func NewRegistrar(ec *clientv3.Client) (registry.Registrar, error) {
	reg, err := pkgregistry.NewEtcd(ec, pkgregistry.Options{})
	if err != nil {
		return nil, fmt.Errorf("fxkit: 构造注册器失败: %w", err)
	}
	return reg, nil
}
