// Package registry 提供游戏模板统一的服务注册装配：
// 基于 Atlas contrib/registry/etcd 构造注册器（实现 Registrar + Discovery）。
package registry

import (
	"fmt"
	"time"

	etcdreg "github.com/huangyuCN/atlas/contrib/registry/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Options 是注册器构造选项（来自服务配置）。
type Options struct {
	// Namespace 是注册中心键前缀，取 namespace.Derived.RegistryPrefix（形如 /atlas/services/test）：
	// 实例键为 <Namespace>/<服务名>/<实例 ID>。**必填**——多套部署共用同一 etcd 时，
	// 前缀与实例 ID 都相同会让后注册者覆盖前者的端点、并在注销时删掉对方的注册，
	// 故缺失即构造失败（R9：不回落 env/default，前缀唯一来源是配置字段 runtime.namespace）。
	Namespace string
	// TTL 是注册租约的存活时间（默认 15s，心跳自动续租）。
	TTL time.Duration
}

// NewEtcd 基于 etcd 客户端构造 Atlas 注册中心：同一对象同时实现
// registry.Registrar 与 registry.Discovery，注册端与发现端因此天然同源。
func NewEtcd(client *clientv3.Client, opts Options) (*etcdreg.Registry, error) {
	if client == nil {
		return nil, fmt.Errorf("registry: etcd 客户端不能为空")
	}
	if opts.Namespace == "" {
		return nil, fmt.Errorf("registry: 键前缀不能为空（取 namespace.Derive 的 RegistryPrefix，配置字段 runtime.namespace）")
	}
	eo := []etcdreg.Option{etcdreg.Namespace(opts.Namespace)}
	if opts.TTL > 0 {
		eo = append(eo, etcdreg.RegisterTTL(opts.TTL))
	}
	return etcdreg.New(client, eo...), nil
}
