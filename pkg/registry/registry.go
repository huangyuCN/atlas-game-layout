// Package registry 提供游戏模板统一的服务注册装配：
// 基于 Atlas contrib/registry/etcd 构造注册器（实现 Registrar + Discovery）。
package registry

import (
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	etcdreg "github.com/huangyuCN/atlas/contrib/registry/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// NamespacePrefix 是注册中心键前缀的默认根：最终前缀为 <根>/<环境>。
const NamespacePrefix = "/atlas/services"

// NamespaceOf 派生注册中心键前缀：显式配置优先，否则按环境隔离。
// 隔离是必需的——实例键为 <前缀>/<服务名>/<实例 ID>，多套部署共用同一 etcd 时，
// 前缀与实例 ID 相同会让后注册者覆盖前者的端点，并在注销时删除对方的注册。
func NamespaceOf(namespace, env string) string {
	if namespace != "" {
		return namespace
	}
	if env == "" {
		env = consts.EnvDefault
	}
	return NamespacePrefix + "/" + env
}

// Options 是注册器构造选项（来自服务配置）。
type Options struct {
	// Namespace 是 etcd 中服务注册的键前缀（默认 <NamespacePrefix>/<环境>）。
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
	eo := make([]etcdreg.Option, 0, 2)
	if opts.Namespace != "" {
		eo = append(eo, etcdreg.Namespace(opts.Namespace))
	}
	if opts.TTL > 0 {
		eo = append(eo, etcdreg.RegisterTTL(opts.TTL))
	}
	return etcdreg.New(client, eo...), nil
}
