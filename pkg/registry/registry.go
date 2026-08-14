// Package registry 提供游戏模板统一的服务注册装配：
// 基于 Atlas contrib/registry/etcd 构造注册器（实现 Registrar + Discovery）。
package registry

import (
	"fmt"
	"time"

	etcdreg "github.com/huangyuCN/atlas/contrib/registry/etcd"
	"github.com/huangyuCN/atlas/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Options 是注册器构造选项（来自服务配置）。
type Options struct {
	// Namespace 是 etcd 中服务注册的键前缀（默认 /atlas/services）。
	Namespace string
	// TTL 是注册租约的存活时间（默认 15s，心跳自动续租）。
	TTL time.Duration
}

// NewEtcd 基于 etcd 客户端构造 Atlas 注册中心。
func NewEtcd(client *clientv3.Client, opts Options) (registry.Registrar, error) {
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
