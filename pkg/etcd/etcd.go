// Package etcd 提供游戏模板统一的 etcd 客户端构造装配。
package etcd

import (
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Options 是 etcd 客户端构造选项（来自服务配置）。
type Options struct {
	// Endpoints 是 etcd 节点地址列表。
	Endpoints []string
	// DialTimeout 是建连超时（默认 5s）。
	DialTimeout time.Duration
	// Username/Password 是可选认证信息。
	Username string
	Password string
}

// NewClient 构造 etcd 客户端（惰性连接，不阻塞建连）。
func NewClient(opts Options) (*clientv3.Client, error) {
	if len(opts.Endpoints) == 0 {
		return nil, fmt.Errorf("etcd: endpoints 不能为空")
	}
	timeout := opts.DialTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	cfg := clientv3.Config{
		Endpoints:   opts.Endpoints,
		DialTimeout: timeout,
	}
	if opts.Username != "" {
		cfg.Username = opts.Username
		cfg.Password = opts.Password
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("etcd: 构造客户端失败: %w", err)
	}
	return client, nil
}
