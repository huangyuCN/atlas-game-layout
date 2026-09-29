package serverutil

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"
)

// errNoDialer 表示惰性连接缺拨号函数或拨号返回空连接（装配缺陷：不静默成功）。
var errNoDialer = errors.New("serverutil: 惰性连接未装配拨号函数")

// LazyConn 是「首次调用时拨号、之后复用」的 gRPC 连接持有者（实现 grpc.ClientConnInterface）。
//
// 为什么需要它：域服务之间的调用不应把「对端已在线」变成**本服务启动的前置条件**——
// 四个服务可以任意顺序启动（`make run-all` 并发拉起、滚动升级、对端重启），
// 启动期拨号会把部署顺序变成隐性依赖（表现为随机启动失败）。故拨号推迟到首个调用，
// 失败只让该次调用失败（失败结果不缓存，下次调用重试）；连接建立后的重连由 gRPC 自身负责。
//
// 它只需要一个 dial 闭包（含面 scheme 与拦截器，见 DialDomain），不重复描述目标。
type LazyConn struct {
	mu   sync.Mutex
	conn grpc.ClientConnInterface
	dial func(ctx context.Context) (grpc.ClientConnInterface, error)
}

// NewLazyConn 构造惰性连接（dial 为 nil 时调用即报错——装配缺失不静默成功）。
func NewLazyConn(dial func(ctx context.Context) (grpc.ClientConnInterface, error)) *LazyConn {
	return &LazyConn{dial: dial}
}

// Invoke 实现 grpc.ClientConnInterface：取连接（首次拨号）后转发一元调用。
func (c *LazyConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	conn, err := c.get(ctx)
	if err != nil {
		return err
	}
	return conn.Invoke(ctx, method, args, reply, opts...)
}

// NewStream 实现 grpc.ClientConnInterface：取连接（首次拨号）后开流。
func (c *LazyConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	return conn.NewStream(ctx, desc, method, opts...)
}

// get 返回已建连接；首次调用时拨号并缓存（并发安全：拨号在锁内只做一次）；
// 拨号失败不缓存（下次调用重试），返回 nil 连接视为装配错误。
func (c *LazyConn) get(ctx context.Context) (grpc.ClientConnInterface, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn, nil
	}
	if c.dial == nil {
		return nil, errNoDialer
	}
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, errNoDialer
	}
	c.conn = conn
	return conn, nil
}

// Close 关闭已建连接（未拨号时为空操作；重复调用安全）。
// 连接不可关闭（测试桩等）时只解除持有，不算错误。
func (c *LazyConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	conn := c.conn
	c.conn = nil
	if closer, ok := conn.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
