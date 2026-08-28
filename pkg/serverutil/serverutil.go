// Package serverutil 提供服务装配的共享辅助（可编程装配与测试装置共用）。
package serverutil

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/huangyuCN/atlas/transport"
)

// Endpointer 是可就绪探测的服务端能力（对齐 transport.Endpointer，便于本包独立引用）。
type Endpointer interface {
	Endpoint() (*url.URL, error)
}

// WaitEndpoint 轮询服务端端点直至监听就绪或超时
// （Atlas 阻塞式 Server.Start 在后台启动时的就绪探测）。
func WaitEndpoint(srv interface{ Endpoint() (*url.URL, error) }, timeout time.Duration) (*url.URL, error) {
	deadline := time.Now().Add(timeout)
	for {
		if ep, err := srv.Endpoint(); err == nil {
			return ep, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("serverutil: 端点未就绪（%s）", timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ServeAsync 以进程内形态启动一组阻塞式 Server（与生产路径同一批构造，
// 改由本函数而非 atlas App 驱动启停）：
//
//   - 每个 Server 一个后台 goroutine 执行 Start（阻塞至显式 Stop，
//     Atlas 传输层不会因 ctx 取消自行退出，回收必须经 Stop 完成）；
//   - 逐个等待端点就绪，返回与入参同序的端点列表；
//   - 任一端点在 timeout 内未就绪：取消并停止已尝试的服务端后报错。
func ServeAsync(timeout time.Duration, servers ...transport.Server) ([]*url.URL, func(context.Context) error, error) {
	sctx, cancel := context.WithCancel(context.Background())
	eps := make([]*url.URL, len(servers))
	for i, srv := range servers {
		endpoint, ok := srv.(Endpointer)
		if !ok {
			cancel()
			return nil, nil, fmt.Errorf("serverutil: server[%d] 未实现 transport.Endpointer，无法就绪探测", i)
		}
		go func() { _ = srv.Start(sctx) }()
		ep, err := WaitEndpoint(endpoint, timeout)
		if err != nil {
			cancel()
			stopAll(context.Background(), servers[:i+1])
			return nil, nil, fmt.Errorf("serverutil: %w", err)
		}
		eps[i] = ep
	}
	stop := func(ctx context.Context) error {
		err := stopAll(ctx, servers)
		cancel()
		return err
	}
	return eps, stop, nil
}

// stopAll 逐个停止服务端，返回首个错误（保证全部尝试完毕）。
func stopAll(ctx context.Context, servers []transport.Server) error {
	var firstErr error
	for _, srv := range servers {
		if err := srv.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
