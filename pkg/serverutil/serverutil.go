// Package serverutil 提供服务装配的共享辅助（可编程装配与测试装置共用）。
package serverutil

import (
	"fmt"
	"net/url"
	"time"
)

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
