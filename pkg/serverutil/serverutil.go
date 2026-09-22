// Package serverutil 提供传输层装配的共享辅助：端点归集与就绪探测。
// 服务端启停不在此包——两种形态（进程形态 cmd/main、进程内形态 services/*/assemble）
// 都由 atlas.App 驱动，见 pkg/bootstrap.ModuleFor。
package serverutil

import (
	"fmt"
	"net/url"
	"time"

	"github.com/huangyuCN/atlas/transport"
)

// 端点 scheme：与各传输层 Endpoint() 返回的 scheme 一致（即 urls 的 map 键）。
// 注意 WS 的 scheme 是 "ws"，与 transport.KindWebSocket 的字符串 "websocket" 不同，
// 故不直接复用 transport.Kind。
const (
	SchemeGRPC = "grpc"
	SchemeHTTP = "http"
	SchemeTCP  = "tcp"
	SchemeWS   = "ws"
	SchemeKCP  = "kcp"
	SchemeUDP  = "udp"
)

// WaitEndpoint 轮询服务端端点直至监听就绪或超时（后台启动阻塞式 Server 时的就绪探测）。
func WaitEndpoint(srv transport.Endpointer, timeout time.Duration) (*url.URL, error) {
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

// Endpoints 按 scheme 归集已启动服务端的就绪端点，返回 scheme → 完整 URL
// （fx 值组不保证提供顺序，不能依赖下标；WS 等带路径的协议需要完整 URL）。
// 服务端必须实现 transport.Endpointer：拿不到端点意味着无法对外寻址，属装配错误，
// 直接报错而不是静默跳过。
func Endpoints(servers []transport.Server) (map[string]*url.URL, error) {
	out := make(map[string]*url.URL, len(servers))
	for i, srv := range servers {
		ep, ok := srv.(transport.Endpointer)
		if !ok {
			return nil, fmt.Errorf("serverutil: server[%d] 未实现 transport.Endpointer，无法归集端点", i)
		}
		u, err := ep.Endpoint()
		if err != nil {
			return nil, fmt.Errorf("serverutil: server[%d] 查询端点失败: %w", i, err)
		}
		if u == nil {
			return nil, fmt.Errorf("serverutil: server[%d] 未返回端点", i)
		}
		out[u.Scheme] = u
	}
	return out, nil
}
