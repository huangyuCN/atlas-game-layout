package observability

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	atlaslog "github.com/huangyuCN/atlas/log"
	atlaspprof "github.com/huangyuCN/atlas/transport/http/pprof"
)

// pprofShutdownTimeout 是 pprof 端点的收尾上限：OnStop 阶段的 ctx 可能已被取消，
// 收尾必须自带有界超时（与指标端点同一姿态）。
const pprofShutdownTimeout = 5 * time.Second

// Pprof 是一次 pprof 端点初始化的产物：监听地址 + 关停句柄。
type Pprof struct {
	// Addr 是 /debug/pprof/ 的实际监听地址（host:port）；未启用端点时为空。
	Addr string

	shutdown func(context.Context) error
}

// Shutdown 关闭 pprof 端点（幂等；未启用端点时空操作）。
func (p *Pprof) Shutdown(ctx context.Context) error {
	if p == nil || p.shutdown == nil {
		return nil
	}
	return p.shutdown(ctx)
}

// InitPprof 按配置启动 pprof 调试端点：
// addr 为空时返回空实例（**不监听任何端口**，默认关闭）——配置缺失不阻断启动；
// 处理器复用框架 transport/http/pprof.NewHandler()（单源：不在模板里再注册一套
// net/http/pprof，否则两条注册路径会随 Go 版本各自漂移）。
//
// 安全：pprof 会暴露进程内存、goroutine 与调用栈。地址是**运维显式配置**的，
// 非回环地址（0.0.0.0 / 内网 / 公网）只告警不拒绝——绑内网供按需采样是允许的部署形态，
// 但公网暴露等于把内存与调用栈交出去；生产建议按需临时开启、采样完即关（见 docs/config.md）。
func InitPprof(addr string) (*Pprof, error) {
	if addr == "" {
		return &Pprof{}, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("observability: pprof 端点监听失败 (%s): %w", addr, err)
	}
	srv := &http.Server{Handler: atlaspprof.NewHandler(), ReadHeaderTimeout: 5 * time.Second}
	serveHTTP(srv, ln, "pprof 端点")
	// bound 是内核分配后的真实地址（配置写 `:0` 时端口由内核给出）。
	bound := ln.Addr().String()
	if !isLoopbackAddr(bound) {
		atlaslog.Warnf("observability: pprof 端点绑定在非回环地址 %s——/debug/pprof/ 会暴露"+
			"进程内存与调用栈，请确认只在内网可达且不对公网开放", bound)
	}
	atlaslog.Infof("observability: pprof 调试端点已启用 (http://%s/debug/pprof/)", bound)

	return &Pprof{Addr: bound, shutdown: func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pprofShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			atlaslog.Warnf("observability: pprof 端点关闭失败: %v", err)
		}
		return nil
	}}, nil
}

// isLoopbackAddr 判断监听地址是否只对本机开放：回环 IP（127.0.0.0/8、::1）或主机名
// localhost 为真；0.0.0.0、空主机（全接口）与其它主机名一律为假——主机名可能解析到
// 任意网卡，无法证明只对本机开放，故按「非回环」处理（只影响告警与否，不影响启停）。
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
