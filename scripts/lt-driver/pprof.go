package main

import (
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// startPprof 在 addr 上起一个只暴露 /debug/pprof/ 的 HTTP 服务，返回实际监听地址与停止函数。
//
// 为什么需要：2026-11-05 的容量实测里，服务端在 5,000 流时只用了 4.2/4.1 核（16 核机器）、
// 没有任何单个协程接近跑满，而 p99 仍在 130–150 ms。要判断尾巴是不是在**压测端自己**（12 核机器上
// 跑 5,000 条 KCP 会话，调度延迟会直接算进探针往返），只能在驱动侧抓 CPU / trace（trace 的
// `-pprof=sched` 直接给「等 P 的调度延迟」）。
//
// addr 为空时不监听（缺省关闭，零开销）。
func startPprof(addr string) (string, func(), error) {
	if addr == "" {
		return "", func() {}, nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), func() { _ = srv.Close() }, nil
}
