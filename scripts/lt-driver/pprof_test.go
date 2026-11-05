package main

import (
	"net/http"
	"testing"
	"time"
)

// TestStartPprofServesIndex 断言 pprof 端点真的起来了（服务端 profile 已证明不饱和，
// 尾巴只能从压测端看，这个开关就是那条观测通道）。
func TestStartPprofServesIndex(t *testing.T) {
	addr, stop, err := startPprof("127.0.0.1:0")
	if err != nil {
		t.Fatalf("起 pprof 失败: %v", err)
	}
	defer stop()
	if addr == "" {
		t.Fatal("应返回实际监听地址（端口 0 时由内核分配）")
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/debug/pprof/")
	if err != nil {
		t.Fatalf("请求 pprof 首页失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pprof 首页状态码应为 200，实际 %d", resp.StatusCode)
	}
}

// TestStartPprofDisabledOnEmptyAddr 断言空地址 = 不监听（缺省零开销），且 stop 可安全调用。
func TestStartPprofDisabledOnEmptyAddr(t *testing.T) {
	addr, stop, err := startPprof("")
	if err != nil {
		t.Fatalf("空地址不应报错: %v", err)
	}
	if addr != "" {
		t.Fatalf("空地址不应监听，实际返回 %q", addr)
	}
	stop() // 不得 panic
}
