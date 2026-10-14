package observability

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestInitPprofDisabled 验证未配置监听地址时不监听任何端口（默认关闭、零开销）：
// 返回实例没有地址，Shutdown 是空操作。
func TestInitPprofDisabled(t *testing.T) {
	p, err := InitPprof("")
	if err != nil {
		t.Fatalf("InitPprof(\"\") 错误 = %v", err)
	}
	if p.Addr != "" {
		t.Fatalf("未配置地址不应有监听地址，实际 %q", p.Addr)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("未启用端点的 Shutdown 应为空操作: %v", err)
	}
}

// TestInitPprofBadAddr 验证地址非法时报错（不静默降级为「不监听」——
// 配了却起不来必须让装配期失败，否则「配了没生效」会从缝里溜进来）。
func TestInitPprofBadAddr(t *testing.T) {
	if _, err := InitPprof("127.0.0.1:not-a-port"); err == nil {
		t.Fatal("非法监听地址应报错")
	}
}

// TestInitPprofServesIndex 验证配置了地址即在该地址可访问 /debug/pprof/：
// 用随机端口 `127.0.0.1:0` 并读回真实地址（内核分配的端口）。
func TestInitPprofServesIndex(t *testing.T) {
	p, err := InitPprof("127.0.0.1:0")
	if err != nil {
		t.Fatalf("InitPprof 错误 = %v", err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	if p.Addr == "" {
		t.Fatal("已配置地址应读回真实监听地址")
	}

	body, code := getText(t, "http://"+p.Addr+"/debug/pprof/")
	if code != http.StatusOK {
		t.Fatalf("/debug/pprof/ 状态码 = %d，期望 200", code)
	}
	// 索引页列出可采样的剖析项：goroutine / heap / profile（CPU）。
	for _, want := range []string{"goroutine", "heap", "profile"} {
		if !strings.Contains(body, want) {
			t.Errorf("/debug/pprof/ 索引页缺 %q:\n%s", want, body)
		}
	}
}

// TestInitPprofProfile 验证短采样能取到非空 CPU profile（1 秒采样，避免拖慢测试）。
func TestInitPprofProfile(t *testing.T) {
	p, err := InitPprof("127.0.0.1:0")
	if err != nil {
		t.Fatalf("InitPprof 错误 = %v", err)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()

	resp, err := http.Get("http://" + p.Addr + "/debug/pprof/profile?seconds=1")
	if err != nil {
		t.Fatalf("取 profile 失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读 profile 失败: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/debug/pprof/profile 状态码 = %d，期望 200", resp.StatusCode)
	}
	if len(body) == 0 {
		t.Fatal("profile 为空（采样 1 秒应产出非空剖析数据）")
	}
}

// TestInitPprofShutdownStopsServing 验证停机后端点不再对外服务（监听与连接一并关闭）：
// 断言「再访问必然失败」而非「同一端口可被重新绑定」——后者会与其它进程抢占
// 临时端口（同机压测/浏览器/压测驱动都会用临时端口做客户端本地端口）偶发冲突。
func TestInitPprofShutdownStopsServing(t *testing.T) {
	p, err := InitPprof("127.0.0.1:0")
	if err != nil {
		t.Fatalf("InitPprof 错误 = %v", err)
	}
	addr := p.Addr
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown 错误 = %v", err)
	}
	resp, err := http.Get("http://" + addr + "/debug/pprof/")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("停机后 %s 仍可访问（状态码 %d）", addr, resp.StatusCode)
	}
}

// TestIsLoopbackAddr 验证回环判定（非回环地址只告警不拒绝：内网按需采样是允许的部署形态）。
func TestIsLoopbackAddr(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:6060":  true,
		"127.0.0.2:6060":  true,
		"[::1]:6060":      true,
		"localhost:6060":  true,
		"0.0.0.0:6060":    false,
		":6060":           false,
		"10.10.9.36:6060": false,
		"bogus":           false,
		"":                false,
	}
	for addr, want := range cases {
		if got := isLoopbackAddr(addr); got != want {
			t.Errorf("isLoopbackAddr(%q) = %v，期望 %v", addr, got, want)
		}
	}
}

// getText 取该 URL 的响应体与状态码（用例共用，避免逐处重复 HTTP 样板）。
func getText(t *testing.T, url string) (string, int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s 失败: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读 %s 响应失败: %v", url, err)
	}
	return string(body), resp.StatusCode
}
