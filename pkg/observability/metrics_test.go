package observability

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/huangyuCN/atlas/metrics"
)

// TestInitMetricsNoop 验证未配置抓取地址时返回 noop 采集器（零开销）。
func TestInitMetricsNoop(t *testing.T) {
	c, shutdown, err := InitMetrics("", "game")
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	if !metrics.IsNoop(c) {
		t.Fatal("未配置端点应返回 noop 采集器")
	}
}

// TestInitMetricsRequiresServiceName 验证配置了端点但缺服务名时报错。
func TestInitMetricsRequiresServiceName(t *testing.T) {
	if _, _, err := InitMetrics("127.0.0.1:0", ""); err == nil {
		t.Fatal("缺服务名应报错")
	}
}

// TestInitMetricsEndpoint 验证采集值经 /metrics 端点可被抓取，
// 且 service 常量标签随行导出。
func TestInitMetricsEndpoint(t *testing.T) {
	c, shutdown, err := InitMetrics("127.0.0.1:0", "game")
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())
	if metrics.IsNoop(c) {
		t.Fatal("已配置端点不应返回 noop 采集器")
	}
	c.Gauge("test_players_online").Set(3)

	resp, err := http.Get("http://" + lastEndpointAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	// 常量标签随行导出：gauge 行形如 test_players_online{service="game"} 3。
	if !strings.Contains(text, "test_players_online{service=\"game\"} 3") {
		t.Fatalf("/metrics 缺 test_players_online 值: %s", text)
	}
}
