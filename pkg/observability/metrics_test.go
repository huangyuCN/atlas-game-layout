package observability

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/metricstest"
	"github.com/huangyuCN/atlas/metrics"
	"go.opentelemetry.io/otel"
)

// TestInitMetricsNoop 验证未配置抓取地址时返回 noop 采集器（零开销、无监听地址）。
func TestInitMetricsNoop(t *testing.T) {
	m, err := InitMetrics("", ServiceIdentity{Name: "game"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())
	if !metrics.IsNoop(m.Collector) {
		t.Fatal("未配置端点应返回 noop 采集器")
	}
	if m.Addr != "" {
		t.Fatalf("未配置端点不应有监听地址，实际 %q", m.Addr)
	}
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatalf("noop Shutdown 应为空操作: %v", err)
	}
}

// TestInitMetricsRequiresServiceName 验证配置了端点但缺服务名时报错。
func TestInitMetricsRequiresServiceName(t *testing.T) {
	if _, err := InitMetrics("127.0.0.1:0", ServiceIdentity{}); err == nil {
		t.Fatal("缺服务名应报错")
	}
}

// TestInitMetricsEndpoint 验证采集值经 /metrics 端点可被抓取，
// 且 service 常量标签随行导出。
func TestInitMetricsEndpoint(t *testing.T) {
	m, err := InitMetrics("127.0.0.1:0", ServiceIdentity{Name: "game", ID: "game-1", Version: "v1.2.3", Env: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(context.Background())
	if metrics.IsNoop(m.Collector) {
		t.Fatal("已配置端点不应返回 noop 采集器")
	}
	m.Collector.Gauge("test_players_online").Set(3)

	resp, err := http.Get("http://" + m.Addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	// 常量标签随行导出（Prometheus 按标签名排序）：服务身份四项齐全。
	want := `test_players_online{env="test",service="game",service_instance_id="game-1",service_version="v1.2.3"} 3`
	if !strings.Contains(text, want) {
		t.Fatalf("/metrics 缺服务身份标签\nwant 含 %s\n got %s", want, text)
	}
}

// TestInitMetricsInstallsGlobalProvider 验证 InitMetrics 把采集器的 MeterProvider 装为全局：
// OTel 原生埋点（如 pkg/middleware 的请求指标）才能落到同一个 /metrics 端点。
func TestInitMetricsInstallsGlobalProvider(t *testing.T) {
	m, err := InitMetrics("127.0.0.1:0", ServiceIdentity{Name: "game"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Shutdown(context.Background()) }()

	counter, err := otel.Meter("test-scope").Int64Counter("native_counter_total")
	if err != nil {
		t.Fatalf("创建 instrument 失败: %v", err)
	}
	counter.Add(context.Background(), 7)

	resp, err := http.Get("http://" + m.Addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "native_counter_total") {
		t.Fatalf("/metrics 缺少 OTel 原生埋点（全局 provider 未装）:\n%s", body)
	}
}

// plainCollector 是不支持拉取式能力的采集器（用于验证降级路径）。
type plainCollector struct{}

// Counter 返回空实现。
func (plainCollector) Counter(string, ...string) metrics.Counter { return metrics.Noop().Counter("") }

// Histogram 返回空实现。
func (plainCollector) Histogram(string, ...string) metrics.Histogram {
	return metrics.Noop().Histogram("")
}

// Gauge 返回空实现。
func (plainCollector) Gauge(string, ...string) metrics.Gauge { return metrics.Noop().Gauge("") }

// TestRegisterObservableGauge 验证支持拉取式能力的采集器被登记；
// nil / noop / 不支持该能力的采集器静默降级（不 panic）。
func TestRegisterObservableGauge(t *testing.T) {
	rec := metricstest.New()
	RegisterObservableGauge(rec, "game_players_online", func() float64 { return 42 })
	fn, ok := rec.Observable("game_players_online")
	if !ok {
		t.Fatal("采集器支持拉取式能力时应完成登记")
	}
	if got := fn(); got != 42 {
		t.Fatalf("回调值 = %v, 期望 42", got)
	}

	// 降级路径：不得 panic。
	RegisterObservableGauge(nil, "nil_meter", func() float64 { return 1 })
	RegisterObservableGauge(metrics.Noop(), "noop_meter", func() float64 { return 1 })
	RegisterObservableGauge(plainCollector{}, "plain_meter", func() float64 { return 1 })
}
