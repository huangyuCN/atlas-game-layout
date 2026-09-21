package observability

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	otelmetrics "github.com/huangyuCN/atlas/contrib/metrics/otel"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/metrics"
)

// metricsShutdownTimeout 是抓取端点与底层 MeterProvider 的收尾上限：
// OnStop 阶段的 ctx 可能已被取消，收尾必须自带有界超时。
const metricsShutdownTimeout = 5 * time.Second

// Metrics 是一次指标初始化的产物：采集器 + 抓取端点地址 + 关停句柄。
type Metrics struct {
	// Collector 是采集器（未配置端点时为 noop）。
	Collector metrics.Collector
	// Addr 是 /metrics 的实际监听地址（host:port）；未启用端点时为空。
	Addr string

	shutdown func(context.Context) error
}

// Shutdown 关闭抓取端点与底层 MeterProvider（幂等；未启用端点时空操作）。
func (m *Metrics) Shutdown(ctx context.Context) error {
	if m == nil || m.shutdown == nil {
		return nil
	}
	return m.shutdown(ctx)
}

// InitMetrics 建立指标采集器与 Prometheus 抓取端点：
// promAddr 为空时返回 noop 采集器（不监听端口、零开销）——配置缺失不阻断启动；
// serviceName 作为 service 常量标签附加到全部导出指标（面板按服务区分）。
func InitMetrics(promAddr, serviceName string) (*Metrics, error) {
	if promAddr == "" {
		return &Metrics{Collector: metrics.Noop()}, nil
	}
	if serviceName == "" {
		return nil, fmt.Errorf("observability: 初始化指标暴露需要服务名")
	}
	exp, err := otelmetrics.New(
		otelmetrics.WithConstLabels(map[string]string{"service": serviceName}),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: 构建指标采集器失败: %w", err)
	}
	ln, err := net.Listen("tcp", promAddr)
	if err != nil {
		_ = exp.Shutdown(context.Background())
		return nil, fmt.Errorf("observability: 指标端点监听失败 (%s): %w", promAddr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", exp.Handler())
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			atlaslog.Errorf("observability: 指标端点异常退出: %v", serveErr)
		}
	}()
	atlaslog.Infof("observability: Prometheus 抓取端点已启用 (http://%s/metrics)", ln.Addr())

	return &Metrics{
		Collector: exp.Collector(),
		Addr:      ln.Addr().String(),
		shutdown: func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), metricsShutdownTimeout)
			defer cancel()
			if err := srv.Shutdown(ctx); err != nil {
				atlaslog.Warnf("observability: 指标端点关闭失败: %v", err)
			}
			if err := exp.Shutdown(ctx); err != nil {
				atlaslog.Warnf("observability: 指标 provider 关闭失败: %v", err)
			}
			return nil
		},
	}, nil
}

// RegisterObservableGauge 注册拉取式仪表：采集时回调 fn 读取当前值，
// 值始终来自真相源，不依赖生命周期事件成对增减。采集器不支持该能力时
// 静默跳过（noop 后端零开销）；labels 必须为 [key1, val1, ...] 键值对。
func RegisterObservableGauge(c metrics.Collector, name string, fn func() float64, labels ...string) {
	if c == nil || fn == nil || metrics.IsNoop(c) {
		return
	}
	if oc, ok := c.(metrics.ObservableCollector); ok {
		oc.ObservableGauge(name, fn, labels...)
	}
}
