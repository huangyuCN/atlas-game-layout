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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
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
// 服务身份（id）作为常量标签附加到全部导出指标（service / service_version /
// service_instance_id / env，空值不注入），与链路资源属性同源。
//
// 抓取端点同时暴露 Go 运行时与进程指标（go_* / process_*，见 newPromRegistry）：
// 长跑的内存、句柄与 goroutine 判据都取这里（投产清单 OBS-4）。
func InitMetrics(promAddr string, id ServiceIdentity) (*Metrics, error) {
	if promAddr == "" {
		return &Metrics{Collector: metrics.Noop()}, nil
	}
	if id.Name == "" {
		return nil, fmt.Errorf("observability: 初始化指标暴露需要服务名")
	}
	exp, reg, err := newPromExporter(id)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", promAddr)
	if err != nil {
		_ = exp.Shutdown(context.Background())
		return nil, fmt.Errorf("observability: 指标端点监听失败 (%s): %w", promAddr, err)
	}
	// 采集器的 MeterProvider 装为全局：OTel 原生埋点（如 pkg/middleware 的请求指标）
	// 与本采集器共用同一套 instrument 与 /metrics 端点——不装的话 otel.Meter 取到
	// 默认 provider，那些埋点会静默丢失（不报错、也没数据）。
	prevProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(exp.MeterProvider())
	srv := newMetricsServer(reg)
	serveHTTP(srv, ln, "指标端点")
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
			otel.SetMeterProvider(prevProvider)
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

// newMetricsServer 构造抓取端点的 HTTP 服务端：/metrics 由本包自建的注册表暴露
// （运行时指标与 OTel 采集指标同一份注册表，见 newPromRegistry）。
func newMetricsServer(reg *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

// serveHTTP 后台启动 HTTP 服务端：异常退出经日志暴露，不阻断进程启动
// （正常关闭（http.ErrServerClosed）不算错误）。
func serveHTTP(srv *http.Server, ln net.Listener, what string) {
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			atlaslog.Errorf("observability: %s异常退出: %v", what, err)
		}
	}()
}

// newPromExporter 建「含 Go 运行时与进程指标」的注册表与 OTel 采集器。
//
// 为什么自建注册表：框架 contrib/metrics/otel 的 Exporter 持有**私有**注册表，
// 未开放「追加 collector」的接缝，故走框架明确支持的 WithReader 接缝交回自建 reader；
// 指标口径与框架内置端点逐项对齐（见 promOptions），差别只在注册表里多了 go_* / process_*。
// 采集器本身仍由框架实现提供（不手写指标）。
func newPromExporter(id ServiceIdentity) (*otelmetrics.Exporter, *prometheus.Registry, error) {
	reg, err := newPromRegistry()
	if err != nil {
		return nil, nil, err
	}
	reader, err := otelprom.New(promOptions(reg, id)...)
	if err != nil {
		return nil, nil, fmt.Errorf("observability: 构建 Prometheus 导出器失败: %w", err)
	}
	exp, err := otelmetrics.New(
		otelmetrics.WithReader(reader),
		otelmetrics.WithConstLabels(id.metricLabels()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("observability: 构建指标采集器失败: %w", err)
	}
	return exp, reg, nil
}

// newPromRegistry 建注册表并登记 Go 运行时与进程指标（client_golang 官方 collector，
// 不手写一套）：go_*（goroutine / 堆内存 / GC）与 process_*（常驻内存 / 打开 fd / CPU），
// 是长跑稳定性（内存与句柄增长、goroutine 泄漏）的判据来源（投产清单 OBS-4）。
func newPromRegistry() (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, fmt.Errorf("observability: 注册进程指标失败: %w", err)
	}
	return reg, nil
}

// promOptions 返回与框架内置端点逐项对齐的 Prometheus 导出选项：
// 不加计数后缀、不带 scope / unit / target 信息（口径一变，既有面板与告警的指标名即漂移），
// 并把服务身份四项作为常量标签附加到全部导出指标（与 OTel 资源属性同一份来源）。
func promOptions(reg *prometheus.Registry, id ServiceIdentity) []otelprom.Option {
	opts := []otelprom.Option{
		otelprom.WithRegisterer(reg),
		otelprom.WithoutCounterSuffixes(),
		otelprom.WithoutScopeInfo(),
		otelprom.WithoutUnits(),
		otelprom.WithoutTargetInfo(),
	}
	labels := id.metricLabels()
	if len(labels) == 0 {
		return opts
	}
	keys := make([]attribute.Key, 0, len(labels))
	for k := range labels {
		keys = append(keys, attribute.Key(k))
	}
	return append(opts, otelprom.WithResourceAsConstantLabels(attribute.NewAllowKeysFilter(keys...)))
}
