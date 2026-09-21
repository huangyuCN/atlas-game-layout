// Package observability 提供服务侧可观测性的初始化装配：
// 链路追踪经 OTLP gRPC 导出到后端（服务器测试环境为 Tempo；Grafana 面板查询），
// 指标经 contrib/metrics/otel 采集并以 Prometheus 文本端点暴露（Grafana 面板查询）。
// 端点未配置时跳过初始化——采集器退化为 noop，热路径零开销。
package observability

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	otelmetrics "github.com/huangyuCN/atlas/contrib/metrics/otel"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/metrics"
)

// noopShutdown 是未配置时的空关停函数（noop：无需 flush、无需关闭）。
func noopShutdown(context.Context) error { return nil }

// lastEndpointAddr 记录最近一次创建的抓取端点地址（host:port；仅测试断言用，
// InitMetrics 进程内仅初始化一次，无并发争用）。
var lastEndpointAddr string

// InitMetrics 建立指标采集器与 Prometheus 抓取端点：
// promAddr 为空时返回 noop 采集器（不监听端口、零开销）——配置缺失不阻断启动；
// serviceName 作为 service 常量标签附加到全部导出指标（面板按服务区分）。
// 返回的 shutdown 负责关闭抓取端点与底层 MeterProvider（服务停止时调用，幂等）。
func InitMetrics(promAddr, serviceName string) (metrics.Collector, func(context.Context) error, error) {
	if promAddr == "" {
		return metrics.Noop(), noopShutdown, nil
	}
	if serviceName == "" {
		return metrics.Noop(), noopShutdown, fmt.Errorf("observability: 初始化指标暴露需要服务名")
	}
	exp, err := otelmetrics.New(
		otelmetrics.WithConstLabels(map[string]string{"service": serviceName}),
	)
	if err != nil {
		return metrics.Noop(), noopShutdown, fmt.Errorf("observability: 构建指标采集器失败: %w", err)
	}

	ln, err := net.Listen("tcp", promAddr)
	if err != nil {
		_ = exp.Shutdown(context.Background())
		return metrics.Noop(), noopShutdown, fmt.Errorf("observability: 指标端点监听失败 (%s): %w", promAddr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", exp.Handler())
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	lastEndpointAddr = ln.Addr().String()
	atlaslog.Infof("observability: Prometheus 抓取端点已启用 (http://%s/metrics)", ln.Addr())

	shutdown := func(ctx context.Context) error {
		if err := srv.Shutdown(ctx); err != nil {
			atlaslog.Warnf("observability: 指标端点关闭失败: %v", err)
		}
		if err := exp.Shutdown(ctx); err != nil {
			atlaslog.Warnf("observability: 指标 provider 关闭失败: %v", err)
		}
		return nil
	}
	return exp.Collector(), shutdown, nil
}
