// Package observability 提供服务侧可观测性的初始化装配：
// 链路追踪经 OTLP gRPC 导出到后端（服务器测试环境为 Tempo），
// 指标经 contrib/metrics/otel 采集并以 Prometheus 文本端点暴露；
// 两者未配置端点时均跳过初始化——provider/采集器退化为 noop，热路径零开销。
// Grafana 面板统一查询日志（Loki）、链路（Tempo）与指标（Prometheus）。
package observability

import (
	"context"
	"fmt"

	atlaslog "github.com/huangyuCN/atlas/log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// InitTracing 注册 OTLP 链路导出（按服务名标记 resource）：
// otlpAddr 为空时直接返回 nil（noop；配置缺失不阻断启动）。
// 成功后返回的 shutdown 在服务停止时调用（flush 未导出的 span）。
func InitTracing(ctx context.Context, otlpAddr, serviceName string) (shutdown func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }
	if otlpAddr == "" {
		return noop, nil
	}
	if serviceName == "" {
		return noop, fmt.Errorf("observability: 初始化链路导出需要服务名")
	}
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(otlpAddr), otlptracegrpc.WithInsecure())
	if err != nil {
		return noop, fmt.Errorf("observability: 构造 OTLP 导出器失败: %w", err)
	}
	res := sdkresource.NewSchemaless(
		attribute.String("service.name", serviceName),
	)
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	// W3C TraceContext 传播：actor 集群信封（env.Trace）与之同语义往返。
	otel.SetTextMapPropagator(propagation.TraceContext{})
	shutdown = func(ctx context.Context) error {
		if err := provider.Shutdown(ctx); err != nil {
			atlaslog.Warnf("observability: 链路导出关闭失败: %v", err)
		}
		return nil
	}
	atlaslog.Infof("observability: OTLP 链路导出已启用 (%s)", otlpAddr)
	return shutdown, nil
}
