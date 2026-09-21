// Package observability 提供服务侧可观测性的初始化装配：
// 链路追踪经 OTLP 导出（gRPC 或 HTTP，由端点 scheme 决定）到 Tempo 等后端，
// 资源属性带 service.name/version/instance.id/deployment.environment 与采样器；
// 指标经 contrib/metrics/otel 采集并以 Prometheus 文本端点暴露；
// 两者未配置端点时均跳过初始化——provider/采集器退化为 noop，热路径零开销。
// 业务侧用 StartSpan/EndSpan 埋点（见 span.go），Grafana 面板统一查询
// 日志（Loki）、链路（Tempo）与指标（Prometheus）。
package observability

import (
	"context"
	"fmt"
	"net/url"

	atlaslog "github.com/huangyuCN/atlas/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// TraceSampler 是链路采样器种类（零值 = 按比率采样且跟随父级）。
type TraceSampler int

// 采样器的全部取值。
const (
	TraceSamplerParentBasedRatio TraceSampler = iota // 跟随上游决策；根 span 按 SampleRatio 抽取（默认）
	TraceSamplerAlwaysOn                             // 全量采集
	TraceSamplerAlwaysOff                            // 全量丢弃
)

// traceSamplerNames 是采样器名下标表（下标 = 枚举值）。
var traceSamplerNames = [...]string{
	TraceSamplerParentBasedRatio: "parent_based_ratio",
	TraceSamplerAlwaysOn:         "always_on",
	TraceSamplerAlwaysOff:        "always_off",
}

// String 返回采样器名（日志用）；越界值返回 unknown(n) 以便自解释。
func (s TraceSampler) String() string {
	if int(s) >= 0 && int(s) < len(traceSamplerNames) {
		return traceSamplerNames[s]
	}
	return fmt.Sprintf("unknown(%d)", int(s))
}

// TracingOptions 是链路追踪初始化参数（runtime 身份 + observability 配置）。
type TracingOptions struct {
	// Endpoint 是 OTLP 导出端点，必须带 scheme：grpc://（明文 gRPC）/grpcs://（TLS gRPC）/
	// http://（OTLP/HTTP 明文）/https://（OTLP/HTTP+TLS）；空 = 不导出（noop）。
	Endpoint string
	// ServiceName 注入 service.name（必填）。
	ServiceName string
	// ServiceID 注入 service.instance.id（空则不注入）。
	ServiceID string
	// ServiceVersion 注入 service.version（空则不注入）。
	ServiceVersion string
	// Env 注入 deployment.environment.name（空则不注入）。
	Env string
	// Sampler 是采样器种类（零值 = 跟随父级、按 SampleRatio 抽根）。
	Sampler TraceSampler
	// SampleRatio 是根 span 采样率（0..1），仅 Sampler=TraceSamplerParentBasedRatio 使用。
	SampleRatio float64
}

// InitTracing 注册 OTLP 链路导出（按端点 scheme 选 gRPC/HTTP 导出器）：
// Endpoint 为空时直接返回 noop（不注册 provider，配置缺失不阻断启动）；
// 采样器与采样率、端点格式非法时快速失败（配置错误不静默降级）。
func InitTracing(ctx context.Context, opts TracingOptions) (shutdown func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }
	if opts.Endpoint == "" {
		return noop, nil
	}
	if opts.ServiceName == "" {
		return noop, fmt.Errorf("observability: 初始化链路导出需要服务名")
	}
	if err := opts.validate(); err != nil {
		return noop, err
	}
	res, err := buildResource(ctx, opts)
	if err != nil {
		return noop, err
	}
	exp, err := newTraceExporter(ctx, opts.Endpoint)
	if err != nil {
		return noop, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(newSampler(opts)),
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
	atlaslog.Infof("observability: OTLP 链路导出已启用 (%s, sampler=%s, ratio=%.2f)",
		opts.Endpoint, opts.Sampler, opts.SampleRatio)
	return shutdown, nil
}

// validate 校验采样器与采样率（配置错误启动即失败）。
func (o TracingOptions) validate() error {
	switch o.Sampler {
	case TraceSamplerParentBasedRatio, TraceSamplerAlwaysOn, TraceSamplerAlwaysOff:
	default:
		return fmt.Errorf("observability: 未知采样器 %d", int(o.Sampler))
	}
	if o.SampleRatio < 0 || o.SampleRatio > 1 {
		return fmt.Errorf("observability: sample_ratio 必须在 [0,1]，收到 %v", o.SampleRatio)
	}
	return nil
}

// buildResource 构造链路资源：服务身份按 semconv 注入（空值跳过），
// 另带 telemetry.sdk.* 便于后端识别 SDK 版本。
func buildResource(ctx context.Context, opts TracingOptions) (*sdkresource.Resource, error) {
	attrs := []attribute.KeyValue{semconv.ServiceName(opts.ServiceName)}
	if opts.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(opts.ServiceVersion))
	}
	if opts.ServiceID != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(opts.ServiceID))
	}
	if opts.Env != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentNameKey.String(opts.Env))
	}
	res, err := sdkresource.New(ctx,
		sdkresource.WithAttributes(attrs...),
		sdkresource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: 构造链路资源失败: %w", err)
	}
	return res, nil
}

// newSampler 按配置构造采样器：默认 ParentBased(TraceIDRatioBased)，
// 即跟随上游采样决策、根 span 按比率抽取。
func newSampler(opts TracingOptions) sdktrace.Sampler {
	switch opts.Sampler {
	case TraceSamplerAlwaysOn:
		return sdktrace.AlwaysSample()
	case TraceSamplerAlwaysOff:
		return sdktrace.NeverSample()
	default:
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(opts.SampleRatio))
	}
}

// newTraceExporter 按端点 scheme 选择 OTLP 导出器：
// grpc/grpcs → OTLP/gRPC（明文/TLS）；http/https → OTLP/HTTP（明文/TLS）。
// 缺 scheme、未知 scheme 或缺少 host 一律报错（配置错误不静默降级）。
func newTraceExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("observability: OTLP 端点 %q 解析失败: %w", endpoint, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("observability: OTLP 端点 %q 必须形如 scheme://host:port（支持 grpc/grpcs/http/https）", endpoint)
	}
	switch u.Scheme {
	case "grpc":
		return otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(u.Host), otlptracegrpc.WithInsecure())
	case "grpcs":
		return otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(u.Host))
	case "http":
		return otlptracehttp.New(ctx, append(httpOptions(u), otlptracehttp.WithInsecure())...)
	case "https":
		return otlptracehttp.New(ctx, httpOptions(u)...)
	default:
		return nil, fmt.Errorf("observability: 不支持的 OTLP scheme %q（支持 grpc/grpcs/http/https）", u.Scheme)
	}
}

// httpOptions 组装 OTLP/HTTP 公共选项：端点 + 可选路径
// （不带 path 时用导出器默认 /v1/traces）。
func httpOptions(u *url.URL) []otlptracehttp.Option {
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(u.Host)}
	if u.Path != "" && u.Path != "/" {
		opts = append(opts, otlptracehttp.WithURLPath(u.Path))
	}
	return opts
}
