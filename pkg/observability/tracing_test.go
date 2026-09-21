package observability

import (
	"context"
	"strings"
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// TestNewTraceExporterScheme 验证按端点 scheme 选择 OTLP 导出器；缺 scheme/未知 scheme 报错。
func TestNewTraceExporterScheme(t *testing.T) {
	ok := []string{
		"grpc://127.0.0.1:4317",
		"grpcs://collector.internal:4317",
		"http://127.0.0.1:4318",
		"https://collector.internal:4318/v1/traces",
	}
	for _, endpoint := range ok {
		exp, err := newTraceExporter(context.Background(), endpoint)
		if err != nil {
			t.Fatalf("%s: newTraceExporter() 错误 = %v", endpoint, err)
		}
		if exp == nil {
			t.Fatalf("%s: exporter 为 nil", endpoint)
		}
		_ = exp.Shutdown(context.Background())
	}

	bad := []string{
		"127.0.0.1:4317",       // 缺 scheme
		"ftp://127.0.0.1:4317", // 未知 scheme
		"grpc://",              // 缺 host
		"://127.0.0.1:4317",    // 非法 URL
	}
	for _, endpoint := range bad {
		if _, err := newTraceExporter(context.Background(), endpoint); err == nil {
			t.Fatalf("%s: 期望报错，实际为 nil", endpoint)
		}
	}
}

// TestBuildResource 验证资源属性：身份字段注入、空值不注入、telemetry.sdk 随行。
func TestBuildResource(t *testing.T) {
	res, err := buildResource(context.Background(), TracingOptions{
		Identity: ServiceIdentity{Name: "game", ID: "game-1", Version: "v1.2.3", Env: "test"},
	})
	if err != nil {
		t.Fatalf("buildResource() 错误 = %v", err)
	}
	got := map[string]string{}
	for _, kv := range res.Attributes() {
		got[string(kv.Key)] = kv.Value.AsString()
	}
	want := map[string]string{
		string(semconv.ServiceNameKey):               "game",
		string(semconv.ServiceInstanceIDKey):         "game-1",
		string(semconv.ServiceVersionKey):            "v1.2.3",
		string(semconv.DeploymentEnvironmentNameKey): "test",
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("资源属性 %s = %q, 期望 %q（全部属性 %v）", k, got[k], v, got)
		}
	}
	if _, ok := got["telemetry.sdk.name"]; !ok {
		t.Fatalf("缺少 telemetry.sdk.* 属性: %v", got)
	}

	// 空值不注入（只保留 service.name）。
	res, err = buildResource(context.Background(), TracingOptions{Identity: ServiceIdentity{Name: "game"}})
	if err != nil {
		t.Fatalf("buildResource() 错误 = %v", err)
	}
	for _, kv := range res.Attributes() {
		switch string(kv.Key) {
		case string(semconv.ServiceInstanceIDKey), string(semconv.ServiceVersionKey), string(semconv.DeploymentEnvironmentNameKey):
			t.Fatalf("空值不应注入属性 %s", kv.Key)
		}
	}
}

// TestNewSampler 验证三种采样器构造。
func TestNewSampler(t *testing.T) {
	cases := []struct {
		name string
		opts TracingOptions
		want string
	}{
		{"默认按比率且跟随父级", TracingOptions{SampleRatio: 0.5}, "ParentBased{root:TraceIDRatioBased{0.5}"},
		{"全量", TracingOptions{Sampler: TraceSamplerAlwaysOn}, "AlwaysOnSampler"},
		{"全关", TracingOptions{Sampler: TraceSamplerAlwaysOff}, "AlwaysOffSampler"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := newSampler(tc.opts).Description()
			if !strings.Contains(got, tc.want) {
				t.Fatalf("sampler = %q, 期望包含 %q", got, tc.want)
			}
		})
	}
}

// TestInitTracingValidation 验证初始化参数校验与 noop 分支。
func TestInitTracingValidation(t *testing.T) {
	ctx := context.Background()

	// 空端点：noop，不报错。
	shutdown, err := InitTracing(ctx, TracingOptions{})
	if err != nil {
		t.Fatalf("空端点不应报错: %v", err)
	}
	if err := shutdown(ctx); err != nil {
		t.Fatalf("noop shutdown 应为空操作: %v", err)
	}

	// 配了端点但缺服务名。
	if _, err := InitTracing(ctx, TracingOptions{Endpoint: "grpc://127.0.0.1:4317"}); err == nil {
		t.Fatal("缺服务名应报错")
	}
	// 非法采样率。
	if _, err := InitTracing(ctx, TracingOptions{
		Endpoint: "grpc://127.0.0.1:4317", Identity: ServiceIdentity{Name: "game"}, SampleRatio: 1.5,
	}); err == nil {
		t.Fatal("sample_ratio 越界应报错")
	}
	// 未知采样器。
	if _, err := InitTracing(ctx, TracingOptions{
		Endpoint: "grpc://127.0.0.1:4317", Identity: ServiceIdentity{Name: "game"}, Sampler: TraceSampler(9),
	}); err == nil {
		t.Fatal("未知采样器应报错")
	}
	// 非法端点 scheme。
	if _, err := InitTracing(ctx, TracingOptions{
		Endpoint: "127.0.0.1:4317", Identity: ServiceIdentity{Name: "game"},
	}); err == nil {
		t.Fatal("缺 scheme 端点应报错")
	}
}

// TestTraceSamplerString 验证采样器名称（日志用）。
func TestTraceSamplerString(t *testing.T) {
	cases := map[TraceSampler]string{
		TraceSamplerParentBasedRatio: "parent_based_ratio",
		TraceSamplerAlwaysOn:         "always_on",
		TraceSamplerAlwaysOff:        "always_off",
		TraceSampler(9):              "unknown(9)",
	}
	for sampler, want := range cases {
		if got := sampler.String(); got != want {
			t.Fatalf("TraceSampler(%d).String() = %q, 期望 %q", int(sampler), got, want)
		}
	}
}
