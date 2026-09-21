// Package spanstest 提供测试用的内存 span 导出装置：把 SDK provider 装为全局，
// 供 biz 层埋点断言 span 名/属性/错误状态。仅测试依赖，不参与生产装配。
package spanstest

import (
	"context"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Install 把内存导出器装为全局 TracerProvider，返回导出器与恢复函数
// （调用方用 defer/t.Cleanup 恢复原 provider）。
func Install() (*tracetest.InMemoryExporter, func()) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	return exp, func() {
		otel.SetTracerProvider(old)
		_ = tp.Shutdown(context.Background())
	}
}

// ByName 把导出的 span 按名字索引（同名只保留最后一个）。
func ByName(exp *tracetest.InMemoryExporter) map[string]tracetest.SpanStub {
	out := map[string]tracetest.SpanStub{}
	for _, s := range exp.GetSpans() {
		out[s.Name] = s
	}
	return out
}

// Attr 读取 span 属性值（不存在返回空串）。
func Attr(s tracetest.SpanStub, key string) string {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	return ""
}
