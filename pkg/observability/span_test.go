package observability

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
)

// withInMemoryTracer 把内存导出器装为全局 provider，用例结束恢复原 provider。
func withInMemoryTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(old)
		_ = tp.Shutdown(context.Background())
	})
	return exp
}

// TestStartSpanEndSpan 验证业务 span 的名称、属性与错误状态。
func TestStartSpanEndSpan(t *testing.T) {
	exp := withInMemoryTracer(t)

	_, span := StartSpan(context.Background(), "game.Player.Register",
		attribute.String("player.account", "acc-1"))
	EndSpan(span, errors.New("boom"))

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("span 数 = %d, 期望 1", len(spans))
	}
	got := spans[0]
	if got.Name != "game.Player.Register" {
		t.Fatalf("span 名 = %q, 期望 game.Player.Register", got.Name)
	}
	if got.Status.Code != codes.Error {
		t.Fatalf("span 状态 = %v, 期望 Error", got.Status.Code)
	}
	if len(got.Events) == 0 {
		t.Fatal("错误未记录为 span 事件")
	}
	found := false
	for _, kv := range got.Attributes {
		if string(kv.Key) == "player.account" && kv.Value.AsString() == "acc-1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("缺少业务属性: %v", got.Attributes)
	}
}

// TestEndSpanOK 验证成功路径不置错误状态。
func TestEndSpanOK(t *testing.T) {
	exp := withInMemoryTracer(t)

	_, span := StartSpan(context.Background(), "battle.Battle.GetBattle")
	EndSpan(span, nil)

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("span 数 = %d, 期望 1", len(spans))
	}
	if spans[0].Status.Code == codes.Error {
		t.Fatalf("成功路径不应置 Error: %+v", spans[0].Status)
	}
}

// TestStartSpanNoopProvider 验证未配置 exporter（noop provider）时不 panic、不记录。
func TestStartSpanNoopProvider(t *testing.T) {
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(noop.NewTracerProvider())
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	_, span := StartSpan(context.Background(), "game.Player.Login")
	if span.IsRecording() {
		t.Fatal("noop provider 下 span 不应处于记录状态")
	}
	EndSpan(span, errors.New("ignored"))
}
