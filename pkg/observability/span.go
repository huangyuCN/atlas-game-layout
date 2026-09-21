package observability

import (
	"context"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// StartSpan 在 ctx 上开启业务 span（biz 层埋点统一入口）：
// 未配置 exporter 时全局 provider 为 noop，调用零开销；上游 ctx 已带
// actor/gateway span 时，新 span 自动挂为其子 span。
// scope 名统一取 lib/consts.TracerNameBiz，便于集中改名。
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(consts.TracerNameBiz).Start(ctx, name, trace.WithAttributes(attrs...))
}

// EndSpan 结束 span：err 非 nil 时记录错误事件并把状态置为 Error。
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
