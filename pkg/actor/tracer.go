package actor

import (
	"github.com/huangyuCN/atlas/contrib/actor/core"
	oteladapter "github.com/huangyuCN/atlas/contrib/actor/observe/otel"
	"go.opentelemetry.io/otel"
)

// DefaultTracer 返回基于 OTel 全局 tracer provider 的 core.Tracer 适配：
// 未配置 OTel SDK（exporter）时全局 provider 为 noop——不产生遥测数据、
// 热路径零开销；开发者按部署配置 exporter（otlp/jaeger 等）注册到全局
// provider 后自动生效，业务代码与装配无需改动。
func DefaultTracer() core.Tracer {
	return oteladapter.New(otel.GetTracerProvider().Tracer("atlas-actor"))
}
