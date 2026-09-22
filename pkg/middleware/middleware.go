// Package middleware 提供传输层中间件链的默认装配：
// 服务端默认链 = 链路追踪（extract 上游 traceparent + 生成 server span）→ 请求日志 → 请求指标；
// 客户端链 = 链路追踪（注入 traceparent），供 gRPC 拨号挂载，使跨服务链路连通。
// 中间件是函数值、配置表达不了，故装配点是代码层——但默认链与注入点收口在本包，
// 业务用 fx.Decorate 追加自己的中间件（鉴权/限流等），不需要改各服务构造代码。
package middleware

import (
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/middleware/logging"
	mwmetrics "github.com/huangyuCN/atlas/middleware/metrics"
	"github.com/huangyuCN/atlas/middleware/tracing"
	"go.opentelemetry.io/otel"
	"go.uber.org/fx"
)

// serverSecondsHistogramName 是请求耗时直方图的 instrument 名
// （Prometheus 侧导出为 <name>_bucket；atlas 的建议名自带 _bucket，会重复后缀，故不用）。
const serverSecondsHistogramName = "server_requests_seconds"

// Module 提供服务端/客户端中间件链与 HTTP 过滤器链的默认值：
// 业务侧用 fx.Decorate 追加（如 fx.Decorate(func(f serverutil.Filters) serverutil.Filters {...})）。
var Module = fx.Module("middleware",
	fx.Provide(Server, Client, Filters),
)

// Server 返回服务端默认中间件链：追踪 → 日志 → 指标。
// 追踪排在最前，使 server span 覆盖后续全部处理（日志与指标在内层各自观测）。
// 不含 recovery：HTTP/gRPC 服务端已内置 panic 恢复，重复挂载会双重恢复与双重打点。
func Server() (serverutil.Middlewares, error) {
	meter := otel.Meter(consts.MeterNameTransport)
	requests, err := mwmetrics.DefaultRequestsCounter(meter, mwmetrics.DefaultServerRequestsCounterName)
	if err != nil {
		return nil, fmt.Errorf("middleware: 创建请求计数指标失败: %w", err)
	}
	seconds, err := mwmetrics.DefaultSecondsHistogram(meter, serverSecondsHistogramName)
	if err != nil {
		return nil, fmt.Errorf("middleware: 创建请求耗时指标失败: %w", err)
	}
	return serverutil.Middlewares{
		tracing.Server(tracing.WithTracerName(consts.TracerNameTransport)),
		logging.Server(atlaslog.GetLogger()),
		mwmetrics.Server(mwmetrics.WithRequests(requests), mwmetrics.WithSeconds(seconds)),
	}, nil
}

// Client 返回客户端默认中间件链：注入 traceparent（跨服务链路的另一半，
// 只挂服务端会得到「每个服务各自一个 root span」）。
func Client() serverutil.ClientMiddlewares {
	return serverutil.ClientMiddlewares{
		tracing.Client(tracing.WithTracerName(consts.TracerNameTransport)),
	}
}

// Filters 返回 HTTP 过滤器链默认值：默认空——协议层过滤器（CORS/gzip/pprof）按需由
// 业务 fx.Decorate 追加，本函数只提供注入点，保证 fx 图里有该类型的提供者。
func Filters() serverutil.Filters {
	return nil
}
