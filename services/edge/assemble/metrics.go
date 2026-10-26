package assemble

// 本文件是进程内形态的指标接线：抓取端点与进程形态走**同一份** observability.InitMetrics，
// 采集器再经 bootstrap.BootWithCollector 注入依赖图。
//
// 两步缺一不可：只建端点不注入采集器，/metrics 上就只有 go_* / process_*（client_golang 的
// 运行时与进程采集器），一条 edge_* 都没有——2026-10-26 诊断现场抓到的正是这个形态。
//
// 另一条口径必须写在这里：指标序列在**装配期预注册**（框架 newMetricSink / NewResolveCacheCounters
// 先建序列再记 0，见 contrib/edge），故接入层一启动，/metrics 上就有全部 edge_* 家族（值为 0）。
// 这条在 2026-10-27 修正过：旧行为是「OTel 只在第一次观测后建序列」，空闲实例上一条 edge_* 都
// 没有，于是「抓不到」被反复读成「计数为 0 = 没发生」。现在「抓不到」只可能意味着采集器没注入
// （那形态下只剩 go_* / process_*），不再是正常形态。
//
// 边界：InitMetrics 会把 MeterProvider 装成**进程全局**（与进程形态同一份实现），故一个进程
// 只应起一个抓取端点（多实例同进程会把全局 provider 互相覆盖）。压测栈/e2e 不配 MetricsAddr 时
// 仍是 noop，行为与改造前一致。

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	"github.com/huangyuCN/atlas/metrics"
)

// metricsEndpoint 是抓取端点的句柄：采集器（注入依赖图用）、抓取 URL（驱动方/测试用）与关停。
type metricsEndpoint struct {
	collector metrics.Collector
	url       string
	endpoint  *observability.Metrics
}

// startMetrics 按 addr 建立 Prometheus 抓取端点；空地址返回 noop（不监听、热路径零开销）。
// addr 允许 127.0.0.1:0（内核分配端口，测试用），此时抓取 URL 由实际地址回填。
func startMetrics(cfg *conf.Bootstrap, addr string) (*metricsEndpoint, error) {
	if addr == "" {
		return &metricsEndpoint{collector: metrics.Noop()}, nil
	}
	m, err := observability.InitMetrics(addr, bootstrap.ServiceIdentity(cfg))
	if err != nil {
		return nil, fmt.Errorf("assemble: 指标抓取端点启动失败（%s）: %w", addr, err)
	}
	return &metricsEndpoint{
		collector: m.Collector,
		url:       "http://" + m.Addr + "/metrics",
		endpoint:  m,
	}, nil
}

// shutdown 关闭抓取端点（未启用时为空操作，幂等）。
func (m *metricsEndpoint) shutdown(ctx context.Context) error {
	if m == nil || m.endpoint == nil {
		return nil
	}
	return m.endpoint.Shutdown(ctx)
}
