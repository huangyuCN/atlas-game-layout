package bootstrap

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	pkglog "github.com/huangyuCN/atlas-game-layout/pkg/log"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/metrics"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

// ConfigLike 是各服务配置 Bootstrap 的公共 getter 子集：
// 所有服务的 internal/conf/conf.proto 都引用公共配置消息，
// 生成的 *Bootstrap 自动满足该接口。
type ConfigLike interface {
	GetRuntime() *configspb.Runtime
	GetRegistry() *configspb.Registry
	GetLog() *configspb.Log
	GetObservability() *configspb.Observability
}

// Assemble 按服务名加载配置并装配通用模块：
// 配置加载（services/<name>/configs/config.yaml）、日志初始化、
// Atlas App 装配（Module）。extra 用于追加服务自定义的 fx 模块。
//
// 注册中心（etcd 客户端 / Registrar）由各服务的 fx 模块自行声明，
// 可直接复用 pkg/fxkit 的泛型提供器。
func Assemble(name string, cfg proto.Message, extra ...fx.Option) ([]fx.Option, error) {
	if name == "" {
		return nil, fmt.Errorf("bootstrap: 服务名不能为空")
	}
	if err := config.FromService(name, cfg); err != nil {
		return nil, err
	}
	return AssembleLoaded(cfg, extra...)
}

// AssembleLoaded 对已加载的配置装配通用模块（拆分出来便于单测）：
// 日志（含文件输出，供采集器进 Loki）与链路导出（OTLP → Tempo，端点空则
// noop）在此统一初始化，四服务共用。
func AssembleLoaded(cfg proto.Message, extra ...fx.Option) ([]fx.Option, error) {
	like, ok := cfg.(ConfigLike)
	if !ok {
		return nil, fmt.Errorf("bootstrap: 配置类型 %T 未引用公共配置消息", cfg)
	}
	runtime := like.GetRuntime()
	if runtime == nil || runtime.GetName() == "" {
		return nil, fmt.Errorf("bootstrap: 配置缺少 runtime.name")
	}

	logOpts := pkglog.Options{Service: runtime.GetName()}
	if l := like.GetLog(); l != nil {
		logOpts.Level = l.GetLevel()
		logOpts.Format = l.GetFormat()
		logOpts.File = l.GetFile()
	}
	if err := pkglog.Init(logOpts); err != nil {
		return nil, err
	}

	// OTLP 链路导出（端点未配置时 noop）：服务停止时 flush 未导出的 span。
	shutdownTrace, err := observability.InitTracing(context.Background(),
		like.GetObservability().GetOtlp(), runtime.GetName())
	if err != nil {
		return nil, err
	}

	// 指标采集与 Prometheus 抓取端点（未配置时 noop 采集器，热路径零开销）：
	// 采集器以 metrics.Collector 接口注入依赖图（actor 运行时与业务打点共用），
	// 服务停止时关闭抓取端点与底层 provider。
	meter, shutdownMetrics, err := observability.InitMetrics(
		like.GetObservability().GetMetrics().GetPrometheus(), runtime.GetName())
	if err != nil {
		return nil, err
	}

	opts := []fx.Option{
		fx.Supply(cfg),
		fx.Provide(func() metrics.Collector { return meter }),
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{OnStop: shutdownTrace})
			lc.Append(fx.Hook{OnStop: shutdownMetrics})
		}),
		Module(Options{Name: runtime.GetName(), ID: runtime.GetId()}),
	}
	opts = append(opts, extra...)
	return opts, nil
}
