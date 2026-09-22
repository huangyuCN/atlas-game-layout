package bootstrap

import (
	"context"
	"fmt"
	"os"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/lib/version"
	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/pkg/enumconv"
	pkglog "github.com/huangyuCN/atlas-game-layout/pkg/log"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	atlaslog "github.com/huangyuCN/atlas/log"
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
	if err := fillRuntimeIdentity(runtime); err != nil {
		return nil, err
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

	// 服务身份（runtime.name/id/version/env）：链路资源属性与指标常量标签同源。
	identity := serviceIdentityOf(like)
	atlaslog.Infof("服务启动: name=%s id=%s build=%s", identity.Name, identity.ID, version.String())

	// OTLP 链路导出（端点未配置时 noop）：资源带服务身份、采样器与采样率可配；
	// 服务停止时 flush 未导出的 span。
	traceOpts, err := tracingOptionsOf(like, identity)
	if err != nil {
		return nil, err
	}
	shutdownTrace, err := observability.InitTracing(context.Background(), traceOpts)
	if err != nil {
		return nil, err
	}

	// 指标采集与 Prometheus 抓取端点（未配置时 noop 采集器，热路径零开销）：
	// 采集器以 metrics.Collector 接口注入依赖图（actor 运行时与业务打点共用），
	// 常量标签带同一份服务身份；服务停止时关闭抓取端点与底层 provider。
	m, err := observability.InitMetrics(
		like.GetObservability().GetMetrics().GetPrometheus(), identity)
	if err != nil {
		return nil, err
	}

	opts := []fx.Option{
		fx.Supply(cfg),
		fx.Provide(func() metrics.Collector { return m.Collector }),
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{OnStop: shutdownTrace})
			lc.Append(fx.Hook{OnStop: m.Shutdown})
		}),
		Module(Options{Name: runtime.GetName(), ID: runtime.GetId(), Version: version.Version}),
	}
	opts = append(opts, extra...)
	return opts, nil
}

// defaultSampleRatio 是 sample_ratio 未配置时的默认采样率（全量采集）。
const defaultSampleRatio = 1.0

// fillRuntimeIdentity 原地回填 runtime 身份缺省值（下游全部消费方共用这一份）：
// id 缺省取主机名——注册实例 ID、actor NodeID 与指标 service_instance_id 必须同源，
// 留空会让三处各不相同（atlas App 会为注册实例另生成 UUID），且同名服务多副本
// 部署在不同主机时主机名天然唯一（同主机多副本需各自配置端口与 id）；
// env 缺省 default——注册中心键前缀按环境隔离，见 pkg/registry.NamespaceOf。
func fillRuntimeIdentity(runtime *configspb.Runtime) error {
	if runtime.GetId() == "" {
		host, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("bootstrap: runtime.id 未配置且获取主机名失败: %w", err)
		}
		if host == "" {
			return fmt.Errorf("bootstrap: runtime.id 未配置且主机名为空")
		}
		runtime.Id = host
	}
	if runtime.GetEnv() == "" {
		runtime.Env = consts.EnvDefault
	}
	return nil
}

// serviceIdentityOf 从 runtime 配置提取服务身份（链路资源属性与指标常量标签共用）：
// 版本缺省取构建注入值（lib/version），配置 runtime.version 可覆盖。
func serviceIdentityOf(like ConfigLike) observability.ServiceIdentity {
	runtime := like.GetRuntime()
	ver := runtime.GetVersion()
	if ver == "" {
		ver = version.Version
	}
	return observability.ServiceIdentity{
		Name:    runtime.GetName(),
		ID:      runtime.GetId(),
		Version: ver,
		Env:     runtime.GetEnv(),
	}
}

// tracingOptionsOf 把 observability 配置与身份映射为追踪初始化参数：
// proto 采样器枚举 → Go 枚举（未知取值快速失败），sample_ratio 缺省 1.0。
func tracingOptionsOf(like ConfigLike, identity observability.ServiceIdentity) (observability.TracingOptions, error) {
	obs := like.GetObservability()
	sampler, err := traceSamplerOf(obs.GetSampler())
	if err != nil {
		return observability.TracingOptions{}, err
	}
	ratio := defaultSampleRatio
	if obs != nil && obs.SampleRatio != nil {
		ratio = *obs.SampleRatio
	}
	return observability.TracingOptions{
		Identity:    identity,
		Endpoint:    obs.GetOtlp(),
		Sampler:     sampler,
		SampleRatio: ratio,
	}, nil
}

// traceSamplerOf 把 proto 采样器枚举映射为 observability.TraceSampler（未知取值报错）。
func traceSamplerOf(s configspb.TraceSampler) (observability.TraceSampler, error) {
	return enumconv.Map(s, map[configspb.TraceSampler]observability.TraceSampler{
		configspb.TraceSampler_TRACE_SAMPLER_PARENT_BASED_RATIO: observability.TraceSamplerParentBasedRatio,
		configspb.TraceSampler_TRACE_SAMPLER_ALWAYS_ON:          observability.TraceSamplerAlwaysOn,
		configspb.TraceSampler_TRACE_SAMPLER_ALWAYS_OFF:         observability.TraceSamplerAlwaysOff,
	}, "采样器")
}
