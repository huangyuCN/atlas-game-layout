# 链路追踪增强设计（资源属性 / 采样 / 端点 scheme / biz 埋点）

- 日期：2026-09-21
- 状态：已定稿（使用者确认：枚举采样器 + 可选比率 / 端点强制显式 scheme / biz 覆盖全部 handler 公开方法；
  沿用本会话「不做兼容处理、直接破坏性变更」原则）
- 范围：`pkg/observability`（追踪初始化与 span 辅助）、`protobuf/configs/observability.proto`、
  `pkg/bootstrap`（配置→初始化参数映射）、四个服务 `configs/config.yaml`、
  `services/*/internal/biz/handler`（埋点）。不改客户端协议、不改 Redis/actor 语义。

## 1. 背景

- `InitTracing` 只注册 `service.name`，实例 ID / 版本 / 环境都没进 Resource——
  Tempo/Grafana 里无法按实例或版本区分、无法按环境过滤。
- 没有设置采样器：SDK 默认 `ParentBased(AlwaysSample)`，全量采集；也没有采样率启动参数。
- 导出固定走 `otlptracegrpc`：想接 OTLP/HTTP 后端（如部分 SaaS、网关侧 4318 端口）只能改代码。
- biz 层（`services/*/internal/biz/handler`）没有任何 span：链路在 actor 边界就断了，
  业务内部耗时/错误不可见。

## 2. 决策表

| 议题 | 决策 |
|------|------|
| 资源属性 | semconv：`service.name` / `service.version` / `service.instance.id` / `deployment.environment.name`，另加 `telemetry.sdk.*`；空值不注入 |
| 采样器 | proto enum `TraceSampler`：`PARENT_BASED_RATIO`（默认，跟随父级、根按比率）/ `ALWAYS_ON` / `ALWAYS_OFF` |
| 采样率 | `optional double sample_ratio`，缺省 1.0（避免「没配 = 0 = 全丢」），校验 0..1 |
| 端点 | 强制显式 scheme：`grpc://`（明文 gRPC）/`grpcs://`（TLS gRPC）/`http://`（OTLP/HTTP 明文）/`https://`（OTLP/HTTP+TLS）；缺失或未知启动报错 |
| HTTP 路径 | 端点带 path 时按给定路径；不带时用导出器默认 `/v1/traces` |
| biz 埋点 | 全部 handler 公开方法（15 个），span 名 `<service>.<接口>.<方法>`，错误记 `RecordError + SetStatus(Error)` |
| 不做 | 指标侧 resource 标签、host/process 资源属性、baggage/自定义 propagator |

## 3. 追踪初始化

`pkg/observability`：

```go
// TraceSampler 是采样器种类（零值 = 按比率、跟随父级）。
type TraceSampler int

const (
    TraceSamplerParentBasedRatio TraceSampler = iota
    TraceSamplerAlwaysOn
    TraceSamplerAlwaysOff
)

// TracingOptions 是链路追踪初始化参数（来自 runtime 与 observability 配置）。
type TracingOptions struct {
    Endpoint       string       // OTLP 端点，必须带 scheme
    ServiceName    string       // → service.name（必填）
    ServiceID      string       // → service.instance.id
    ServiceVersion string       // → service.version
    Env            string       // → deployment.environment.name
    Sampler        TraceSampler // 采样器
    SampleRatio    float64      // 仅 parent_based_ratio 使用；缺省由调用方填 1.0
}

func InitTracing(ctx context.Context, opts TracingOptions) (shutdown func(context.Context) error, err error)
```

- 端点为空：直接返回 noop shutdown（不注册 provider，零开销）。
- 服务名为空：报错（Resource 缺 service.name 无法定位）。
- `SampleRatio` 不在 [0,1]：报错。
- 采样器：`sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))` / `sdktrace.AlwaysSample()` / `sdktrace.NeverSample()`。
- Resource：`resource.New(ctx, resource.WithAttributes(attrs...), resource.WithTelemetrySDK())`。
- 全局副作用保持不变：`otel.SetTracerProvider(provider)` + `otel.SetTextMapPropagator(propagation.TraceContext{})`。

## 4. 端点解析

```go
// newTraceExporter 按端点 scheme 选择 OTLP 导出器。
func newTraceExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error)
```

| scheme | 导出器 | 传输安全 | 备注 |
|--------|--------|----------|------|
| `grpc` | `otlptracegrpc` | 明文（`WithInsecure`） | `WithEndpoint(host:port)` |
| `grpcs` | `otlptracegrpc` | TLS（默认凭据） | `WithEndpoint(host:port)` |
| `http` | `otlptracehttp` | 明文（`WithInsecure`） | `WithEndpoint(host:port)`，无 path 时默认 `/v1/traces` |
| `https` | `otlptracehttp` | TLS（默认） | 同上，path 非空时 `WithURLPath` |

缺 scheme / 未知 scheme / host 为空：返回错误，启动即失败（配置错误不静默降级）。

## 5. biz 埋点

`pkg/observability` 新增：

```go
// StartSpan 在 ctx 上开启业务 span；未配置 exporter（noop provider）时零开销。
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span)

// EndSpan 结束 span：err 非 nil 时 RecordError + SetStatus(Error)。
func EndSpan(span trace.Span, err error)
```

埋点清单（span 名 = `<service>.<接口>.<方法>`）：

| 服务 | 文件 | 方法 | 关键属性 |
|------|------|------|----------|
| game | `biz/handler/player.go` | `Player.Register` / `Player.Login` | `player.account` / `player.id` |
| game | `biz/handler/game.go` | `Game.GetPlayer` / `Game.GetBackpack` / `Game.GrantItem` | `player.id` / `item.id` |
| matcher | `biz/handler/matcher.go` | `Matcher.QueueMatch` / `Matcher.CancelMatch` / `Matcher.QueryMatch` | `player.id` / `ruleset` |
| matcher | `biz/handler/party.go` | `Matcher.CreateParty` / `JoinParty` / `LeaveParty` / `DescribeParty` / `QueueParty` | `player.id` / `party.id` |
| battle | `biz/handler/battle.go` | `Battle.CreateBattle` / `Battle.GetBattle` | `battle.id` |

- 方法签名改为具名错误返回值（`(rep *X, err error)`）+ `defer func() { observability.EndSpan(span, err) }()`。
- actor → biz 的 ctx 已带 actor span（`pkg/actor.DefaultTracer` 注入），biz span 自动成为其子 span，
  与 gateway 透传链路（relay → actor ask → biz）串成一条 trace。

## 6. proto 与配置

```proto
message Observability {
  string otlp = 1;              // OTLP 端点，必须带 scheme（grpc/grpcs/http/https）
  Metrics metrics = 2;
  TraceSampler sampler = 3;     // 采样器（缺省 = parent_based_ratio）
  optional double sample_ratio = 4; // 采样率 0..1（缺省 1.0；仅 parent_based_ratio 使用）
}

// TraceSampler 是链路采样器种类。
enum TraceSampler {
  TRACE_SAMPLER_PARENT_BASED_RATIO = 0; // 跟随父级；根 span 按 sample_ratio 抽
  TRACE_SAMPLER_ALWAYS_ON = 1;
  TRACE_SAMPLER_ALWAYS_OFF = 2;
}
```

四个 `config.yaml`：`otlp: grpc://127.0.0.1:4317`（原 `127.0.0.1:4317` 不再识别）。
`pkg/bootstrap` 负责 runtime + observability → `TracingOptions` 的映射（含 proto 枚举 → Go 枚举）。

## 7. 影响面

- `pkg/observability/tracing.go`（重写初始化）、新增 `pkg/observability/span.go`（biz span 辅助）；
- `protobuf/configs/observability.proto` + `observability.pb.go`（重新生成）；
- `pkg/bootstrap/assemble.go`（映射）；
- 四个 `services/*/configs/config.yaml`（端点 scheme）；
- `services/{game,matcher,battle}/internal/biz/handler/*.go`（15 个方法埋点）；
- 依赖新增 `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp`（与 grpc 同版本 v1.46.0）、
  `go.opentelemetry.io/otel/semconv/v1.43.0`（随 otel 模块）。
- 服务器：需重新同步 + 重启四服务；Tempo 端点仍是 127.0.0.1:4317（grpc）。

## 8. 测试

- `pkg/observability`：
  - 端点 scheme → 导出器选择（`grpc`/`grpcs`/`http`/`https` 成功；空 scheme / 未知 scheme / 空 host 报错）；
  - Resource 属性（`service.name/version/instance.id/deployment.environment.name`，空值不注入）；
  - 采样器构造（三值）与 `SampleRatio` 越界报错；
  - `StartSpan/EndSpan`：用 `tracetest.InMemoryExporter` 断言 span 名、属性、错误状态；noop provider 下不 panic。
- `pkg/bootstrap`：runtime + observability → `TracingOptions` 映射（含缺省比率 1.0、枚举映射）。
- biz handler：在既有 handler 测试中挂内存 exporter，断言公开方法产出预期名字的 span（错误路径记 Error）。
- 四服务 `config.yaml` 加载测试（已有 `TestServiceConfigLoads`）继续覆盖新字段解析。

## 9. 不做（YAGNI）

- 不加 host/process 资源属性（`WithHost`/`WithProcess`）——需要时再加。
- 不改指标侧 resource 标签（`service_version`/`env`/实例 ID）。
- 不引入自定义 propagator / baggage。
- 不为旧端点写法（无 scheme）做回落。
