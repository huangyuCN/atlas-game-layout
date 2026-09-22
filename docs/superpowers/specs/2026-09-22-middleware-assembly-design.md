# 中间件与过滤器装配设计（HTTP / gRPC）

- 日期：2026-09-22
- 状态：已定稿（使用者确认：默认链含 tracing + logging + metrics（不含 recovery）；fx 注入 + `fx.Decorate` 扩展；
  本轮含 HTTP Filter 注入点与客户端 tracing 传播；按 selector 的中间件暂不做）
- 范围：`lib/consts`（tracer/meter 名）、`pkg/serverutil`（注入点类型与构造参数）、新增 `pkg/middleware`、
  四个 `services/*/internal/server` 与 `internal/app/graph.go`、`services/game/internal/data/repo/matchqueue.go`、
  `services/matcher/internal/server`（规则接口改走 `ctx.Middleware`）。不改客户端协议、不改 actor 语义。

## 1. 背景

上一轮把「基础参数」收口进了 `server.proto`，但中间件/过滤器是**函数值**，proto 表达不了。侦察后的三条结论：

1. **服务端已内置的能力别重复挂**：HTTP 的 panic 恢复（`handlePanic`）+ 超时上下文 + 请求体限额，
   gRPC 的 `safeInvoke` + 超时。故 `recovery.Recovery()` **不进默认链**（会双重恢复、双重打点）。
2. **传输层指标其实是缺口**：atlas 内置的 `transport/metrics.OnRequest` 默认是 `NoopMetrics`，
   全仓没有任何地方注入实现——所以请求 RED 指标现在完全没有。
3. **链路两端都缺**：服务端没挂 `tracing.Server`（不 extract `traceparent`），
   唯一的服务间 gRPC 客户端 `matchqueue.go` 也没挂 `tracing.Client`（不 inject）——
   表现为「每个服务各自一个 root span」，跨服务链路根本连不起来。

## 2. 决策表

| 议题 | 决策 |
|------|------|
| 装配位置 | 函数值走代码层，但默认链与注入点收口到 `pkg/middleware`，服务构造代码不出现具体中间件 |
| 默认服务端链 | `tracing.Server`（tracer 名 `atlas-transport`）→ `logging.Server`（atlas 全局 logger）→ `metrics.Server`（OTel 计数器 + 耗时直方图） |
| 默认客户端链 | `tracing.Client`（tracer 名同上），gRPC 拨号时经 `atlasgrpc.WithMiddleware` 挂载 |
| 默认过滤器链 | 空；只提供注入点（CORS/gzip/pprof 等协议层过滤器按需由业务追加） |
| 注入方式 | 具名集合类型 + fx：`serverutil.Middlewares` / `ClientMiddlewares` / `Filters`；业务用 `fx.Decorate` 追加 |
| 为何不用 fx 值组 | 组按元素类型匹配，具名类型对不上；且链序需要显式（组不保证顺序），故用「单值 + Decorate」 |
| 指标命名 | 计数器用 atlas 建议名 `server_requests_code_total`；直方图用 `server_requests_seconds`（atlas 建议名自带 `_bucket`，Prometheus 再加后缀会重复） |
| HTTP 应用点 | **handler 内显式 `ctx.Middleware(...)`**（protoc 生成代码已如此）；裸 `HandleFunc`（`/health`）不进链——探针不记日志、不打点是有意为之 |
| gRPC 应用点 | 服务端拦截器自动应用（无需 handler 配合）；同一链同时挂一元与流式，避免新增流式 RPC 静默失去可观测性 |
| 管理接口 | matcher 规则查询从裸 `HandleFunc` 改为 `Route + ctx.Middleware`，与生成代码同构，从而享有中间件链 |
| 暂不做 | 按 selector 的路由级中间件（`srv.Use("/*", ...)`）——YAGNI，需要时再加 |

## 3. 装配形态

```go
// pkg/middleware
var Module = fx.Module("middleware", fx.Provide(Server, Client, Filters))

func Server() (serverutil.Middlewares, error) { /* tracing → logging → metrics */ }
func Client() serverutil.ClientMiddlewares    { /* tracing.Client */ }
func Filters() serverutil.Filters             { /* 默认空，仅提供注入点 */ }
```

服务图里加一行 `middleware.Module`；服务端构造只多两个参数：

```go
func NewHTTPServer(cfg *conf.Bootstrap, svc *handler.GameHandler,
	mws serverutil.Middlewares, filters serverutil.Filters) (*atlashttp.Server, error) {
	srv, err := serverutil.HTTPServer(cfg.GetServer().GetHttp(), mws, filters)
	...
}
```

业务追加中间件（不动服务构造代码）：

```go
fx.Decorate(func(m serverutil.Middlewares) serverutil.Middlewares { return append(m, myAuth) })
```

客户端链路传播：`matchqueue.go` 拨号加 `atlasgrpc.WithMiddleware(mws...)`（`mws` 由 fx 注入）。

## 4. 变更清单

| 文件 | 变更 |
|------|------|
| `lib/consts` | `TracerNameTransport`、`MeterNameTransport` |
| `pkg/serverutil` | 新增 `Middlewares`/`ClientMiddlewares`/`Filters` 类型；`HTTPServer` 挂 middleware + filter，`GRPCServer` 挂一元 + 流式 |
| `pkg/middleware`（新） | 默认链、客户端链、过滤器注入点、`Module` |
| 四个 `internal/server` | 构造签名加 `mws`/`filters` 参数 |
| 四个 `internal/app/graph.go` | 加 `middleware.Module` |
| `services/game/internal/data/repo/matchqueue.go`、`internal/app/data.go` | 拨号挂客户端链 |
| `services/matcher/internal/server` | 规则查询改 `Route + ctx.Middleware`（`/health` 仍为裸处理器） |
| `AGENTS.md` | 新增「中间件与过滤器装配」小节 |

## 5. 验证

本地单测（`pkg/middleware` 4 项 + `pkg/serverutil` 过滤器 1 项）：

| 用例 | 断言 |
|------|------|
| `TestServerChainExtractsUpstreamTrace` | 带 `traceparent` 的请求 → 生成 server span，且 traceID/父 spanID 延续上游（extract 生效）|
| `TestClientChainInjectsTraceparent` | atlas gRPC 客户端拨号挂客户端链 → 服务端拦截器读到入站 `traceparent`（inject 生效）|
| `TestServerChainRecordsMetrics` | 内存 MeterReader 采集到 `server_requests_code_total` 与 `server_requests_seconds` |
| `TestServerChainLogsRequest` | 全局 logger 输出含操作名（`/ping`）的请求日志 |
| `TestHTTPServerAppliesFilters` | 过滤器给裸处理器响应加头（过滤器对裸 `HandleFunc` 也生效）|

服务器：四服务重建重启后——传输指标出现在 `/metrics`；请求日志出现；`scripts/e2e -mode dual`
（该脚本自行 `InitTracing`，进程内四服务共享全局 provider）后在 Tempo 里查同一条 trace 含
`game` 与 `matcher` 两侧 span；`test/e2e` 与 `scripts/e2e` 全量回归。

### 5.1 实施结果

- 本地：45 包 ok / 0 FAIL；`make lint` 0 重复组；atlas 侧 147 包 ok / 0 FAIL。
- 服务器（10.10.9.36）：
  - **传输指标落地**：matcher `9152` 导出 `server_requests_code_total` + `server_requests_seconds_bucket/_count/_sum`；
    用 gRPC reflection 触发真实 gRPC 调用后，game `9150` / battle `9153` 同样出现该指标。
  - **请求日志**：`logging.go:50` 输出 `kind=server component=grpc operation=/grpc.reflection.v1.ServerReflection/... code=200 latency=...`
    并带 `trace_id`/`span_id`；HTTP 管理接口同样有 `component=http operation=/v1/matcher/rules` 日志。
  - **跨服务链路**：`scripts/e2e -mode dual` 后在 Tempo 查到单条 trace（`1d12dfbf…`）呈完整父子链：
    `actor.ask` → `actor.process` → `/matcher.v1.Matcher/QueueMatch`（客户端 span）→ 同名服务端 span → `matcher.Matcher.QueueMatch`（biz span）
    —— 证明客户端注入 + 服务端 extract 两端都生效。
  - **每服务独立身份**：三个 daemon 的 gRPC span 分别以 `service.name=game|matcher|battle` 导出。
  - **回归**：`test/e2e` 12/12 PASS；`scripts/e2e -mode dual` 闭环通过；Prometheus 5/5 targets up。

**实施期发现的连带修复**：atlas 的 OTel 导出器持有**隔离的** `MeterProvider`，从未装为全局，
`otel.Meter(...)` 取到默认 provider —— 中间件的请求指标会静默丢失（不报错、无数据）。
故在 atlas 增加 `Exporter.MeterProvider()` 访问器，并由 `pkg/observability.InitMetrics` 装为全局
（关闭时还原），使 OTel 原生埋点与采集器共用同一套 instrument 与 `/metrics` 端点。

**实施期确认的应用点差异**：HTTP 中间件由 handler 内的 `ctx.Middleware(...)` 触发（protoc 生成代码已如此），
裸 `HandleFunc`（`/health`）不进链；gRPC 由服务端拦截器自动应用。matcher 规则查询因此改为
`Route + ctx.Middleware`，与生成代码同构。

## 6. 破坏性变更

- `serverutil.HTTPServer` / `GRPCServer` 增加参数（`Middlewares` / `Filters`）。
- 四个 `internal/server` 的 `NewHTTPServer` / `NewGRPCServer` 增加参数。
- `repo.NewMatchQueueGRPC` / `newMatchQueueClient` 增加 `serverutil.ClientMiddlewares` 参数。
- matcher 规则查询响应改由 atlas 响应编码器输出（JSON 形状不变：`rulesets` / `rule`）。

## 7. 遗留

- 第二轮（进程内形态改走 atlas.App 生命周期）仍未做，`pkg/serverutil.ServeAsync` 暂留。
- 按 selector 的路由级中间件、HTTP 中间件对裸处理器的覆盖策略（是否需要）留待有实际需求时再定。
