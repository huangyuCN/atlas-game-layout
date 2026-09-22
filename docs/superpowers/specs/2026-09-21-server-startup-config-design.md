# 传输层启动参数与构造对齐设计（HTTP / gRPC）

- 日期：2026-09-21
- 状态：已定稿（使用者确认：构造补齐全部启动参数 + 返回具体类型 + 统一 `/health` + 进程内形态改走 atlas.App；
  配置面「全量罗列 atlas 当前能生效的选项（含 TLS）」；时长字段统一 `string` + `time.ParseDuration`；
  交付分两轮：本轮做配置面与构造，进程内生命周期统一留第二轮；TLS 需在服务器实测握手）
- 范围：`protobuf/configs/{server,registry}.proto`、`pkg/serverutil`、四个 `services/*/internal/server/server.go`、
  四个 `services/*/internal/app/graph.go`（`fx.As` 注解）、四个 `configs/config.yaml`。
  第二轮涉及 `services/*/assemble`、`services/gateway/internal/app/graph.go`、`scripts/e2e`、`test/e2e`。
  不改客户端协议、不改 actor 语义。

## 1. 背景

`services/{game,matcher,battle}/internal/server/server.go` 目前只传监听地址：

```go
srv, err := atlashttp.NewServer(atlashttp.WithAddress(cfg.GetServer().GetHttp().GetAddr()))
srv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(cfg.GetServer().GetGrpc().GetAddr()))
```

与 atlas 自带脚手架（`internal/atlascli/testdata/golden/new/default/internal/server/`）及 Kratos v2 布局对照：

| 维度 | atlas 脚手架 / Kratos v2 | game-layout 现状 |
|------|--------------------------|------------------|
| 构造参数 | `WithAddress` + `Timeout(timeout)`；gRPC 另加 `WithReflection()` | 只有 `WithAddress` |
| 超时来源 | `server.{http,grpc}.timeout`（`string` + `time.ParseDuration`） | 无配置，吃 atlas 内置默认（HTTP handler 30s、gRPC 1s） |
| 返回类型 | 具体类型 `*http.Server` / `*grpc.Server` | `transport.Server` 接口 |
| 配置面 | `{addr, timeout}` × 2 | `{addr}` × 2 |

atlas 侧**已支持但模板未接线**的启动参数：

- gRPC：`WithNetwork`、`Timeout`、`WithStreamTimeout`、`WithMaxRecvMsgSize`、`WithReflection`/`WithDisableReflection`、`WithMetadata`、`WithAdmin`、`TLSConfig`
- HTTP：`WithNetwork`、`Timeout`、`WithPathPrefix`、`WithStrictSlash`、`WithMaxRequestBody`、`TLSConfig`

已确认**不是**缺口的部分：atlas 服务端内置了 panic recover、超时上下文、请求指标与请求体限额
（HTTP `serveRequest` / gRPC `unaryServerInterceptor`），无需再挂中间件；
HTTP 的 `ReadTimeout`/`WriteTimeout`/`IdleTimeout`/`MaxHeaderBytes` atlas 硬编码且无 Option
（Kratos v2 同样不暴露），故**不列入**配置面，避免出现配了不生效的死配置。

另有一处版本号不一致：`AssembleLoaded` 里 `Module(Options{Version: version.Version})` 用构建注入版本，
而链路资源属性与指标标签用 `runtime.version` 优先——同一个进程两处版本号会不同。本次一并收敛为同一份解析结果。

## 2. 决策表

| 议题 | 决策 |
|------|------|
| 配置面范围 | 全量罗列 atlas 当前能生效的 HTTP/gRPC 选项（含 TLS）；HTTP stdlib 超时不列（atlas 无 Option） |
| 空值语义 | **空串 / 0 / 未设 = 交给 atlas 默认值，不覆盖**；每个 Option 仅在「已配置」时追加，避免「不配就变行为」与「配了等于没配」 |
| `strict_slash` | 必须 `optional bool`：atlas 默认 `true`，proto3 `bool` 默认 `false`，不加 `optional` 会在未配置时把默认值改掉 |
| `network` | 用 proto enum `Server.Network`（`NETWORK_TCP`(0)/`TCP4`/`TCP6`/`UNIX`）而非字符串：语义有限值必须枚举化（`AGENTS.md`），且与同仓 `data.proto` 的 `mode` 做法一致；proto → `net.Listen` 网络名的映射只此一份（`pkg/serverutil.networkOf` + `Network` Go 枚举），未登记取值启动报错而非静默回落 tcp |
| 超时取值 | 空 = 用底层默认；模板 `config.yaml` 显式写出：gRPC `30s`（**偏离 atlas 默认 1s，属有意的行为变更**，已在 §7 记录）、HTTP `30s`（与默认相同，显式写出便于调整） |
| 时长格式 | 统一 `string` + `time.ParseDuration`（如 `30s`、`1m`），解析失败启动报错；与 atlas 脚手架、Kratos 布局一致 |
| `registry.ttl_seconds` | 一并改为 `string ttl`（上轮刚加的 int32 秒），全仓时长格式统一 |
| 映射实现 | 抽到 `pkg/serverutil`（传输层装配辅助），四个服务共用一份，避免四份复制（`check-dup` 也会拦） |
| 服务端返回类型 | 改回具体类型 `*atlashttp.Server` / `*atlasgrpc.Server`（与脚手架、网关四传输一致）；fx 图用 `fx.As(new(transport.Server))` 保证仍进 `servers` 组 |
| 健康检查 | 抽 `serverutil.HealthHandler(service)`，四服务共用；JSON 形状不变（`{"status":"ok","service":...}`），e2e 断言不受影响 |
| 版本号来源 | 新增 `bootstrap.ModuleFor(ConfigLike)`，`AssembleLoaded` 与四个 assemble 共用；版本取 `runtime.version`，缺省回退构建注入值 |
| 第二轮（进程内生命周期） | 见 §6，本轮不实施 |

## 3. 配置面（`protobuf/configs/server.proto`）

```proto
message Server {
  message TLS {
    bool enabled = 1;       // 关闭时忽略其余字段
    string cert_file = 2;   // 服务端证书链（PEM）
    string key_file = 3;    // 服务端私钥（PEM）
    string ca_file = 4;     // 客户端 CA（PEM）；client_auth 时用于校验
    bool client_auth = 5;   // 是否要求并校验客户端证书（mTLS）
  }
  // Network 是监听网络类型（语义有限值，故用枚举而非散落字符串）。
  enum Network {
    NETWORK_TCP = 0;   // 缺省：IPv4/IPv6 均可
    NETWORK_TCP4 = 1;
    NETWORK_TCP6 = 2;
    NETWORK_UNIX = 3;
  }
  message GRPC {
    optional Network network = 1;   // 未设置 = 底层默认 tcp
    string addr = 2;                // host:port；空 = atlas 默认 :0
    string timeout = 3;             // 单次 RPC 超时；空 = atlas 默认
    string stream_timeout = 4;      // 流式 RPC 超时；空/0 = 不限
    int32  max_recv_msg_size = 5;   // 单条消息接收上限（字节）；0 = gRPC 默认
    bool   reflection = 6;          // 开启 reflection（atlas 默认关闭）
    bool   metadata = 7;            // 注册 metadata 服务
    bool   admin = 8;               // 注册 admin 服务
    TLS    tls = 9;
  }
  message HTTP {
    optional Network network = 1;   // 未设置 = 底层默认 tcp
    string addr = 2;
    string timeout = 3;             // 单次请求 handler 总执行时间；空 = atlas 默认 30s
    string path_prefix = 4;         // 路由前缀
    optional bool strict_slash = 5; // atlas 默认 true
    int64  max_request_body = 6;    // 请求体上限（字节）；0 = atlas 默认 1MB
    TLS    tls = 7;
  }
  GRPC grpc = 1;
  HTTP http = 2;
}
```

`registry.proto` 的 `optional int32 ttl_seconds = 3` 改为 `string ttl = 3`（如 `15s`，空 = atlas 默认 15s）。

## 4. 装配辅助（`pkg/serverutil`）

| 函数 | 职责 |
|------|------|
| `HTTPOptions(*configspb.Server_HTTP) ([]atlashttp.ServerOption, error)` | 时长解析；`network` 经 `networkOf` 映射；`timeout`/`path_prefix`/`max_request_body` 非空（>0）才追加；`strict_slash` 仅在 `optional` 已设时追加；TLS 经 `TLSConfig` |
| `GRPCOptions(*configspb.Server_GRPC) ([]atlasgrpc.ServerOption, error)` | 同上；`reflection`/`metadata`/`admin` 为 true 时追加对应 Option |
| `HTTPServer(*configspb.Server_HTTP, Middlewares, Filters) (*atlashttp.Server, error)` | 映射选项后构造服务端（路由/服务注册由调用方完成），并挂中间件与过滤器；错误经私有 `build` 统一包装 |
| `GRPCServer(*configspb.Server_GRPC, Middlewares) (*atlasgrpc.Server, error)` | 同上（gRPC）；同一链同时挂一元与流式 |
| `Network`（Go 枚举）+ `networkOf(*configspb.Server_Network) (string, error)` | proto 网络枚举 → `net.Listen` 网络名；未设置返回空串（用底层默认），未登记取值报错 |
| `TLSConfig(*configspb.Server_TLS) (*tls.Config, error)` | `enabled=false` 或 nil → 返回 nil（不启用）；否则读证书/私钥；`ca_file` + `client_auth=true` → `ClientCAs` + `RequireAndVerifyClientCert`；**配了 `ca_file` 但未强制时 → `VerifyClientCertIfGiven`**（给了客户端证书就校验、不给也放行——若置 `NoClientCert`，配的 CA 会被完全忽略，正是要避免的「配了等于没配」）；文件缺失或解析失败报错；`MinVersion` 固定 TLS 1.2（不提供配置项避免误配）|
| `HealthHandler(service string) http.HandlerFunc` | 统一健康检查：`Content-Type: application/json` + `{"status":"ok","service":"<name>"}` |
| ~~`ServeAsync`~~ | 第二轮已删除（两形态改由 atlas.App 启停，见 §6）|

> 设计修正（实现期由 `make lint` 的 `check-dup` 驱动）：三个服务的 `NewGRPCServer` 只差一行注册调用，
> 结构指纹一致被判重复。故把「映射选项 + 构造服务端」整体下沉为 `serverutil.HTTPServer`/`GRPCServer`，
> 服务侧只剩「构造 + 注册 handler」，重复真正消除而非加白名单。

## 5. 服务端构造（四个 `internal/server`）

```go
// NewHTTPServer 构造 HTTP 服务端（健康检查 + 业务 REST 接口）。
func NewHTTPServer(cfg *conf.Bootstrap, svc *handler.GameHandler,
	mws serverutil.Middlewares, filters serverutil.Filters) (*atlashttp.Server, error) {
	srv, err := serverutil.HTTPServer(cfg.GetServer().GetHttp(), mws, filters)
	if err != nil {
		return nil, err
	}
	srv.HandleFunc("/health", serverutil.HealthHandler(cfg.GetRuntime().GetName()))
	gamev1.RegisterPlayerHTTPServer(srv, svc)
	return srv, nil
}
```

fx 图（game/matcher/battle/gateway 四处）：

```go
fx.Annotate(server.NewHTTPServer, fx.As(new(transport.Server)), fx.ResultTags(`group:"servers"`)),
fx.Annotate(server.NewGRPCServer, fx.As(new(transport.Server)), fx.ResultTags(`group:"servers"`)),
```

`fx.As` 必需：fx 组按元素类型匹配，返回具体类型后不转换就注入不到 `[]transport.Server group:"servers"`。
网关的 `newServerSet(httpSrv transport.Server, ...)` 首参改为 `*atlashttp.Server`（Out 字段仍是接口，赋值不变）。

`bootstrap.ModuleFor(like ConfigLike) fx.Option`：服务名/实例 ID 取自 `runtime`，版本取
`runtime.version` 优先、缺省 `version.Version`；`AssembleLoaded` 改用它，四个 assemble 在第二轮改用它。

## 6. 第二轮：进程内形态改走 atlas.App（已实施）

现状：`services/*/assemble` 用 `fx.New` + `root.Start` + `serverutil.ServeAsync` + 手工 `registerInstance`
+ 手工 stop 闭包，是 atlas.App 之外的第二条启动路径；且 matcher/gateway 的进程内形态**根本不注册**。

目标：fx 图追加 `bootstrap.ModuleFor(cfg)`，由 atlas.App 统一启动/注册/注销/停止。

| 删除 | 替换 |
|------|------|
| `serverutil.ServeAsync` + 各 assemble 的 `startServers`/`stopServers` | `root.Start(ctx)`（靠 `StartSignal` 保证返回时已注册完成） |
| 各 assemble 的 `registerInstance`/`instanceOf` | atlas.App 自动注册（matcher/gateway 因此首次获得注册能力） |
| 网关 `embed_servers` 组 + `wrapWSHandler`（httptest 包装 `/ws`） | WS 服务端自起随机端口，`WSURL` 取其 `Endpoint()`（WS `Handler()` 对路径不敏感，已确认） |
| `scripts/e2e` 的手工 `registerMatcher` | matcher 自行注册 |

前置的 atlas 改动（已做）：`Signal()` 传空当前会 `signal.Notify(c)` → **监听全部信号**，
进程内形态会劫持测试进程信号；改为「空 = 不注册信号处理」并补 `TestSignalDisabled`。

实施补充：
- `pkg/bootstrap` 新增 `Boot(ctx, cfg, app, handles, require...)`：装配 fx（服务模块 + `ModuleForEmbedded`）
  并启动，返回根 App 与 require 声明的 scheme → 就绪端点；缺端点即回收并报错。各服务的进程内 handles
  内嵌 `bootstrap.Servers`（servers 值组回捞载体），句柄构造因此只剩服务特有字段。
- `pkg/serverutil`：删除 `ServeAsync`，新增 `Endpoints`（按 scheme 归集端点）；`WaitEndpoint` 保留给测试
  与自研嵌入式驱动。
- 网关删 `embed_servers` 组与 `wrapWSHandler`（httptest 包装）：WS 与进程形态一致由 WS Server 独立监听，
  `WSURL` 取其自身 `Endpoint()`（`ws://host:port/`，WS Server 默认 path 为 `/`；WS Handler 对路径不敏感）。
- `scripts/e2e` 删掉手工 `registerMatcher`：matcher 由 atlas.App 自行注册。
- 本地新增 `pkg/bootstrap` 两项用例：`TestBootStartsAndStopsGraph`（返回时端点已就绪、停机后监听关闭）、
  `TestBootRequiresEndpoints`（缺端点报错并回收已启动服务端）。

## 7. 破坏性变更

- `registry.ttl_seconds`（int32 秒）→ `registry.ttl`（string，如 `15s`）。
- `server.{grpc,http}.network` 由 `string`（`tcp`/`tcp4`/…）改为 `optional Server.Network` 枚举
  （配置值改为枚举名，如 `NETWORK_TCP4`）。
- **模板 `config.yaml` 显式把 gRPC `timeout` 设为 30s**：Atlas 默认 1s，等于放宽 30 倍——这是有意的
  行为变更（对齐 atlas 脚手架的长事务取值），HTTP 侧 30s 与默认相同（显式写出便于调整）。
- `services/*/internal/server` 构造函数返回类型由 `transport.Server` 变为 `*atlashttp.Server` / `*atlasgrpc.Server`，
  并新增 `mws` / `filters` 参数（见中间件装配规格）。
- `serverutil.HTTPServer` / `GRPCServer` 增加 `Middlewares` / `Filters` 参数。
- `repo.NewMatchQueueGRPC` / `newMatchQueueClient` 增加 `serverutil.ClientMiddlewares` 参数。
- 网关 `newServerSet` 首参类型变化（内部函数）。
- 第二轮另有：删除 `pkg/serverutil.ServeAsync` 与网关 `embed_servers` 组、`scripts/e2e` 去掉手工注册。

## 8. 验证计划

本地：全量 `go test ./...` + `make lint`；`pkg/serverutil` 单测覆盖时长解析失败、空值不覆盖、
`strict_slash` 未设不改默认、`reflection`/`metadata`/`admin` 生效（用 `srv.GetServiceInfo()` 断言注册项）、
TLS 文件缺失报错、`HealthHandler` 输出。

服务器（10.10.9.36）：

1. 四服务重建重启，配置项生效：gRPC reflection 可列出服务（`grpcurl` 或反射客户端）、HTTP handler 超时按配置生效。
2. TLS 实测：openssl 自签证书，起一对启用 TLS 的 HTTP/gRPC 服务端；`openssl s_client` 验证握手成功、
   gRPC 侧 ALPN `h2`；mTLS 场景下不带客户端证书应握手失败、带证书成功。
3. 回归：`test/e2e` 10 项 + `go run ./scripts/e2e -mode dual` 闭环。

### 8.1 第一轮实施结果

- 单测：`pkg/serverutil` 20 项全绿，含 3 项真实 TLS 握手——
  `TestHTTPTLSHandshake`（自签 CA 信任链）、`TestHTTPMutualTLS`（无客户端证书握手失败 / 带证书 200）、
  `TestGRPCTLSHandshake`（gRPC over TLS 真实调用 health 返回 SERVING）；
  另含 `TestNetworkOf`（枚举映射 + 未登记取值报错）、`TestHTTPOptionsStrictSlash`（未设置 → 301 保持默认 true；
  显式关闭 → 404）、`TestGRPCServerAppliesStreamMiddleware`（流式 RPC 也经过中间件链）、
  `TestModuleForVersionSource`（App 版本与身份同源）；
  `pkg/config.TestParseDuration`、`pkg/fxkit` TTL 用例、四服务 `config_test.go` 均通过。
- 四个 `config.yaml` 已按「全量罗列 + 注释默认值」展开（含 TLS 示例与其余可选项注释，四份措辞一致）。
- 全量 `go test ./...` 45 包 ok / 0 FAIL；`make lint` 0 重复组。
- 服务器验证见提交说明（reflection 生效、TLS 握手、mTLS、`test/e2e` 与 `scripts/e2e` 回归）。

### 8.2 第二轮实施结果（进程内形态改走 atlas.App）

- 单测：`pkg/bootstrap` 新增 `TestBootStartsAndStopsGraph`（Boot 返回时端点已就绪、Stop 后监听关闭）、
  `TestBootRequiresEndpoints`（缺端点报错并回收已启动服务端）；`-count=60` 稳定通过。
- 服务器（10.10.9.36）：`test/e2e` **12/12 PASS**（四服务全走 `bootstrap.Boot`，含冲突快速失败、WS/KCP/UDP 通道、
  命名空间隔离）；`scripts/e2e -mode dual` 闭环通过（WS URL 为 `ws://127.0.0.1:<port>/`）；
  常驻四服务停机后 etcd 键归零（注销由 atlas.App 完成）、重启 4 进程/4 键，Prometheus 5/5 targets up。
- 评审修正：`Instance.Stop` 改为等到服务端真正停止（`App.Stop` 只触发停机，`server.Stop` 跑在 Run 的 errgroup 里，
  不等会出现「Stop 返回了但端口还没关」，实测 6 次 1 次失败）；scheme 字面量收敛为 `serverutil.Scheme*` 常量；
  删除与 `transport.Endpointer` 同形的本包类型；atlas `Run()` 抽出 `startServers`/`watchSignals`（74 → 40 行，符合 ≤50 行规范）。
