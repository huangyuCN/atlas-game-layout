# 配置约定（缺省约定唯一规则 · 五面派生 · 逐节点分类）

> 适用范围：`protobuf/configs/*.proto`（公共配置段）与 `services/*/configs/config.yaml`（各服务自包含配置）。
> 本文件是「缺省约定」的**唯一事实来源**：proto 字段注释只保留该字段的必要语义，
> 「空值/缺节怎么处理」这类通用规则一律以本文件为准，不再各写一份措辞。
> 上游设计：`docs/superpowers/specs/2026-09-23-*`（框架仓 `docs/superpowers/plans/2026-09-23-v2-p5-config-unification.md`，R9/R10）。

## 1. 唯一规则

**必需参数非空 = 该能力启用；可选参数为空 = 不覆盖底层默认；节点缺失 ≡ 其全部参数为空。**

- **节点**：YAML 里的一个配置段（如 `server.grpc`、`server.tcp`、`data.redis`）。
- **必需参数**：该能力成立的最小充分条件（监听地址、后端端点、后端地址列表等），逐节点清单见 §4。
- **空**：空串 / `0` / 未设置（proto3 零值）。零值本身有语义的布尔与枚举用 proto3 `optional`
  显式表达「未设置」（先例：`server.proto` 的 `optional Network network`、`optional bool strict_slash`）。
- **时长**：统一为字符串 + `time.ParseDuration`（如 `30s`/`1m`）；解析失败或非正值在启动期报错，不静默降级。

### 1.1 冲突裁决顺序

1. **先判必需参数（决定启用与否）**：节点缺失、或必需参数为空 ⇒ 该能力**不启用**——不再看它的任何可选参数。
   故「节点在但什么都没配」是「不启用」，而不是「启用 + 全部走底层默认」。
2. **再对可选参数判覆盖（仅在该能力已启用时）**：为空/`0`/未设置 ⇒ 不追加该选项，交底层默认；
   非空 ⇒ 显式覆盖。可选参数互不牵连：配了一个不会顺带改掉别的默认值。
3. **配置错误在装配期直接失败**：能力已启用但必需项缺项（启用 TLS 却没给证书、KCP 的 FEC/窗口只配一侧、
   两面地址相同等）一律报错，不猜、不回落、不静默降级。

## 2. R9 严格模式：`runtime.namespace`

`runtime.namespace` 是**唯一命名空间字段**，**必须显式配置**：

- 缺失、为空、或含 `[A-Za-z0-9_-]` 之外的字符 ⇒ **启动失败**（框架 `namespace` 包**不提供** `Default` 常量，
  不回落 `env`、不回落 `default`、不静默归一）。
- 进程形态（`bootstrap.AssembleLoaded`）与进程内/嵌入形态（`services/*/assemble` 的 `newBootstrap`）同一规则；
  错误信息含字段全名与示例值（`runtime.namespace: test`）。
- `runtime.env` **仅作标签**（链路资源 `deployment.environment.name` + 指标 `env` 标签），**不参与任何前缀派生**；
  `env` 为空时回填 `default` 属于「标签缺省」，与命名空间无关。

## 3. 派生链：一个字段 → 五个平面 + 一个标签

```
runtime.namespace = "test"  ──→  框架 namespace.Derive(ns)   ← 唯一派生点（本仓不再拼任何前缀字面量）
```

| 平面 | 形态（`ns=test`） | 取值字段 | 消费方 |
|------|-------------------|----------|--------|
| ① 注册中心键前缀 | `/atlas/services/test` | `Derived.RegistryPrefix` | `pkg/registry`（实例键 `<前缀>/<服务名>/<实例 ID>`） |
| ② actor NATS subject 前缀 | `atlas_actor.test` | `Derived.ActorSubjectPrefix` | `pkg/actor` → 框架 subject 单包（只拼后缀） |
| ③ 业务 topic 前缀 | `atlas.test` | `Derived.TopicPrefix` | `lib/consts.NewTopics`（`atlas.<ns>.push/event/gw.*`） |
| ④ redis 键前缀 | `atlas:test:` | `Derived.RedisKeyPrefix` | `pkg/redis.NewKeys` |
| ⑤ etcd 目录前缀 | `/atlas/actors/test` | `Derived.EtcdDirectory` | `pkg/actor`（节点归属键 + locator 前缀） |
| 边界面 subject 根 | `atlas.actor.test` | `Derived.EdgeSubjectPrefix()` | ingress/推送/死信（不在上述五面内，同由本包派生） |
| 标签（非前缀） | `env: test` | `runtime.env` | 链路 `deployment.environment.name`、指标 `env` |

**严格模式**：`Derive("")` 返回错误；派生失败即在装配/启动期失败。测试夹具显式传 `"test"` 之类的 token，
不靠缺省（例：e2e 用 `e2e-<纳秒>` / `it-<纳秒>` 独占命名空间与常驻进程隔离）。

## 4. 逐节点「必需/可选」分类表

「必需参数」一列为空表示该节点**没有启用开关**（常驻能力或纯可选参数节点）。
「为空/缺失时」一列即该节点的裁决结果；任何第 5 类语义都必须回写本表，不得在代码里新增例外。

| 节点 | 必需参数（启用判据） | 为空/缺失时 | 可选参数（为空 = 不覆盖底层默认） |
|------|----------------------|-------------|-----------------------------------|
| `runtime.name` | 非空 | 装配期报错（`bootstrap: 配置缺少 runtime.name`） | — |
| `runtime.namespace` | 非空且 `[A-Za-z0-9_-]` | **启动失败**（R9，无任何兜底） | — |
| `runtime.id` | 无 | — | 空 ⇒ 回填 `<服务名>-<主机名>`（注册实例 ID / actor NodeID / 指标实例标签三处同源） |
| `runtime.version` | 无 | — | 空 ⇒ 构建注入版本（`lib/version`，`-ldflags` 写入） |
| `runtime.env` | 无 | — | 空 ⇒ `default`（**仅标签**） |
| `runtime.min_client_version(_mode)` | 无 | — | 门槛为空 + `OFF` ⇒ 不校验（校验逻辑属 P3，网关登录入口） |
| `registry.etcd.endpoints` | 非空 | etcd 客户端构造失败（装配期报错，注册/发现均不可用） | — |
| `registry.ttl` | 无 | — | `""` ⇒ 底层默认 15s（`registry.NewEtcd` 只在 `>0` 时追加 `RegisterTTL`） |
| `server.grpc.edge_addr` | 非空 | **不启用 edge 面**（不监听、不注册 Edge 接口） | 见下「共享可选参数」 |
| `server.grpc.internal_addr` | 非空 | **不启用 internal 面** | 同上；两面地址相同 ⇒ 装配期报错（端口 `0` 除外，内核分配必然不同） |
| `server.http` | 无（**常驻面**：模板健康检查 `/health`） | 节点缺失/`addr` 空 ⇒ 仍监听（底层默认随机端口 `:0`） | `addr`/`timeout`/`path_prefix`/`strict_slash`/`max_request_body`/`tls` 为空 ⇒ 底层默认 |
| `server.tcp.addr` | 非空 | **该协议不监听**（节点缺失 ≡ `addr` 空，同一处理） | 见下「共享可选参数」 |
| `server.websocket.addr` | 非空 | 同上 | 同上（`read_buffer`/`write_buffer` 按侧独立，只配一侧合法） |
| `server.kcp.addr` | 非空 | 同上 | `fec_data_shards`/`fec_parity_shards` 与 `window_snd`/`window_rcv` 必须成对（只配一侧报错） |
| `server.udp.addr` | 非空 | 同上 | 见下「共享可选参数」 |
| `server.*.tls` | `enabled=true` 且 `cert_file`/`key_file` 非空 | 未启用（缺失或 `enabled=false`）⇒ 明文，不追加选项 | `ca_file` 空 ⇒ 不校验客户端；`client_auth=true` 时 `ca_file` 必填（缺则报错） |
| `session.ttl` | 无 | — | `""` ⇒ 默认 30s（`session.DefaultTTL`） |
| `session.sweep_interval` | 无 | — | `""`/非正 ⇒ `ttl` 的一半 |
| `data.redis.addrs` | 非空 | **不装配 redis**（构造失败：single 需恰好 1 个地址） | `mode` 空/`0` ⇒ 单点；`password`/`db` 空 ⇒ 不覆盖；`master_name` 在哨兵形态必填 |
| `data.nats.url` | 非空（需要 NATS 的能力） | 构造失败（`nats: url 不能为空`） | — |
| `data.mongo.uri` / `database` | 均非空（需要 Mongo 的服务） | 构造失败 | — |
| `log.level` / `log.format` | 无 | — | `""` ⇒ `info` / `text`；非法值启动报错 |
| `log.file` | 无 | — | `""` ⇒ 仅 stdout（非空 ⇒ stdout + 文件双写，供采集器进 Loki） |
| `observability.otlp` | 非空（端点即必需参数） | **exporter noop**（不导出，零开销） | — |
| `observability.metrics.prometheus` | 非空 | **不暴露抓取端点**（采集器退化为 noop） | — |
| `observability.sampler` | 无 | — | 未设置/`0` ⇒ `PARENT_BASED_RATIO`；`sample_ratio` 未设置 ⇒ `1.0` |

**共享可选参数**（`server.grpc` / `server.tcp` / `server.websocket` / `server.kcp` / `server.udp` 共用同一套语义）：
`network` 未设置 ⇒ 底层默认 `tcp`（非法枚举值启动报错，不静默回落）；数值项 `0` ⇒ 底层默认（负数报错）；
时长项空/非正 ⇒ 底层默认（`idle_timeout`/`write_timeout`/`stream_timeout` 类为「不限」）；
`reflection`/`metadata`/`admin` 等开关未设置 ⇒ 关闭（安全默认）。

## 5. 每服务一份自包含 YAML（R10）

- 四个服务各持**一份完整** `services/<service>/configs/config.yaml`：`runtime`/`registry`/`server`/`data`/`log`/`observability`
  全部自带，**只读自己这一份**（`pkg/config` 是唯一加载入口：yaml → JSON → protojson）。
- **不做** `configs/common.yaml`、**不做**公共默认 + 服务覆盖、**不做**深合并。
- 段级重复（`registry.etcd.endpoints`、`observability.otlp`、`data.nats.url`、`log.level`+`format`）
  **是有意保留的设计**：单服务可独立改端口/端点而不牵连他人；代价是改公共约定要动四处，这是 R10 明确接受的取舍，
  评审时不再当作「待去重项」。
- protojson **不忽略未知字段**：旧字段名/拼错的键一律解组失败（不静默忽略），这是「配了没生效」的防线之一。

## 6. 信任边界：`server.grpc` 的 edge / internal 两面

`server.grpc` 不是「一个地址」，而是**两个互相独立的面**（两个 `grpc.Server`、两个 listener、两套生命周期）：

| 字段 | 号 | 语义 |
|------|----|------|
| `edge_addr` | 2（由 `addr` 改名，号保留） | **edge 面**：不可信区。客户端 op 经网关转发到域 `rpc/` 平面的 **Edge 接口**（`ACCESS_CLIENT`） |
| `internal_addr` | 11（新增） | **internal 面**：可信区。服务间 gRPC 调用 + P7 管理面 `AdminService` |

- **空 = 不启用该面**（符合 §1 唯一规则）；进程内/测试形态显式写 `127.0.0.1:0`（随机端口），不靠留空。
- 两面地址不得相同（装配期报错）；端口写 `0` 时内核分配必然不同，故不算冲突。
- 域 `rpc/` 平面的注册规则：**Edge 接口 → edge listener，Internal 接口 → internal listener**
  （生成的 `Register...Edge`/`Register...Internal` 形参类型即本面接口，注册错面在编译期即失败）。
- 两面都挂 `opgrpc.UnaryServerInterceptor()`（入站 metadata → ctx 身份、出站错误 → gRPC status），
  由 `pkg/serverutil.GRPCServers` 统一挂载，服务侧无法漏挂。
- 端点 scheme：internal 面用 `grpc`（框架发现解析器按 scheme `grpc` 寻址 ⇒ 服务间调用落到可信面），
  edge 面用 `grpc-edge`（与 internal 区分；同一实例注册两个面时不会互相覆盖）。常量见 `pkg/serverutil`。
