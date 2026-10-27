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
| ② actor 集群 NATS subject 前缀 | `atlas_actor.test` | `Derived.ClusterSubjectPrefix` | `pkg/actor` → 框架 subject 单包（只拼后缀） |
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
| `runtime.min_client_version` | 无 | — | 空 ⇒ **仅记录**（记录客户端上报版本，不拒绝）；非空 ⇒ **登录期强制**（低于门槛/未上报/版本串非法一律拒绝，reason 常量 `CLIENT_VERSION_TOO_LOW`）。比较规则、非法串处置与取值见 §8 |
| `runtime.min_client_version_mode` | 无 | — | 只有 `NEGOTIATE` 有意义（只记录不拒绝的灰度逃生门）；缺省（含 `OFF`）与 `ENFORCE` 在网关侧同义 = 强制（见 §8） |
| `actor.lease_ttl` / `actor.lease_renew_interval` | 无（常驻能力：只有 actor 宿主 battle / game 持有归属对象） | 空 ⇒ 缺省 `60s` / `20s`（不覆盖底层默认） | 显式值必须是 `time.ParseDuration` 字符串、为正，且 `lease_renew_interval` **严格小于** `lease_ttl`，违反即**启动失败**（不静默回落）；TTL 与接管等待的取舍见 §10 |
| `actor.directory.backend` | 无 | 空 ⇒ `etcd`（保持现网行为） | `etcd` / `redis`；**托管方（battle/game）与接入层（edge）必须同值**，见 §11 |
| `actor.directory.redis.endpoints` | `backend: redis` 时非空 | 启动失败（不猜地址） | 单点 1 个；哨兵为全部哨兵地址；`master_name` 空 = 单点直连 |
| `actor.directory.redis.*` | 无 | — | `password` / `db` / `tls` / `insecure_skip_verify` / `sync_replicas` / `sync_timeout`（空 ⇒ 底层默认，`sync_timeout` 缺省 2s） |
| `actor.directory.quiesce_grace` | 无 | 空 ⇒ 框架 `5s` | Claim 保护窗；**redis 后端要求 `>= replication_lag`**，违反即启动失败 |
| `actor.directory.replication_lag` | 无（redis 后端强烈建议显式声明） | 空 ⇒ 不校验（仍有双活风险） | 切主完成窗口上界（含哨兵降级旧主的时间），见 §11.1 |
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
| `relay.timeout`（gateway） | 无 | — | `""` ⇒ 5s（`conf.DefaultRelayTimeout`）；显式值须为 `time.ParseDuration` 字符串且 >0，`≤0`/非法 ⇒ **启动失败**（懒激活冷启动预算，非超时兜底） |
| `matchmaker.admit_timeout_ms`（matcher） | 无 | — | `0`/缺失 ⇒ 5000ms（`biz.DefaultAdmitTimeout`）；显式正值 = 单局「开局+出票」预算（含 battle 侧懒激活冷启动那一跳） |
| `data.redis.addrs` | 非空 | **不装配 redis**（构造失败：single 需恰好 1 个地址） | `mode` 空/`0` ⇒ 单点；`password`/`db` 空 ⇒ 不覆盖；`master_name` 在哨兵形态必填 |
| `data.nats.url` | 非空（需要 NATS 的能力） | 构造失败（`nats: url 不能为空`） | — |
| `data.mongo.uri` / `database` | 均非空（需要 Mongo 的服务） | 构造失败 | — |
| `log.level` / `log.format` | 无 | — | `""` ⇒ `info` / `text`；非法值启动报错 |
| `log.file` | 无 | — | `""` ⇒ 仅 stdout（非空 ⇒ stdout + 文件双写，供采集器进 Loki） |
| `observability.otlp` | 非空（端点即必需参数） | **exporter noop**（不导出，零开销） | — |
| `observability.metrics.prometheus` | 非空 | **不暴露抓取端点**（采集器退化为 noop） | — |
| `observability.sampler` | 无 | — | 未设置/`0` ⇒ `PARENT_BASED_RATIO`；`sample_ratio` 未设置 ⇒ `1.0` |
| `observability.pprof` | 非空 | **不监听任何端口**（默认关闭，零开销） | 非空 ⇒ 该地址暴露 `/debug/pprof/`；地址非法 ⇒ **装配期失败**（不静默降级）。安全约定、建议端口与验证命令见 §9 |
| `server.kcp.idle_timeout` / `server.udp.idle_timeout`（battle） | 无（本服务按掉线窗口推导） | 空/`0s` ⇒ 取 `battle.offline_timeout / 3`（缺省 15s → 5s） | 显式值必须**严格小于** `battle.offline_timeout`，否则装配期启动失败（数据报面无关闭握手，掉线只能靠空闲读超时发现，见 §7） |
| `battle.ticket_key` | 非空 base64 且解码后 32 字节 | **启动失败**（无默认值） | 必须与 `edge.ticket_key` 同值，否则帧面全面拒票 |
| `battle.ticket_ttl` | 无 | — | 空 ⇒ 120s；必须为正；应大于 `offline_timeout`（窗口内重连复用同一张票） |
| `battle.offline_timeout` | 无 | — | 空 ⇒ 15s；显式 `0s` = **关闭掉线判定**（此时数据报面 idle 不推导也不校验）；负值/非法即失败 |
| `battle.max_frames` / `battle.tick_interval_ms` | 无 | — | **整项缺失** ⇒ 6000 帧 / 100ms（= 10 分钟一局；60 帧只够 6 秒，见 §7.6 的取值依据）；**显式值**必须为正且不超上限（`max_frames ≤ 20000`、`tick_interval_ms ≤ 1000`），显式 `0`/负值/超上限一律启动失败（未配置与显式 `0` 语义不同，故用 `optional`；依据见 §7.6） |
| `battle.seat_window_ms` / `battle.max_players` | 无 | — | **整项缺失** ⇒ 3000ms 入座窗口 / 10 人会话容量；`seat_window_ms` 显式 `0` = **关闭窗口**（成局即开打，旧行为），负值或 `> 30000` 失败；`max_players` 合法区间 `[1, 64]`（`0` 不是「不限」），越界失败（语义见 §7.7） |
| `battle.frame_broadcast_protobuf` | 无 | `false`（protojson / 帧头 ver=1，行为零变化） | `true` ⇒ 帧广播载荷切 protobuf 二进制 + 帧头 ver=2：同一广播实测 **1631 B → 465 B（≈3.5×）**，**帧数不变**（每连接每 tick 仍一次写）。**前置条件**：三端 SDK 都能解 ver=2 帧广播——当前 Go 直连 SDK 的解码与补帧基准写死 protojson，TS/C# 只把 `(payload, version)` 透传且补帧基准只认 ver=1，未升级的客户端会**静默丢弃**广播（语义见 §7.10） |
| `battle.edge_endpoints` | 列表非空 + `transport` 非 `UNSPECIFIED` + 地址非空 + 面不重复 | **启动失败**（客户端拿到的接入层地址只有这一个来源） | 面枚举 `EDGE_TRANSPORT_{WS,KCP,UDP}`；**按面下发**、不要求三面齐全（只开 ws 也能启动） |
| `battle.frame_advertise_host` | 无（整项缺失 = 回落） | 整项缺失 ⇒ 回落 `edge_endpoints` 的主机（**仅适合同机部署**） | 只填主机（不含端口、IPv6 不带方括号）；空白串/带端口/带方括号即启动失败；跨机部署**必须**显式配置（语义见 §7） |
| `edge.ticket_key` | 非空 base64 且解码后 32 字节 | **启动失败**（无默认值） | 必须与 `battle.ticket_key` 同值 |
| `edge.frame_service` | 无 | — | 空 ⇒ `battle-frame`（battle 注册的帧面实例服务名） |
| `edge.actor_namespace` | 无 | — | 空 ⇒ 取 `runtime.namespace`（**不是**注册键前缀路径）；必须与 battle 同值，否则查不到属主 |
| `edge.listeners` | 至少一面，且 `name`/`network`/`addr`/`carrier` 齐备 | **启动失败**（`edge.listeners 不能为空`） | `name` 必须与 battle 帧面实例元数据里的端口键（`ws`/`kcp`/`udp`）一致；KCP/UDP 面只能配 `CARRIER_DATAGRAM`（框架不允许数据报面配 `stream_hello`），WS 面配 `CARRIER_WS_UPGRADE` |
| `edge.max_streams` / `edge.idle_timeout` / `edge.max_per_ip` | 无 | — | 空/非正 ⇒ 框架缺省（并发流 `1024` / 空闲回收 `60s` / 不限每 IP 连接数） |
| `edge.new_conn_rate` / `edge.new_conn_burst` | 无 | — | 空/非正 ⇒ 不限制速率 / 桶深取 `max(new_conn_rate, 1024)`；**两者语义不同**（每秒补充 vs 瞬时桶深，见 §7.8） |
| `edge.dial_timeout` / `edge.probe_after` / `edge.probe_timeout` | 无 | — | 空 ⇒ 框架缺省（拨号与握手读取 `5s`；探活触发取 `idle_timeout`、等待回应取触发时长的一半、下限 `50ms`） |

> 两个 **5s** 都是**懒激活冷启动预算**：gateway 的是推送/控制回调首次触达 redis 路由表/会话表的预算，
> matcher 的是单局「开局+出票」首次触达 battle 并触发对局 actor 懒激活的预算。它们不是慢请求的超时兜底，
> 也不要当成压测里「服务端处理慢」的背锅项——超时只说明冷启动那一跳没在预算内完成。

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

> **客户端侧（拨号方）同一条边界**：服务发现解析默认只取 `grpc`（internal 面）端点；
> 要拨 domain 的 edge 面必须显式 `atlasgrpc.WithEndpointScheme("grpc-edge")`——
> 不配就拨不到 Edge 面（网关的客户端 op 透传正是靠它落到不可信面）。

- **空 = 不启用该面**（符合 §1 唯一规则）；进程内/测试形态显式写 `127.0.0.1:0`（随机端口），不靠留空。
- 两面地址不得相同（装配期报错）；端口写 `0` 时内核分配必然不同，故不算冲突。
- 域 `rpc/` 平面的注册规则：**Edge 接口 → edge listener，Internal 接口 → internal listener**
  （生成的 `Register...Edge`/`Register...Internal` 形参类型即本面接口，注册错面在编译期即失败）。
- 两面都挂 `opgrpc.UnaryServerInterceptor()`（入站 metadata → ctx 身份、出站错误 → gRPC status），
  由 `pkg/serverutil.GRPCServers` 统一挂载，服务侧无法漏挂。
- 端点 scheme：internal 面用 `grpc`（框架发现解析器按 scheme `grpc` 寻址 ⇒ 服务间调用落到可信面），
  edge 面用 `grpc-edge`（与 internal 区分；同一实例注册两个面时不会互相覆盖）。常量见 `pkg/serverutil`。
- **阶段 3（2026-09-29）起 battle 不启用 edge 面**：客户端战斗 op 不再经网关转 Edge gRPC 面，改为凭
  `battle_ticket` 直连接入层 → battle 的 KCP/UDP/WS 直连帧面（§7）；battle 的 `server.grpc.edge_addr`
  留空即「该面不启用」，只剩 internal 面（matcher 开局/取票、game 结算联动）。game 的 edge 面照旧启用。

## 7. 战斗直连：地址、票据与掉线推导（battle / edge 专属语义）

### 7.1 接入层地址按「传输面 → 地址」下发

- 接入层分面监听（模板：ws=tcp:7100、kcp=udp:7101、udp=udp:7102），**一个地址不够**——单地址无法让
  SDK 知道该拨哪个端口。故契约下发 `repeated EdgeEndpoint{transport, address}`（`battle.v1`），
  SDK 按自身支持的传输面取一项（TS 只有 ws）；**缺该面即明确报错**，不猜端口、不回落换面。
- 地址的**唯一来源**是 `battle.edge_endpoints`：`IssueEntryTicket` 出票时一并回执，matcher 只搬运、
  不重组，网关逐人扇出。装配期校验「至少一面、面不重复、地址非空」，违反即启动失败；
  **不要求三面齐全**——只开 ws 的部署也能启动，缺面只影响用该面的客户端。

### 7.2 `edge_endpoints` vs `frame_advertise_host`（两个方向，别混）

| 项 | 给谁用 | 填什么 | 缺失时 |
|---|---|---|---|
| `battle.edge_endpoints` | **客户端**（随成局通知下发） | 接入层各面的对外地址（host:port） | **启动失败**（客户端无面可连） |
| `battle.frame_advertise_host` | **接入层**（battle 注册 `battle-frame` 帧面实例时报的主机） | 只填本节点对外可达的**主机**（不含端口） | 回落 `edge_endpoints` 的主机——**仅适合同机部署**；跨机部署不配会让接入层拿自己的地址去拨后端（现象是连不上后端且极难定位） |

端口既不写死也不进票据：接入层从 `battle-frame` 实例元数据里读各面**已绑定**的真实端口
（`server.kcp/udp/websocket.addr` 的端口填 0 时，注册的是内核分配后的真实端口）。

### 7.3 `battle.offline_timeout` 与数据报面 `idle_timeout` 的推导

- `battle.offline_timeout` 是掉线判定窗口（空 ⇒ 15s；显式 `0s` = **关闭**掉线判定，此时既不推导也不校验）。
- **KCP/UDP 没有关闭握手**：掉线只能靠帧面空闲读超时发现，故 `server.kcp.idle_timeout` /
  `server.udp.idle_timeout` 未配置（或配 `0s`）时取 `offline_timeout / 3`（15s → 5s）；显式配置必须
  **严格小于** `offline_timeout`，否则装配期启动失败。WS/TCP 面有可靠 EOF，**继承底层缺省、不做推导**。
- 违反这条推导的后果不是「慢一点」：掉线事件会推迟到缺省的 120s 读超时，掉线策略形同虚设。
- **客户端保活约定**：帧面活跃由**收包**刷新（流式面重设空闲读超时、数据报面刷新 peer 活跃时间，
  与 op 是什么无关），故客户端须以 **≤ `offline_timeout / 3`** 的周期（缺省 15s → **5s**）发送帧或
  `Ping`（`battle.v1.BattleService/Ping`：Tell 无回执、不参与对局逻辑、不改对局状态；无输入期间用它）；
  超过该周期不只会在数据报面被判掉线判负，长时间静默还会让 NAT 映射失效——此后**下行**帧收不到，
  现象是「连接还在但没数据」。`SendFrameInput` 不能兼职保活（会往 lockstep 输入流里塞垃圾）。

### 7.4 票据密钥两处同值

`edge.ticket_key` 与 `battle.ticket_key` 是同一把 32 字节 AEAD（AES-256-GCM）密钥，**缺失/长度不符即
启动失败**，生产经配置中心注入。两处不一致表现为「接入层能验票、battle 一律拒票」（或反之），
排查成本高，故 e2e 与发布检查必须校验两值相同；票据内含 `kid` 预留轮换，轮换编排归 M11。

### 7.5 容量与告警阈值（阶段 3 压测实测口径）

**怎么复现**（服务器 `10.10.9.36`；进程内起五服务 + 客户端同机，故数字是**单机口径**）：

```bash
cd ~/atlas-game-layout
go build -o /tmp/loadtest ./scripts/loadtest
/tmp/loadtest -players 256 -transport ws  -via edge -duration 20s -pps 10   # 连接数阶梯
/tmp/loadtest -players 64  -transport ws  -via edge -duration 20s -pps 400  # 包速率阶梯
/tmp/loadtest -players 128 -transport ws  -via direct -duration 20s -pps 10 # 直连基线对照
```

实测结论（2026-09-30，12 核 x86_64，客户端与服务端同机；完整表见变更手册 §6.5）：

| 维度 | 实测 | 说明 |
|---|---|---|
| 直连帧连接数 | **512 条稳定**（0 失败，帧面 RTT p99 23.8ms，5125 pkt/s 下行） | 未触顶；再往上受限的是注册/匹配链路耗时，而非接入层 |
| 上行包速率 | **≥25.6k pkt/s**（64 连接 × 400 pps，0 失败，RTT p99 5.6ms） | 未触顶 |
| 数据报面（UDP） | 128 连接 / 1.28k pkt/s 干净；**256 连接出现退化** | 数据报面无重传：单包丢失即让该连接挂起到判掉线（`offline_timeout` 15s）→ 结算拆流；生产需客户端补帧/重传兜底 |
| 新流准入 | **15–22ms/流**（hello → 验票 → 查目录 + 选帧面实例 → 回 flow-id） | 数据报面准入在单读取循环内串行 ⇒ 约 45–65 新流/秒/实例；WS 面握手与首帧分离，未见同等排队 |
| 转发开销（微基准） | 经接入层 ≈ 直连后端的 **1.9–2.1 倍** 单包往返（同机回环） | 增量就是那一跳；稳态吞吐不受影响（见上两行） |

**告警阈值建议**（阈值按本机口径给保守值；换机器先按同口径重测再定）：

| 指标 | 建议阈值 | 含义 |
|---|---|---|
| `edge_streams_active` | ≥ `edge.max_streams` 的 80% 持续 1 分钟 | 接近并发流上限，需扩容或调高上限 |
| `edge_ticket_rejected_total{reason}` | 5 分钟增量 > 新流总数的 1% | 票/密钥/时钟漂移（`ticket_invalid`/`ticket_expired`）或后端解析失败（`backend_unavailable`） |
| `edge_streams_teared_down_total{reason=owner_changed}` | 突增（同比 > 5 倍） | 属主迁移/目录异常；同时看 battle 侧是否在重连 |
| `edge_streams_teared_down_total{reason=takeover}` | 突增（同比 > 5 倍） | 数据报面重新握手即接管（同身份新对端顶替旧 flow-id）；正常重连会出现，突增说明客户端在反复重连 |
| `edge_probe_failures_total` | 5 分钟增量 > 活跃流数的 1% | 后端不可达或链路异常 |
| 单实例新流准入速率 | > 50 流/秒 持续 1 分钟 | 准入路径串行，需水平扩接入层实例 |
| `battle_offline_timeouts_total` | > 对局数的 5%/分钟 | 客户端保活缺失（三 SDK 的周期心跳**已补**：缺省 2s ≤ `offline_timeout/3`，Go `084fcdf` / TS `937c8b2` / C# `f94e6fa`） |
| `battle_reconnects_total` | 与 `battle_offline_timeouts_total` 同看 | 回座次数；**回座率骤降**说明窗口内重连不成功（票过期/接入层不可达） |
| `battle_online_players` | 突降（同比 < 50%） | 战斗域直连在线人数；突降伴随 `edge_streams_active` 同步下跌即接入层或帧面故障 |
| `TRANSPORT_DOWNLINK_FAILED`（错误 reason） | 出现即告警（5 分钟增量 > 0） | 一次性回执超过单包上限，下行无法投递（见 `atlas/errors/class.go`；按 reason 计数需服务端在错误路径打点） |

**落地位置**（阈值即上面这张表，规则与面板同源，改一处要同步改另一处）：

- Prometheus 告警规则：`deploy/observability/rules/atlas-alerts.yml`（`promtool check rules` 可校验）；
- Grafana 面板：`deploy/observability/grafana/provisioning/dashboards/json/atlas-stage3.json`。

> **口径前提（装配期预注册）**：`edge_*` 序列在**装配期**预注册（框架先建序列再记 0），
> 故接入层一启动，`/metrics` 上就有全部 `edge_*` 家族（值为 0）。这条在 2026-10-27 修正过：
> 旧行为是「OTel 只在第一次观测后建序列」，空闲实例上一条 `edge_*` 都没有（只有 `go_*` /
> `process_*`），于是「抓不到」被多轮推断读成「计数为 0 = 没发生」。
> 现在「抓不到」只说明采集器没注入；验收接线仍建议**触发一次事件**确认值会动
> （打一次坏票 / 建一条流 / 拨一次失败后端）——「为 0」与「没接线」是两回事。

### 7.6 局时长可配（`battle.max_frames` / `battle.tick_interval_ms`）

单局寿命 = `max_frames` × `tick_interval_ms`。两者**整项不配置即取缺省**：6000 帧 × 100ms =
**10 分钟一局**。这两个值原来是 `services/battle/internal/actor` 里的常量，导致「单局内跑一段 8s
静默窗口」在物理上不可达（局先结束了）。

**缺省为什么是 6000 而不是 60**：真实对局时长 5–15 分钟 ⇒ 3000–9000 帧，6000 取区间中位。
旧缺省 60 只够 6 秒（60 × 100ms），对局刚开打就结算——压测/联调时端上看到的全是「对局已结束」，
会被误判为**对局异常结束**（几轮压测数据因此全部作废）。**这个数字有口径，不是随手挑的**：
调整前先确认对局时长口径，并同步 `services/battle/internal/conf/conf.proto`、
`services/battle/configs/config.yaml` 与本节的注释与表格。

| 配置 | 形态 | 缺省 | 校验（违反即**启动失败**，与 `offline_timeout` 同风格） |
|---|---|---|---|
| `battle.max_frames` | `optional int64`（帧数） | 6000（10fps 下 10 分钟） | `>0` 且 `≤ 20000`；显式 `0`/负值/超上限报错 |
| `battle.tick_interval_ms` | `optional int64`（**毫秒整数**） | 100 | `>0` 且 `≤ 1000`；显式 `0`/负值/超上限报错 |

- **为什么是毫秒整数而不是 `google.protobuf.Duration`**：本文件既有的两个时长字段
  （`ticket_ttl` / `offline_timeout`）都是「字符串 + `time.ParseDuration`」，全仓无任何
  `Duration` 用法；毫秒整数与 `protobuf/configs/server.proto` 的数值风格一致，且免去
  `Duration` 秒/纳秒双字段的误配。
- **为什么用 `optional`**：proto3 裸标量区分不出「未配置」与「显式 0」。未配置要取缺省（6000 帧），
  而显式 `0` 是笔误——两者语义必须分开（同 `battle.frame_advertise_host` 的写法）。
- **上限依据**：
  - `max_frames ≤ 20000`：按 2 人局每帧 ≈238B 估算（每帧每玩家一条 `lockstep.Input` ≈112B 含
    分配开销；快照每 `SnapshotEvery`（缺省 10）帧一份 ≈136B），20000 帧 ≈ **4.8MiB/局**
    （10fps 下 ≈33 分钟），留有一倍以上余量。`MemoryStorage` 的快照是**只追加不裁剪**的，
    故上限同时约束了快照增长。
  - `tick_interval_ms ≤ 1000`：① 帧同步语义——帧间隔 >1s 时客户端插值/预测窗口失效；
    ② 与数据报面空闲读超时耦合——`server.kcp/udp.idle_timeout` 缺省取
    `offline_timeout/3`（缺省 15s → 5s），帧间隔 1s 时尚有 5 帧余量，再大则丢一两个包就可能
    被空闲超时误判掉线。
- **调大之后的连带约束**：局变长不等于可以不保活。数据报面（KCP/UDP）仍按空闲读超时判活，
  客户端必须以 **≤ `offline_timeout/3`** 的周期发送帧或 `Ping`（见 §7.3）。
- **长局验收**（8s 静默窗口）：缺省 6000 帧（10 分钟）本身就足以跑完 8s 静默窗口，**无需再调**
  `max_frames`；若要一个「整局只剩几十秒」的加速回归口径，可显式配 `300`（30s 一局，**只是示例、
  不是缺省**），用 Go SDK 的 `examples/directloop -idle-hold 8s` 断言
  「静默期不被判出局 + 仍收帧 + 静默后仍能补帧」。

### 7.7 坐满再开打（`battle.seat_window_ms` / `battle.max_players`）

成局到开打之间原来没有缝：`Admit`（开局 + 出票）一返回，lockstep 会话时钟就开始走，而客户端
从收到成局通知到建连、握手、发出首帧要 1–2s——这段时间被算进单局寿命（当时的缺省 60 帧 ×
100ms ≈ 6s；现缺省 6000 帧 = 10 分钟，但「成局即开打」仍会把这 1–2s 白算进局内），
于是晚到玩家拿到的是 `BATTLE_ENDED`（压测一轮实测 9,986 次）。

现在成局后先打开**入座窗口**：会话挂起（帧号停在 0，不产生任何对局时间），等参战者的帧连接建立。

| 配置 | 形态 | 缺省 | 校验（违反即**启动失败**） |
|---|---|---|---|
| `battle.seat_window_ms` | `optional int64`（**毫秒整数**） | 3000 | 显式 `0` = **关闭窗口**（成局即开打，旧行为）；负值或 `> 30000` 报错 |
| `battle.max_players` | `optional int32`（人数） | 10 | `[1, 64]`；`0`/负值/超上限报错（`0` 不是「不限」） |

语义（三条必须说清）：

1. **坐满立即开打**：最后一名参战者的帧连接建立即结束窗口（`outcome="all_seated"`）。
2. **到期即开打**：窗口到期仍未坐满时不再等待（`outcome="timeout"`）；未入座者**不剔除、不重配**——
   他在对局结束前仍可加入并参与，只是对局不再等他（迟到补投/拒绝仍是既有的 `BATTLE_ENDED`
   语义，帧协议零改动）。若需要「踢掉未入座者」的强语义，属另一轮（要给客户端新的失败回执）。
3. **窗口内掉线释放座位**：该玩家重新回到「等入座」，窗口截止时刻不变。

测量口径：`battle_seat_window_total{outcome}`。`timeout` 占比高说明客户端入座慢于窗口
（要么调大 `seat_window_ms`，要么查建连/取票耗时）；恒为 `all_seated` 说明窗口已够。
**`max_players` 必须 ≥ 实际成局人数**（casual 规则集单侧上限 5 ⇒ 一局 10 人）：小于成局人数时
多出来的人会因「会话已满」进不去——旧缺省 `2` 就等于把 10 人局做成 2 人局。

### 7.8 新连接准入：`edge.new_conn_rate` 与 `edge.new_conn_burst` 的分工

两者是同一只令牌桶的两个旋钮，**不是**「同一个数的两种写法」：

| 配置 | 语义 | 作用域 |
|---|---|---|
| `edge.new_conn_rate` | **每秒补充的令牌数**（稳态上限：平均每秒最多新建多少条流） | 持续一整段时间 |
| `edge.new_conn_burst` | **瞬时桶深**（一次性突发最多放行多少条） | 某一瞬间 |

只配 `rate` 时桶深缺省取 `max(rate, 1024)`。开服、大规模重连、对局集中开局都属于「一瞬间涌入
几千条」的场景：桶深一旦小于并发建连数，超出的连接就会被**静默拒绝**（L4 只有断开，客户端看到的是
`connection reset by peer` 或 `unexpected EOF`，服务端拒绝计数在 `edge_ticket_rejected_total{reason="rate_limited"}`）。

算账依据（2026-10-26 诊断 §3.2，4096 人一轮压测）：`new_conn_rate: 200`、桶深缺省 1024 时，
放行条数为 `1024 + 200 × 12s ≈ 3430`，与实际建连成功数 `players_matched=3430` 完全一致，
剩余的 666 条即被限流拒绝——**数字对得上，就不再需要怀疑 TCP/WS 层损坏**。

取值建议：`burst ≥ 一次开服/大规模重连的并发建连数`（本模板 `max_streams: 4096` ⇒ 取 `5000` 留余量），
`rate ≥ 峰值人/秒`。两者同值是合理起点：速率上限不变，只是允许一次性突发到桶深。

### 7.9 数据报面收缓冲：生产必须把 `net.core.rmem_max` 抬到 ≥ 8 MiB

接入层在 `ListenPacket` 之后对每个数据报面调用 `SetReadBuffer(8 MiB)`，但**能不能拿到由内核决定**：
内核会把请求值截断到 `net.core.rmem_max`。默认值通常只有 208 KiB 量级（实测 `212992`），
单读循环在 10–20k pps 下缓冲只够约 0.2 秒，一旦被调度延迟就**静默丢包**——表现为客户端重传/补帧、
最终判掉线，而服务端日志里什么都看不到，只有内核计数器在涨：

```bash
sysctl net.core.rmem_max                  # 现状（生产要求 ≥ 8388608）
ss -u -a -n | grep 7102                   # Recv-Q 持续非 0 也说明读不动
cat /proc/net/snmp | grep -A1 '^Udp:'     # UdpRcvbufErrors 即内核丢包计数（只增不减）
```

- **生产必须**：`net.core.rmem_max ≥ 8388608`（8 MiB），否则本项等于没设；
- 申请失败（无权限、容器只读 sysctl）**只告警不阻断启动**，日志里会有
  「接入面收缓冲申请失败（内核按 net.core.rmem_max 限流…）」，此时必须按上面的办法抬上限；
- 长期修法是「每面多个 socket + `SO_REUSEPORT` + 多读协程」，把单读循环的天花板解开（尚未实现）。

### 7.10 帧广播载荷编码：`battle.frame_broadcast_protobuf`

帧广播的**内容**（`battle.v1.FrameBroadcast`）一帧只编码一次、复用于该帧全部目标；可切换的只有
**编码方式**（规范 §3.1 载荷编码协商：帧头 version 位自描述编码）：

| 编码 | 帧头 ver | 同一广播体积 | 每帧编码开销（本机 10 目标基准） |
|---|---|---|---|
| protojson（缺省） | 1 | 1631 B | ≈12 µs/op、10520 B/op、163 allocs/op |
| protobuf 二进制 | 2 | **465 B（≈1/3.5）** | ≈1.7 µs/op、784 B/op、12 allocs/op |

- **只减包体、不减包数**：每连接每 tick 仍是一次写（写是必需的 syscall）。ws 面 41k pps ×
  1546 B ≈ 603 Mbps 的链路占用，按包体降到 ≈1/3.5 后约 ≈172 Mbps，重传与 p99 随之缓解。
- **不改任何消息字段**：收发两侧的 DTO 不变，变化只在帧头 version 位与载荷字节。
- **开关是整节点粒度**（装配期一次决定，`stream.NewRegistryWithEncoding` 校验「编码 ↔ 版本」自洽），
  缺省关闭即行为零变化。
- **开启前提**：三端 SDK 都能解 ver=2 帧广播。现状：Go 直连 SDK 的 `OnFrame`/补帧基准写死
  protojson；TS/C# 把 `(payload, version)` 原样交给上层但补帧基准只认 ver=1 ⇒ 未升级的客户端
  会静默丢弃帧广播。客户端就绪后再打开。

## 8. 客户端版本门槛（gateway 登录入口专属）

阶段 3 起战斗帧由客户端凭 `battle_ticket` **直连**接入层（需新 SDK），老客户端会「匹配成功但连不上」，
故门槛必须在**匹配入口之前**生效：网关 `Login`/`Resume` 在**建立会话之前**校验，被拒请求不建立会话、
不触达域、不进入匹配（后续业务 op 因无会话身份被拒 `INVALID_TOKEN`）。

### 8.1 语义（`runtime.min_client_version`）

| 配置 | 行为 |
|---|---|
| 空 | **仅记录**：把客户端上报版本与门槛值写进日志，一律放行（未设门槛的部署行为不变） |
| 非空 | **登录期强制**：低于门槛 / 未上报 / 版本串非法一律拒绝（reason 常量 `CLIENT_VERSION_TOO_LOW`） |
| 非空 + `min_client_version_mode: NEGOTIATE` | 仅记录不拒绝（灰度期逃生门） |

- **「设置门槛即强制」是刻意的**：门槛与 mode 分开配时，漏配 mode 会静默退化成只记录，正是
  「匹配成功但连不上」的成因。`OFF` 与 `ENFORCE` 在网关侧同义（proto3 标量区分不出「未配置」与
  「显式 OFF」，缺省值按强制处理）；要只记录请显式写 `NEGOTIATE`。
- 拒绝回执的 message 统一为「客户端版本<成因>：当前 `<上报值>`，最低要求 `<门槛>`，请升级客户端」，
  成因区分「未上报 / 无法识别（需形如 1.2.3） / 过低」；客户端按 reason 分支即可，不必比对字面量。
- **取值**：首个支持「战斗帧直连」的 SDK 版本。模板现值 **`0.7.0`（2026-10-09 上调）**——三 SDK
  统一发行版号 `0.7.0` 已发布并核验（Go 打 `v0.7.0` tag、TS `@huangyucn/atlas-sdk-ts@0.7.0`、
  C# `HuangyuCN.Atlas.Sdk 0.7.0`），故按发布方案把门槛抬到该版本；比它老的客户端不支持直连，
  现在会**在登录期被明确拒绝**（reason `CLIENT_VERSION_TOO_LOW`，message 带最低版本）而不是
  「匹配成功但连不上」。**回退只需把本值改回 `0.1.0`**。
  上调动作＝把模板 `services/gateway/configs/config.yaml` 的 `runtime.min_client_version`
  由 `0.1.0` 改为 `0.7.0` 并重新部署网关（**已于 2026-10-09 执行**）（`game`/`matcher`/`battle` 三份同名样例只是字段位、
  不参与判定，可一并对齐但非必须）。
- **顺序硬约束：先发 SDK 再上调**，否则在用客户端会被一并拒绝。完整发布步骤（服务端先上线 →
  三 SDK 发布 → 门槛上调 → 公告 → 观察窗，含逐步验证与回滚）见框架 `atlas` 仓
  `docs/superpowers/plans/2026-09-29-stage3-direct-battle-connect-plan.md` 的「发布方案」。

### 8.2 版本比较与非法串处置

- 逐段**数字**比较（`0.10.0 > 0.9.0`，不是字符串比较）；段数不足按 0 补齐（`1.2` == `1.2.0`）；
  构建元数据忽略（`1.2.0+build.5` == `1.2.0`）；两侧空白容忍；预发布低于同号正式版
  （`1.2.0-rc.1 < 1.2.0`；预发布之间按字符串比较）。
- **版本串非法按「低于门槛」处理**（复用同一 reason `CLIENT_VERSION_TOO_LOW`）：新增独立 reason 要改
  `api/error/v1` 协议，且客户端与客服的分支只需「升级客户端」一条；用 message 的成因措辞区分即可。
  未上报（空串）同样拒绝——旧客户端恰恰多数不报版本，放行等于把「匹配成功但连不上」放回来。

## 9. 运行时自省面：pprof 与 go/process 运行时指标（OBS-4）

### 9.1 运行时指标（无需配置，抓取端点启用即随行导出）

来源是 `client_golang` 官方 collector（`collectors.NewGoCollector` / `collectors.NewProcessCollector`，
**不手写一套**），与 OTel 采集指标**同一份注册表**：框架 `contrib/metrics/otel` 的 Exporter 持
**私有**注册表、未开放「追加 collector」的接缝，故模板经框架明确支持的 `otelmetrics.WithReader`
接缝交回自建注册表（口径逐项对齐：不加计数后缀、不带 scope/unit/target 信息、服务身份作常量标签）。

| 组 | 代表指标 | 用途 |
|---|---|---|
| `go_*` | `go_goroutines`、`go_memstats_heap_inuse_bytes`、`go_gc_duration_seconds` | goroutine 泄漏、堆增长、GC 压力 |
| `process_*` | `process_resident_memory_bytes`、`process_open_fds`、`process_cpu_seconds_total` | 常驻内存与句柄泄漏（长跑判据） |

验证（抓取端口见 §4 的 `observability.metrics.prometheus`，五服务分别为 9151/9150/9152/9153/9154）：

```bash
curl -s http://127.0.0.1:9153/metrics | grep -c '^go_'                                 # > 0
curl -s http://127.0.0.1:9153/metrics | grep -E '^(go_goroutines|process_resident_memory_bytes) '
```

### 9.2 pprof（`observability.pprof`，**默认关闭**）

值为监听地址 `host:port`：**空/不配 = 不监听任何端口**；配了但地址非法 ⇒ **装配期失败**
（不静默降级成「不监听」——「配了没生效」比启动失败更贵）。处理器复用框架
`transport/http/pprof`（单源：模板不再自行注册一套 `net/http/pprof`）。

| 服务 | 建议端口（同机起全栈时避免撞端口） |
|---|---|
| gateway / game / matcher / battle / edge | 6060 / 6061 / 6062 / 6063 / 6064 |

安全策略（三条，缺一不可）：

1. **只绑回环或内网**：`/debug/pprof/` 会暴露进程内存、goroutine 与调用栈——**公网暴露等于把
   内存与调用栈交出去**；生产用 `127.0.0.1:606x`（同机排障）或内网管理网卡地址。绑到非回环地址时
   启动日志给一条 `WARN`（**仅告警不拒绝**：绑内网按需采样是允许的部署形态）。
2. **按需开启**：压测剖析/内存排查时临时打开，采样完立即关闭；不长期常开。
3. **不走公网入口**：接入层、K8s Service、LB 一律不转发 `/debug/pprof/*`。

验证：

```bash
curl -s http://127.0.0.1:6063/debug/pprof/ | head                       # 索引页（goroutine/heap/profile）
curl -s -o /tmp/cpu.pb.gz "http://127.0.0.1:6063/debug/pprof/profile?seconds=30"
go tool pprof -top /tmp/cpu.pb.gz
curl -s -o /tmp/heap.pb.gz http://127.0.0.1:6063/debug/pprof/heap
```

> 面板与长跑采样的落地位置同 §7.5（`deploy/observability/**`）：内存 / goroutine / fd 三张图的
> PromQL 取值来源即 9.1 的指标；常开采样（Prometheus 抓取）只看 9.1，pprof 只在 9.2 的按需窗口开。

## 10. actor 集群租约（`actor.lease_ttl` / `actor.lease_renew_interval`）

actor 目录的归属是 etcd 里**带 TTL 的键**（前缀 `/atlas/actors/<ns>`）：属主每
`lease_renew_interval` 对本节点全部 owned 做一次「LookupCAS + CAS」，把键 TTL 与记录的
`lease_deadline` 一起推到「当下 + `lease_ttl`」。两项都只对 **actor 宿主服务**
（battle / game）有意义；gateway / matcher 是 actor 平面的调用方（不持有归属），
配了也没有对象可续租。

| 配置 | 缺省 | 语义 |
|---|---|---|
| `actor.lease_ttl` | `60s` | 归属键 TTL 与 `lease_deadline` 的写入值（`time.ParseDuration` 字符串） |
| `actor.lease_renew_interval` | `20s` | 归属续租周期（建议取 `lease_ttl/3`） |

**为空/缺失** ⇒ 取上表缺省（不追加配置、交底层默认）；**显式值**必须能被 `time.ParseDuration`
解析、为正，且 `lease_renew_interval` **严格小于** `lease_ttl`——违反即**启动失败**（不静默回落、
不静默归一）。校验点在 `pkg/actor.NewRuntime`（`Options.leaseConfig`），且发生在建 NATS/etcd
连接**之前**：配置写错在装配期就炸，不拖到运行期。

### 10.1 为什么缺省不是框架的 10s/3s

框架 `cluster.DefaultLease()` 是**开发期取值**（10s/3s）。生产实测（1024 流压测，单节点 etcd）：

- etcd 后端抖动：commit 延迟 0.88ms → **24.12ms**、WAL fsync 0.66ms → **14.12ms**，抖动窗口十几秒；
- `TTL=10s` 时键会在续租落地**之前**过期 ⇒ 该轮续租读到 `ErrNotFound` ⇒ 判 Lost：
  同一秒约 100 个对局被冻结（`renewed=27 lost=72`），上层表现为 matcher 取票超时、
  客户端「匹配成功但成局失败」；
- 同一抖动下 `TTL=60s` 完全无影响（抖动 ≪ TTL）。框架自带压测节点用的就是 `60s/20s`。

### 10.2 代价：接管等待 = `lease_ttl` + `QuiesceGrace`

TTL 变长不是免费的：他节点必须等「租约过期 + QuiesceGrace（缺省 5s）」才能抢占失效属主。

| 配置 | 抖动容忍 | 崩溃 / 被 kill 后他节点接管等待 | etcd 写量（相对） |
|---|---|---|---|
| 框架缺省 `10s` / `3s` | 约 10s | 约 **15s** | 1× |
| 模板缺省 `60s` / `20s` | 约 60s | 约 **65s** | 约 **1/6.7** |

写量口径：每个 owned PID 每周期 2 次 etcd 往返（`LookupCAS` + `CAS`），commit 速率
≈ `2 / lease_renew_interval`。调法：强单活业务若不能接受 65s 接管窗口，把 `lease_ttl`
下调（如 `30s`，`lease_renew_interval: 10s`）即可——只要 TTL 仍显著大于实测抖动幅度。

### 10.3 判 Lost 之后（自愈）

`60s/20s` 只降低失租概率，不改变「已经 Lost」的处理：判 Lost 立即打栅栏 + 冻结本地 cell
（杜绝双活窗口），随后框架按指数退避**自动重 Claim**（1s 起、30s 封顶、最多 8 次，走既有
Lookup-first / 按 PID 单飞 / 隔离窗口 / 激活闸门纪律）；成功即恢复服务，用尽上限则保持冻结
并输出 Error 日志 + `actor_reclaim_total{result="exhausted"}`（可告警）。语义细节见框架仓
`contrib/actor/cluster/doc.go` 的「Lost 自愈（判 Lost 不再永久冻结）」一节。

## 11. actor 目录后端（`actor.directory.*`：etcd | redis）

目录（PID → 属主归属记录）默认仍写 **etcd**（`/atlas/actors/<ns>`，保持现网行为）。
把 `actor.directory.backend` 配成 `redis` 后，写入侧（battle / game 的 `pkg/actor`）和
只读侧（edge 的属主解析）**同时**切到 Redis——两端共用 `pkg/directory` 的同一份解析与
前缀派生，配得不一致会表现为「写入正常、接入层查不到属主」。

**范围**：只有 actor 目录换后端。注册中心、配置中心、rollout 选举、再平衡锁、以及
节点 ID 归属声明（node claim，仍走 etcd 租约键）都不变，故 `registry.etcd.endpoints`
仍然必填。

| 配置 | 缺省 | 语义 |
|---|---|---|
| `actor.directory.backend` | 空 ⇒ `etcd` | 目录后端：`etcd` \| `redis`（其它值启动失败） |
| `actor.directory.redis.endpoints` | 无 | 单点 1 个地址；哨兵形态为全部哨兵地址（backend=redis 必填） |
| `actor.directory.redis.master_name` | 空 | 哨兵监控的主库名（空 = 单点直连） |
| `actor.directory.redis.password` / `db` | 空 / 0 | 认证与库号 |
| `actor.directory.redis.tls` / `insecure_skip_verify` | false | TLS 与测试环境跳过证书校验 |
| `actor.directory.redis.sync_replicas` / `sync_timeout` | 0 / `2s` | 关键写 WAIT 的从库数与时延上限（生产 1 主 N 从建议 `sync_replicas: 1`） |
| `actor.directory.quiesce_grace` | 空 ⇒ 框架 `5s` | Claim 保护窗（租约过期后额外等待） |
| `actor.directory.replication_lag` | 空 ⇒ 不声明 | 主从复制滞后上界，**redis 后端要求 `quiesce_grace >= replication_lag`** |

**前缀**：`/atlas/actors/<ns>`（etcd）与 `atlas:<ns>:actors`（redis）由
`namespace.Derive` 派生，一一对应；同一 Redis 上多套部署按 `<ns>` 隔离。

**监听（Watch）语义**：Redis 后端用 pub/sub 增量 + **订阅后全量对齐**（重订阅再来一次），
写事件带全局单调版本号，重复/乱序事件按版本丢弃；连接正常时靠版本号连续性发现丢消息
（每次检查一次 GET），发现缺口立刻全量。因此属主变更的收敛延迟是「一次 pub/sub 投递」，
断线/丢消息场景是一次全量扫描。

### 11.1 切主双活风险（硬约束）

Redis 续租是高频写，**不等待同步复制**（不 WAIT）。切主时有两段写会丢：

1. **稳态复制滞后**：最后一次续租写还没同步到从库；
2. **切主完成窗口**：哨兵先提升新主，之后才把旧主降为从库——这段窗口里旧主仍在接受续租写，
   而这些写永远到不了新主（实测窗口约 3s，上界是哨兵的 `failover-timeout`）。

两段合起来就是配置项 `actor.directory.replication_lag` 要声明的量（**取上界，不是稳态 P99**；
演练时把 `sentinel failover-timeout` 调小到与它一致最稳）。

框架侧为此做了两件事（都在框架仓，模板无需额外代码）：

- **归属键的 TTL = `lease_ttl` + `quiesce_grace`**：Claim 的「过期后仍拒抢」只在记录还存在时
  生效；键若与记录内的 `lease_deadline` 同时消失，他节点的 `Create` 会直接成功、绕过 grace。
  让键多活一个 grace 后，抢占最早时刻恒为「最后一次成功续租 + `lease_ttl` + `quiesce_grace`」，
  `quiesce_grace >= replication_lag` 才真正闭合双活窗口。
- **装配期校验** `quiesce_grace >= replication_lag`（`pkg/directory.Parse` 与
  `pkg/actor.NewRuntime` 两处），违反即**启动失败**，不静默放行。

**结论（必须遵守）**：`quiesce_grace` 要覆盖「切主完成窗口」上界。若哨兵 `failover-timeout`
是 10s，就配 `quiesce_grace: 10s` 且 `replication_lag: 10s`；只配缺省 5s 而 `failover-timeout`
是 10s，就仍存在一个不超过 5s 的双活窗口（期间投递由 epoch 栅栏挡住，但共享库写不保证）。

**观测口径**：切主演练（`contrib/actor/loadtest` 的 `TestFailoverSingleOwnerDrill`）会用
样本 PID 断言「任一时刻至多一个节点自认属主」；演练拓扑的 `failover-timeout` 必须
≤ 声明的 `replication_lag`，否则演练本身就会红——那不是框架缺陷，而是配置没覆盖真实窗口。
