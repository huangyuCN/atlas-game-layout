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
| `server.kcp.idle_timeout` / `server.udp.idle_timeout`（battle） | 无（本服务按掉线窗口推导） | 空/`0s` ⇒ 取 `battle.offline_timeout / 3`（缺省 15s → 5s） | 显式值必须**严格小于** `battle.offline_timeout`，否则装配期启动失败（数据报面无关闭握手，掉线只能靠空闲读超时发现，见 §7） |
| `battle.ticket_key` | 非空 base64 且解码后 32 字节 | **启动失败**（无默认值） | 必须与 `edge.ticket_key` 同值，否则帧面全面拒票 |
| `battle.ticket_ttl` | 无 | — | 空 ⇒ 120s；必须为正；应大于 `offline_timeout`（窗口内重连复用同一张票） |
| `battle.offline_timeout` | 无 | — | 空 ⇒ 15s；显式 `0s` = **关闭掉线判定**（此时数据报面 idle 不推导也不校验）；负值/非法即失败 |
| `battle.max_frames` / `battle.tick_interval_ms` | 无 | — | **整项缺失** ⇒ 60 帧 / 100ms（与硬编码常量逐字相同，行为零变化）；**显式值**必须为正且不超上限（`max_frames ≤ 20000`、`tick_interval_ms ≤ 1000`），显式 `0`/负值/超上限一律启动失败（未配置与显式 `0` 语义不同，故用 `optional`；依据见 §7.6） |
| `battle.edge_endpoints` | 列表非空 + `transport` 非 `UNSPECIFIED` + 地址非空 + 面不重复 | **启动失败**（客户端拿到的接入层地址只有这一个来源） | 面枚举 `EDGE_TRANSPORT_{WS,KCP,UDP}`；**按面下发**、不要求三面齐全（只开 ws 也能启动） |
| `battle.frame_advertise_host` | 无（整项缺失 = 回落） | 整项缺失 ⇒ 回落 `edge_endpoints` 的主机（**仅适合同机部署**） | 只填主机（不含端口、IPv6 不带方括号）；空白串/带端口/带方括号即启动失败；跨机部署**必须**显式配置（语义见 §7） |
| `edge.ticket_key` | 非空 base64 且解码后 32 字节 | **启动失败**（无默认值） | 必须与 `battle.ticket_key` 同值 |
| `edge.frame_service` | 无 | — | 空 ⇒ `battle-frame`（battle 注册的帧面实例服务名） |
| `edge.actor_namespace` | 无 | — | 空 ⇒ 取 `runtime.namespace`（**不是**注册键前缀路径）；必须与 battle 同值，否则查不到属主 |
| `edge.listeners` | 至少一面，且 `name`/`network`/`addr`/`carrier` 齐备 | **启动失败**（`edge.listeners 不能为空`） | `name` 必须与 battle 帧面实例元数据里的端口键（`ws`/`kcp`/`udp`）一致；KCP/UDP 面只能配 `CARRIER_DATAGRAM`（框架不允许数据报面配 `stream_hello`），WS 面配 `CARRIER_WS_UPGRADE` |
| `edge.max_streams` / `edge.idle_timeout` / `edge.new_conn_rate` / `edge.max_per_ip` | 无 | — | 空/非正 ⇒ 框架缺省（并发流 `1024` / 空闲回收 `60s` / 不限新连接速率 / 不限每 IP 连接数） |
| `edge.dial_timeout` / `edge.probe_after` / `edge.probe_timeout` | 无 | — | 空 ⇒ 框架缺省（拨号与握手读取 `5s`；探活触发取 `idle_timeout`、等待回应取触发时长的一半、下限 `50ms`） |

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
| `battle_offline_timeouts_total` | > 对局数的 5%/分钟 | 客户端保活缺失（含 **UDP/WS 的 SDK 周期心跳**这一已知待办） |
| `battle_reconnects_total` | 与 `battle_offline_timeouts_total` 同看 | 回座次数；**回座率骤降**说明窗口内重连不成功（票过期/接入层不可达） |
| `battle_online_players` | 突降（同比 < 50%） | 战斗域直连在线人数；突降伴随 `edge_streams_active` 同步下跌即接入层或帧面故障 |
| `TRANSPORT_DOWNLINK_FAILED`（错误 reason） | 出现即告警（5 分钟增量 > 0） | 一次性回执超过单包上限，下行无法投递（见 `atlas/errors/class.go`；按 reason 计数需服务端在错误路径打点） |

**落地位置**（阈值即上面这张表，规则与面板同源，改一处要同步改另一处）：

- Prometheus 告警规则：`deploy/observability/rules/atlas-alerts.yml`（`promtool check rules` 可校验）；
- Grafana 面板：`deploy/observability/grafana/provisioning/dashboards/json/atlas-stage3.json`。

### 7.6 局时长可配（`battle.max_frames` / `battle.tick_interval_ms`）

单局寿命 = `max_frames` × `tick_interval_ms`。两者**缺省即现值**（60 × 100ms ≈ 6s）——不配置时
装配结果与硬编码常量逐字相同，行为零变化。此前这两个值是 `services/battle/internal/actor`
里的常量，导致「单局内跑一段 8s 静默窗口」在物理上不可达（局先结束了）。

| 配置 | 形态 | 缺省 | 校验（违反即**启动失败**，与 `offline_timeout` 同风格） |
|---|---|---|---|
| `battle.max_frames` | `optional int64`（帧数） | 60 | `>0` 且 `≤ 20000`；显式 `0`/负值/超上限报错 |
| `battle.tick_interval_ms` | `optional int64`（**毫秒整数**） | 100 | `>0` 且 `≤ 1000`；显式 `0`/负值/超上限报错 |

- **为什么是毫秒整数而不是 `google.protobuf.Duration`**：本文件既有的两个时长字段
  （`ticket_ttl` / `offline_timeout`）都是「字符串 + `time.ParseDuration`」，全仓无任何
  `Duration` 用法；毫秒整数与 `protobuf/configs/server.proto` 的数值风格一致，且免去
  `Duration` 秒/纳秒双字段的误配。
- **为什么用 `optional`**：proto3 裸标量区分不出「未配置」与「显式 0」。缺省必须保持现值，
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
- **长局验收**（8s 静默窗口）：把 `max_frames` 调到 `300`（30s 一局），用 Go SDK 的
  `examples/directloop -idle-hold 8s` 断言「静默期不被判出局 + 仍收帧 + 静默后仍能补帧」。

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
- **取值**：首个支持「战斗帧直连」的 SDK 版本。模板示例 `0.1.0` 只拒绝不报版本与 `0.0.x` 的旧客户端
  （不误伤在用 SDK：Go `0.5.0` / TS `0.6.0` / C# `0.1.0` 均通过）；三 SDK 直连版本发布并确认覆盖后上调
  （如 `0.7.0`）。**必须先发 SDK 再上调**，否则在用客户端会被一并拒绝。

### 8.2 版本比较与非法串处置

- 逐段**数字**比较（`0.10.0 > 0.9.0`，不是字符串比较）；段数不足按 0 补齐（`1.2` == `1.2.0`）；
  构建元数据忽略（`1.2.0+build.5` == `1.2.0`）；两侧空白容忍；预发布低于同号正式版
  （`1.2.0-rc.1 < 1.2.0`；预发布之间按字符串比较）。
- **版本串非法按「低于门槛」处理**（复用同一 reason `CLIENT_VERSION_TOO_LOW`）：新增独立 reason 要改
  `api/error/v1` 协议，且客户端与客服的分支只需「升级客户端」一条；用 message 的成因措辞区分即可。
  未上报（空串）同样拒绝——旧客户端恰恰多数不报版本，放行等于把「匹配成功但连不上」放回来。
