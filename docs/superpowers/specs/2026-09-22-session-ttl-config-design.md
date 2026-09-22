# 会话租期配置化设计（gateway 会话 TTL / 清扫周期）

- 日期：2026-09-22
- 状态：已定稿（使用者确认：gateway 会话租期全量——`ttl` + `sweep_interval`，清扫周期可配、缺省 `ttl/2`）
- 范围：新增 `protobuf/configs/session.proto`；`services/gateway/internal/conf/conf.proto` 引用；
  `services/gateway/internal/session`（`Options` + 清扫周期）；`services/gateway/internal/app/infra.go`（配置映射）；
  `services/gateway/configs/config.yaml`、`AGENTS.md`。不改网络协议、不改会话语义。

## 1. 背景

会话租期此前是 gateway 里的一枚硬编码常量：

```go
// services/gateway/internal/app/infra.go
const sessionTTL = 30 * time.Second // 会话路由的默认租期（心跳续租周期，与 game 会话租期对齐）
```

它同时决定四件事：

| 生效点 | 用途 |
|--------|------|
| `Manager.Bind` | redis 路由表 `SET ... EX ttl`（路由存活窗口） |
| `Manager.Heartbeat` | 心跳续租 `EXPIRE ttl`（客户端按 SDK 心跳周期调用，默认 30s、集成脚本 10s） |
| `Manager.SweepOnce` | 过期判定阈值（`LastHeartbeat` 早于 `now - ttl` 即过期） |
| `Manager.Start` | 过期清扫 ticker 周期（**恒等于 `ttl/2`，与租期绑死**） |

两个痛点：改租期要重新编译发版；清扫节奏无法独立调整（调大租期会让清扫同步变稀疏，
调小租期又会让清扫变密集）。本轮把这两个参数收口到 `protobuf/configs/`，与传输层启动参数同一套约定。

## 2. 决策表

| 议题 | 决策 |
|------|------|
| 配置位置 | 新增公共配置 `atlas.configs.Session`（`protobuf/configs/session.proto`），gateway `Bootstrap.session` 引用（仅 gateway 消费） |
| 字段表达 | `string ttl` / `string sweep_interval`——延续「proto 用 string 表达 duration、`pkg/config.ParseDuration` 解析」的既有约定（proto3 无 duration 标量，`google.protobuf.Duration` 在 YAML 里写起来啰嗦且零值语义含糊） |
| 空值语义 | 空串 = 不覆盖：`ttl` 取 30s、`sweep_interval` 取生效 `ttl/2` |
| 非法值 | 无单位 / 解析失败 / 非正值一律**启动期报错**（不静默降级——「配了不生效」最难排查） |
| 默认值落点 | 默认租期下沉到 session 包（`session.DefaultTTL`），装配层只做「解析 + 透传 0」；默认值只写一次 |
| 构造函数形态 | `NewManager(store, instanceID, session.Options{TTL, SweepInterval})`——两个相邻的 `time.Duration` 裸形参在调用侧极易写反，用具名结构体消除 |
| 清扫周期下限 | 生效值下限 1ms（`time.NewTicker` 对非正值 panic；极小租期配置下自保，与旧实现的内联保护等价） |
| 关系校验 | **不做** `sweep_interval < ttl` 校验：清扫比租期稀疏是合法运维选择（会话多留一会儿再清），校验只会挡住合理配置 |
| 客户端心跳 | **不做**：心跳周期属于 SDK（`sdkclient.WithSessionHeartbeatInterval`），服务端协议面不表达客户端行为 |
| 其他租期 | **本轮不动**：matcher 票据/映射 TTL（5min）、game 快照 TTL（72h）与落盘周期（3min）是另一批领域语义，不在「会话租期」范围 |

## 3. 配置面

```proto
// protobuf/configs/session.proto
message Session {
  // ttl 是会话路由租期（redis 路由 TTL 与心跳续租周期）；空 = 默认 30s。
  // 取值应 ≥ 客户端会话心跳周期的 3 倍（SDK 默认心跳 30s，故用默认心跳时 ttl 应 ≥90s）。
  string ttl = 1;
  // sweep_interval 是过期会话清扫周期；空 = ttl 的一半。
  string sweep_interval = 2;
}
```

```yaml
# services/gateway/configs/config.yaml
session:
  ttl: 30s                # 会话租期（redis 路由 TTL + 心跳续租周期；空 = 默认 30s）
  # sweep_interval: 15s   # 过期会话清扫周期（空 = ttl 的一半）
```

`ttl` 与客户端心跳的关系：**生效租期应 ≥ 客户端会话心跳周期的 3 倍**（允许连丢两次仍不掉线）。
心跳周期由客户端 SDK 决定：`sdkclient.WithSessionHeartbeatInterval` **默认 30s**，本仓集成脚本
（`scripts/e2e`、`test/e2e`）显式设为 10s。故模板 `ttl: 30s` 对应 10s 心跳；客户端若用 SDK 默认心跳，
须把 `ttl` 配到 ≥90s，否则活跃会话会被误判过期清扫并触发下线联动。下调会缩短断线发现时间，
代价是 redis 心跳写更频繁。

## 4. 生效链路

```text
config.yaml: session.ttl / session.sweep_interval
  → conf.Bootstrap.session（protojson 按字段名解组）
  → app.sessionOptionsOf：ParseDuration（空 = 0 透传，非法 = 报错）
  → session.Options{TTL, SweepInterval}
  → session.NewManager → Options.resolve()：ttl<=0 → DefaultTTL；sweep<=0 → ttl/2；两者下限 1ms
  → Manager.ttl / Manager.sweepInterval
      ├─ Bind        ：路由写入 TTL
      ├─ Heartbeat   ：续租 TTL
      ├─ SweepOnce   ：过期判定阈值
      └─ Start       ：清扫 ticker 周期
```

`newSessionManager` 因此从「纯构造」变为「可失败构造」（`(*session.Manager, error)`）——
fx 在启动期即报错，配置错误不会拖到运行期。

## 5. 变更清单

| 文件 | 变更 |
|------|------|
| `protobuf/configs/session.proto`（新） | `atlas.configs.Session`（`ttl` / `sweep_interval`）+ 生成物 `session.pb.go` |
| `services/gateway/internal/conf/conf.proto` | `Bootstrap` 新增 `atlas.configs.Session session = 6` |
| `services/gateway/internal/session/options.go`（新） | 新增 `DefaultTTL` / `Options` / `resolve()`（session.go 已 456 行，逼近 500 行上限故独立成文件） |
| `services/gateway/internal/session/session.go` | `Manager` 持有生效 `ttl` / `sweepInterval`；`NewManager` 改收 `Options`；`Start` 用生效清扫周期 |
| `services/gateway/internal/app/infra.go` | 删除 `sessionTTL` 常量；新增 `sessionOptionsOf`；`newSessionManager` 返回 error |
| `services/gateway/configs/config.yaml` | 新增 `session` 节（`ttl` 显式写出 + `sweep_interval` 注释示例） |
| `AGENTS.md` | 新增 `## protobuf/configs — 公共配置面` 小节，含「会话租期」 |

## 6. 验证

| 用例 | 断言 |
|------|------|
| `session.TestOptionsResolve` | 零值 → (30s, 15s)；只给 ttl → 清扫 = ttl/2；两个都给 → 原样；非法/非正 → 回落默认；极小 ttl → 清扫下限 1ms |
| `session.TestStartHonorsSweepInterval` | `ttl=2s、sweep=10ms` 下过期会话在 400ms 内被清扫（默认 `ttl/2=1s` 时该断言不成立，用于区分「配置真的生效」） |
| `session.TestHeartbeatAppliesConfiguredTTL` | 配置 `ttl=90s`：`Bind` 写入 ≈90s；把路由 TTL 压到 5s 后 `Heartbeat` 必须续回 ≈90s（两跳各自可判别，不靠 Bind 的副产物） |
| `app.TestSessionOptionsOf` | 空配置 → 零值 Options 无错；`ttl=45s`/`sweep_interval=5s` 正确映射；`abc`/`0s` → 报错且错误信息指明字段 |
| `assemble.TestServiceConfigLoads`（扩展） | 模板真实 `config.yaml` 的 `session.ttl` 解组为 `30s` |
| 集成（服务器 10.10.9.36，守护形态） | `session.ttl: abc` → 进程**启动失败**（`app: session.ttl 无效: config: 时长 "abc" 解析失败…`，exit=1），证明配置经 `bootstrap.Assemble → newSessionManager → sessionOptionsOf` 真正参与启动；恢复 `30s` 后正常监听 9001。模板自带 e2e 驱动（进程内形态）12/12 与 `scripts/e2e -mode dual` 闭环通过 |

### 6.1 生效边界（评审确认的已知范围）

- **进程内形态（`services/*/assemble`）不读 `config.yaml`**：它由 `newBootstrap(o)` 合成 `conf.Bootstrap`（只映射地址/注册中心/数据源），
  因此 `session` 配置只作用于进程形态；进程内形态固定用默认租期（30s / ttl/2）。集成脚本的客户端心跳 10s，与默认租期是 3 倍关系，安全。
  若将来需要进程内形态也调租期，在 `assemble.Options` 加字段并透传到 `newBootstrap` 即可（本轮不做，避免范围蔓延）。
- **`scripts/e2e -mode fault` 的「会话过期联动」不是租期敏感用例**：实测默认配置与 `ttl=5s` 配置下都在 ~5s 内通过——
  它观察到的是重登接管导致的票据取消，而非 TTL 到期清扫。租期是否生效由单元测试（`Bind`/`Heartbeat`/`Start` 三处变异均被检出）
  与上面的启动期校验共同覆盖。

## 7. 破坏性变更

按会话约定不做兼容处理：

- `session.NewManager(store, instanceID, ttl)` → `session.NewManager(store, instanceID, session.Options)`（签名变更，调用点仅 gateway 装配与测试）。
- `services/gateway/internal/app/infra.go` 的 `const sessionTTL` 删除；未配置时默认值仍是 30s，行为不变。
- gateway `Bootstrap` 新增字段 `session`（YAML 不写即全默认，不影响存量配置文件的可用性）。

## 8. 独立评审与修复（两轴：规范 / 规格）

| 发现 | 轴 | 处理 |
|------|----|------|
| 注释宣称「客户端默认每 10s 一次心跳」与事实不符：SDK 的 `WithSessionHeartbeatInterval` **默认 30s**（10s 只是本仓集成脚本的显式覆盖）；且默认 `ttl=30s` 与 SDK 默认心跳等值，活跃会话会在清扫边界被误判过期 | 规范 | 五处注释（`session.proto` / `options.go` / `config.yaml` / `AGENTS.md` / 本规格 §3）改为事实描述，并写明规则「生效租期 ≥ 客户端心跳的 3 倍」与「客户端用 SDK 默认心跳时 `ttl` 须 ≥90s」；模板 `ttl: 30s` + 集成脚本 10s 心跳的组合保持不变（不改变断线发现时延） |
| `Manager.sweepEvery` 与 `Options.SweepInterval` 同一概念两种叫法 | 规范 | 统一为 `sweepInterval` |
| 「会话租期」小节挂在 `## pkg/` 下，但内容属配置面而非 pkg 产物 | 规范 | 新增顶层小节 `## protobuf/configs — 公共配置面` 承载 |
| 生成物 protoc-gen-go 版本混杂（`registry.pb.go`/`server.pb.go` 为 1.36.10，其余 1.36.12） | 规范 | 用当前插件重新生成这两个文件（各只差版本注释一行），全目录统一 1.36.12 |
| `TestHeartbeatAppliesConfiguredTTL` 无判别力：90s 已由 `Bind` 写入，删掉心跳里的 `Expire` 该用例仍通过（变异实测） | 规格 | 用例改为「`Bind` 断言 ≈90s → `Expire` 压到 5s → `Heartbeat` → 断言续回 ≈90s」，两跳各自可判别 |
| 规格 §5 写新代码落在 `session.go`（实际在 `session/options.go`）、§6 写 300ms（实际 400ms） | 规格 | 规格文本订正 |

评审同时确认：四个生效点全部由配置驱动、无残留硬编码；matcher 票据 TTL 与 game 快照 TTL 未被误改；无范围蔓延。
