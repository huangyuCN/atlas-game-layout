# 注册中心身份与隔离设计（键前缀 / 实例 ID / 冲突快速失败）

- 日期：2026-09-21
- 状态：已定稿（使用者确认范围：`runtime.id` 自动生成 + 键前缀按环境隔离 + 注册/发现配置同源 +
  Atlas 侧注册冲突快速失败；沿用本会话「不做兼容处理、直接破坏性变更」原则）
- 范围：Atlas `registry`（错误语义）、`contrib/registry/etcd`（注册/注销事务）、`app.Stop`（停机不阻断）；
  模板 `protobuf/configs/registry.proto`、`lib/consts`、`pkg/registry`、`pkg/fxkit`、`pkg/bootstrap`、
  `services/*/internal/app`、`services/*/assemble`、四个 `configs/config.yaml`、`test/e2e`、`scripts/e2e`。
  不改客户端协议、不改 actor 语义。

## 1. 背景

`fxkit.NewRegistrar` 传入的是空的 `pkgregistry.Options{}`，追下去发现三个独立问题：

1. **死配置面**：`pkgregistry.Options` 的 `Namespace` / `TTL` 在全部 8 处生产调用点都是零值
   （唯一赋非零值的是 `pkg/registry/registry_test.go`），因为 `registry.proto` 里根本没有这两个字段——
   看起来可配、实际不可配。
2. **没有环境隔离**：键前缀硬编码 `/atlas/services`，`runtime.env` 只进链路资源属性与指标标签。
   实例键是 `<前缀>/<服务名>/<实例 ID>`，而四个服务的 `runtime.id` 都是写死的字面量
   （`game-1` / `matcher-1` / `battle-1` / `gateway-1`），`pkg/config` 也没有环境变量插值——
   同一份配置部署到两套环境（或同环境多副本）时实例键**完全相同**。
3. **静默顶替 + 误删**（`contrib/registry/etcd/registry.go`）：
   - `registerWithKV` 是无条件 `Put`（键存在时同时改绑租约）→ 后注册者覆盖前者端点；
     前者的 `heartBeat` 只做 `KeepAlive`、不再写 value，于是它「以为在注册」但数据已被顶掉。
   - `Deregister` 是无条件 `Delete(key)` → **一个环境的进程关停会删掉另一个环境的注册**。
   - `GetService` / `Watch` 按前缀扫描 → 跨环境互相看到对方端点，网关会把请求路由到另一个环境。

另有一处不一致：`atlas.App.New` 在未显式给 ID 时会生成 UUID 作为**注册实例 ID**，而 actor NodeID
取的是 `runtime.id`——`runtime.id` 留空时两者分叉，违反「实例 ID 必须 == actor NodeID」的硬约束。

## 2. 决策表

| 议题 | 决策 |
|------|------|
| 键前缀 | `registry.namespace` 显式配置优先；否则 `/atlas/services/<runtime.env>`，`env` 缺省 `default`（`pkg/registry.NamespaceOf`）|
| 实例 ID | `runtime.id` 显式配置优先；否则主机名（`pkg/bootstrap.fillRuntimeIdentity` **原地回填 cfg**，保证注册实例 ID、actor NodeID、指标 `service_instance_id` 三处同源）；取不到主机名则启动报错 |
| 环境名 | `runtime.env` 缺省 `default`（同样回填），使链路属性、指标标签与键前缀三者一致 |
| 配置同源 | 新增 `fxkit.RegistryOptions[B RegistryConfig]`；`NewRegistrar` / `NewEtcdDiscovery` 共用私有 `newEtcdRegistry` 一条构造路径；`pkg/registry.NewEtcd` 返回同时实现 Registrar 与 Discovery 的具体类型（删除 `NewEtcdDiscovery`）|
| TTL | `registry.ttl_seconds`（`optional int32`，秒）：未配置留零值交给底层默认 15s；配置为 ≤0 启动报错 |
| 注册冲突 | `registerWithKV` 改为事务：仅当键不存在、或键仍由**本注册器为该键持有的租约**占用时才写；否则返回 `registry.ErrInstanceConflict`（含占用方租约号）|
| 注销归属 | `Deregister` 改为事务：`LeaseValue(key) == 本实例租约` 才删除，否则返回 `ErrInstanceConflict` 且不动该键 |
| 租约记录 | `serviceCancel` 增加 `leaseID`，`ctxMap` 加互斥锁（原先注册/心跳/注销并发访问无保护）；心跳重注册成功后同步更新租约号 |
| 心跳重复注册 | 同键重复 `Register` 时先取消上一次的心跳，避免旧租约被持续续租而泄漏 |
| 启动失败升级 | `pkg/bootstrap` 新增 `StartSignal`：`App.AfterStart` 上报「已注册」，`RegisterLifecycle.OnStart` 等待结果——注册失败即 fx 启动失败、进程退出（原先只记一条 Error 日志，进程带着「端口已监听但未注册」的半死状态继续跑）|
| 停机不阻断 | `atlas.App.Stop` 注销失败只记录错误、仍取消上下文（原实现 `return err` 会跳过 `a.cancel()`，传输层不停止、进程退不出去）|
| 嵌入式形态 | `services/*/assemble.Options` 增加 `Namespace`；测试/脚本传**本次运行独占**的前缀（`/atlas/services/it-<纳秒>`、`e2e-<纳秒>`），与常驻进程隔离并规避上次运行残留实例键 |
| 服务/actor 发现 | **不隔离**：actor 集群成员就是服务实例（`cluster` 用 `Discovery.GetService(serviceName)` 取候选并转 Node），两套前缀会把集群打散；要隔离的是「环境」与「实例」 |

## 3. 变更清单

Atlas：

| 文件 | 变更 |
|------|------|
| `registry/registry.go` | 新增 `ErrInstanceConflict` 哨兵 |
| `contrib/registry/etcd/registry.go` | 注册/注销事务化、租约归属记录与互斥、心跳同步租约号、重复注册取消旧心跳 |
| `app.go` | `Stop` 注销失败不再阻断停机 |
| `contrib/registry/etcd/registry_conflict_integration_test.go` | 新增 4 个集成用例（`//go:build integration` + `ETCD_ENDPOINTS`）|
| `app_test.go` | `TestDeregisterError` 增加「传输层必须停止」断言 |

模板：

| 文件 | 变更 |
|------|------|
| `protobuf/configs/registry.proto`（+ `.pb.go`）| 新增 `namespace`、`optional int32 ttl_seconds` |
| `lib/consts/consts.go` | 新增 `EnvDefault` |
| `pkg/registry/registry.go` | `NamespacePrefix`、`NamespaceOf`；`NewEtcd` 返回具体类型（删除 `NewEtcdDiscovery`）|
| `pkg/fxkit/fxkit.go` | `WithRuntime`、`RegistryConfig`、`RegistryOptions`；`NewRegistrar[B]` / `NewEtcdDiscovery[B]` |
| `pkg/bootstrap/assemble.go` | `fillRuntimeIdentity`（回填 `runtime.id`/`runtime.env`）|
| `pkg/bootstrap/bootstrap.go` | `StartSignal`、`RegisterLifecycle` 等待启动结果 |
| `services/*/internal/app/{graph,infra,data}.go` | 图内提供 `fxkit.NewEtcdDiscovery[*conf.Bootstrap]`；`NewActorRuntime`/`NewActorClient`/`newMatchQueueClient` 改为注入 `registry.Discovery`（不再各自 `Options{}`）|
| `services/*/assemble/assemble.go` | `Options.Namespace`；`registerInstance` 走 `fxkit.RegistryOptions` |
| `services/*/configs/config.yaml` | 删除写死的 `id`，改为注释说明缺省主机名；`env` 注释补「决定键前缀」 |
| `test/e2e/env_test.go`、`scripts/e2e/main.go` | 本次运行独占前缀 |
| `test/e2e/registry_test.go` | 新增：冲突快速失败、跨前缀同名同 ID 共存且互不可见 |

## 4. 验证（集成服务器 10.10.9.36）

| 验证项 | 结果 |
|--------|------|
| Atlas 注册器集成用例（真 etcd）| 4/4 PASS：冲突拒绝且不覆盖、非归属方注销不删他人注册、同实例重注册幂等、租约被回收后心跳重注册仍生效 |
| 模板全量测试 | 本地 44 包 ok / 0 FAIL；服务器 44 包 ok / 0 FAIL |
| `test/e2e`（真中间件）| 10/10 PASS，且**四个常驻进程同时在跑**（旧实现下两边共用同一前缀）|
| 命名空间落地 | etcd 键为 `/atlas/services/test/{game,gateway,matcher,battle}/shimmer-bi-MS-7D17`；旧前缀键已随优雅停机清空 |
| 实例 ID 同源 | 启动日志 `id=shimmer-bi-MS-7D17`；指标 `service_instance_id="shimmer-bi-MS-7D17"` |
| 冲突快速失败（进程级）| 同 ID 第二实例（改端口避开占用）`exit=1`，日志 `实例键 /atlas/services/test/game/<主机名> 已被租约 … 占用（本实例租约 0）`；原实例注册未被改动 |
| 异常退出重启 | `kill -9` 后立即重启 → `exit=1`（冲突）；等待租约 TTL 15s 后重启成功，键重新出现 |
| 全链路 | `go run ./scripts/e2e -mode dual` → 「e2e 闭环通过（dual 形态）」 |

## 5. 破坏性变更

- `pkg/registry.NewEtcd` 返回类型由 `registry.Registrar` 变为 `*etcdreg.Registry`；`NewEtcdDiscovery` 删除。
- `pkg/fxkit.NewRegistrar` 由 `func(*clientv3.Client)` 变为泛型 `func[B RegistryConfig](B, *clientv3.Client)`；fx 图内需写 `fxkit.NewRegistrar[*conf.Bootstrap]`。
- `services/*/internal/app` 的 `NewActorRuntime` / `NewActorClient` / `newMatchQueueClient` 参数由 `*clientv3.Client` 改为 `registry.Discovery`。
- `services/*/assemble.Options` 新增 `Namespace`；`registerInstance` 增加 cfg 参数。
- 键前缀默认值由 `/atlas/services` 变为 `/atlas/services/<env>`：升级后旧注册不会被新进程发现（滚动升级需整批切换）。
- 配置文件删除 `runtime.id`，实例 ID 变为主机名（指标 `service_instance_id`、注册实例 ID、actor NodeID 同步变化）。

## 6. 权衡与遗留

- **异常退出后 15s 内无法重启同名实例**：这是「拒绝静默顶替」的代价。错误信息给出了占用租约号，等待 TTL 过期即可；若要缩短窗口可下调 `registry.ttl_seconds`（代价是心跳更频繁）。
- **同主机多副本**仍需显式配唯一 `runtime.id`（且端口本就需各自配置）；跨主机多副本靠主机名天然唯一。
- **Atlas 未做「实例键归属租约已死」的探测**：无法区分「旧进程已死但租约未过期」与「另一个活进程持有」，故一律拒绝；如需平滑接管，可在后续版本引入显式接管开关。
