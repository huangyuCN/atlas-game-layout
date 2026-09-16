# 玩家存档耐久性设计（第一期）

- 日期：2026-09-16
- 状态：已批准（对齐 shimmer/server 同名设计，2026-09-16）
- 范围：`services/game/internal/{data/{models,repo},actor}` 的加载 / 统一落盘 / 下线降级 / 在线冻结；
  `models.Player` 增加单调 `persistVersion`。不改客户端协议。

## 1. 背景

改造前链路（`repo/store.go`、`actor/player.go`）：

- 在线权威在内存聚合根。每 10s 刷一次 redis（明文 JSON，TTL 5 分钟）。
- 下线在 `OnStop` 写 mongo，**失败只返回 error**（stop 流程不重试，聚合根照常销毁）。
- 登录加载「redis 优先、mongo 兜底」，无版本比较——旧快照可回档。
- redis 快照失败是静默的；下线落库失败无任何恢复路径。

结论：单边失败可能被旧缓存回档；双边失败必丢整个在线期变更。本期改造目标：**在线热路径只写 Redis，RPO（恢复点目标）约 3 分钟；单边失败可恢复；双边失败冻结改档避免进度堆积**。

## 2. 决策表

| 议题 | 决策 |
|------|------|
| 丢失上限 | 最多约 3 分钟（tick 周期，第一期已批准取舍） |
| 刷盘周期 | 约 3 分钟，只刷 Redis（成功不写 Mongo） |
| 写入关系 | Redis 主路径；Redis 失败才降级 Mongo |
| 登录选源 | 两边都读（Redis 命中时仍读 Mongo），按 `persistVersion` 取大；相同用 Redis |
| Redis 连不上 | 不能登录（key miss 仍回落 Mongo） |
| 双边失败 | 在线冻结（拒绝改档、保连接、每分钟重试），不 Kick 不 Terminate |
| 冻结后 Redis 恢复 | 解冻继续玩 |
| 冻结后仅 Mongo 成功 | 强制下线（在线热路径依赖 Redis） |
| 挂起不退出 | 第一期不做；下线同步短重试 3×500ms 后无论成败退出 |
| PERSIST | Mongo 失败则 key 转永不过期；登录补写 Mongo 成功后 EXPIRE 72h；**禁止先 DEL、禁止失败路径 DEL** |

## 3. 序列化

redis 存档 = **压缩后的 BSON**：`bson.Marshal`（PlayerSnapshot 视图，与 mongo 文档同构）→ **s2 压缩**（klauspost/compress）→ 1 字节格式版本头（演进留位）。认证字段（Salt/Password）**不进快照**（凭据只存 mongo，登录口令校验走 `LoadCredential` mongo 专用读）。

## 4. persistVersion

- `models.Player.PersistVersion uint64`（bson tag，不进客户端协议，不进快照 JSON 面）。
- 仅在统一落盘且 CRC 判脏后自增一次，Redis/Mongo 共用同一版本；老档视为 0。
- 指纹（CRC）对 `persistVersion` 置零后的快照编码计算，保证「数据没变 + 版本没变」时指纹稳定。
- 实现走 COW 生成的 `PutPersistVersion` 语义（持久化侧副作用不进 undo 回滚栈）。

## 5. 职责划分

| 角色 | 职责 |
|---|---|
| 内存聚合根 | 在线权威。冻结后不再改档。 |
| redis `atlas:player:{playerID}` | 耐久缓冲 + 崩溃/下线兜底。正常 TTL 72h；Mongo 失败转 PERSIST。 |
| mongo `player` | 权威持久库。下线、显式落地、Redis 失败降级。 |

## 6. 三条降级链

### 6.1 周期落盘（3 分钟 tick）

数据指纹判脏 → 跳过（版本不涨）；脏 → `persistVersion++` → 编码一次 → **先写 Redis（成功即止）** → 失败用**同一份编码与同一版本**写 Mongo → Mongo 成功 → **强制下线**（在线热路径依赖 Redis，先踢再停止）→ 双失败 → **进入在线冻结**。

### 6.2 在线冻结

- 冻结标记后全部改档 handler 守卫拒绝（`SERVER_FROZEN`，HTTP 503 语义）；查询类放行。
- **不 Kick、不 Terminate**——踢人会触发下线落盘链路，冻结态会丢。
- 定时器改每分钟重试（仍先 Redis 后 Mongo）：Redis 恢复 → 解冻；仅 Mongo 成功 → 强制下线；一直失败 → 保持冻结（进度不再堆积）。
- 冻结期收到完整 Login（仍已登录）→ 按现状落盘+退出，等同第一期下线语义。

### 6.3 下线（Logout / 停止）

**流程反转**：落库成功才发起 `ctx.Stop`（OnStop 退化为零失败收尾）。同步短重试 3×500ms（Agent 内执行）：

| Redis | Mongo | 退出前动作 |
|---|---|---|
| 成功 | 成功 | TTL 保持或设回 72h |
| 成功 | 失败 | 该 key `PERSIST`；Error 日志 + 指标 |
| 失败 | 成功 | 以 Mongo 为准；Redis 可能仍是旧缓存（登录比版本纠正） |
| 失败 | 失败 | Error 日志 + 指标后仍退出；丢已冻结约 3 分钟 |

### 6.4 登录选源与对齐

1. Redis 连不上（非 key miss）→ 登录失败；
2. Redis miss → 只读 Mongo；
3. 命中 → 再读 Mongo，`persistVersion` 大者胜（相同用 Redis）；Mongo 读失败但 Redis 有档 → 用 Redis；
4. 选源后双向对齐补写：选中 Redis → 补写 Mongo（成功且原为 PERSIST → EXPIRE 72h；失败保持 PERSIST）；选中 Mongo → 覆盖 Redis（TTL 72h）。

## 7. 运维约束

- 玩家存档 redis 的 `maxmemory-policy` **必须 noeviction 或 volatile-***：PERSIST 后的无 TTL key 在 allkeys-* 策略下仍会被 LRU 淘汰（指标需能发现 PERSIST 后 key 丢失）。
- mongo 故障为全局事件：探活拒绝登录 + 在线冻结 + 强制下线，恢复后自然回归。

## 8. 指标（可观测性）

Redis 写失败、Mongo 降级成功、进入冻结、PERSIST 次数、登录选源分布（Redis/Mongo）、PERSIST 后 key 丢失。模板未接 metrics 后端，指标接线交由使用者按需接入（日志已有 Error/Info 分级打点）。

## 9. 测试

`repo/store_test.go` 对照设计场景表（fake 注入）：Redis 成功不写 Mongo、降级成功、CRC 跳过、版本相同用 Redis、Mongo 更新覆盖 Redis、连不上拒登、PERSIST→EXPIRE 闭环、老档视为 0。故障注入运维步骤：mongo 容器 pause/unpause（10.10.9.36），验证五形态 e2e + 冻结/解冻/强制下线链路。

## 10. 后续（不在第一期，交由开发者按需自建）

- 双库失败时 Agent 挂起直到至少一边写成功再退出（再救已冻结的约 3 分钟）。
- 后台扫描 PERSIST 脏 key，不依赖玩家再次登录来补 Mongo。
- 在线异步落 Mongo（需内存快照，避免卡住 Actor 邮箱）。
