# 状态哈希口径统一（帧级与快照同值）

- 日期：2026-11-03
- 仓库：`atlas-game-layout`（仅本仓，未动框架仓 `atlas`）
- 状态：已实现（未提交）

## 1. 背景与问题

同一次对局状态存在**两条哈希口径**：

| 位置 | 旧实现 | 代码 |
|------|--------|------|
| 帧级 `LockstepFrame.state_hash` | `Battle.Hash()`：定向二进制编码 + 内联 FNV-1a 64 | `services/battle/internal/biz/simulator/hash.go` |
| 快照 `SnapshotMeta.state_hash` | `json.Marshal(State)` + `fnv.New64a` | `services/battle/internal/biz/simulator/simulator.go` 的 `Snapshot` |

同一状态必然产出两个不同数值（JSON 字节 ≠ 定向编码字节）。客户端把「帧级 hash」与「快照 hash」都当作同一份「状态指纹」做比对（快照帧、补帧、重连、回放核对），
于是**跨帧/跨快照比对必然误报失步**（desync 假阳性）；`DesyncHook.Check` 收到的也是 JSON 口径值。

## 2. 决定

**帧级与快照共用同一 hash 口径（定向编码 + FNV-1a 64）**，只保留一条实现：

- 快照路径：`Battle.Snapshot()` 的 `Hash` 字段直接取 `b.Hash()` 的返回值（`simulator.go`），
  `json.Marshal` + `fnv` 的哈希路径**整体删除**（`hash/fnv` 从 `simulator.go` 移除，不留第二套）。
- `Snapshot.State` 仍是 JSON：它要**可还原**（`Restore`、结算解析 `checkSettle`、迁移透传），
  与「哈希口径」是两件事——序列化 != 哈希。
- 线上（proto 的 `bytes StateHash` / `state_hash`）统一由 actor 层 `hashBytes(uint64)` 编码：
  **uint64 按固定 8 字节大端**（`services/battle/internal/actor/battle_frame.go`）。
  帧级与快照级都走同一个函数，因此**逐字节相等**。

### 字节序说明（两层，不可混）

1. **哈希内部**：整数按 8 字节**小端**并入 FNV-1a 的混合顺序（`hash.go` 的 `hashU64`），
   只影响哈希数值、不影响可比性；`hash_test.go` 的独立参考实现按同一混合顺序钉住。
2. **线上**：哈希值（uint64）按 **8 字节大端**写成 `bytes`（`battle_frame.go` 的 `hashBytes`），
   帧级 `LockstepFrame.state_hash` 与快照 `SnapshotMeta.state_hash` 完全一致。
   `HasHash=false` 时帧级留空（不得用 8 个零字节冒充真实哈希）。

## 3. 用例（TDD：先红后绿）

| 用例 | 断言 | 旧实现 |
|------|------|--------|
| ① `TestSnapshotHashEqualsFrameHash` | 同一状态：快照 hash == 帧级 hash，且**大端 8 字节逐字节相等** | **真红**（四个子用例全不等） |
| ① 端到端 `TestBattleFrameStateHashPerFrame`（快照帧新增断言） | 快照帧的 `SnapshotMeta.StateHash` 与同帧 `LockstepFrame.StateHash` 逐字节相等 | **真红**（第 10 帧两值不同） |
| ② `TestSnapshotAndFrameHashTrackStateChanges` | 位置/得分/胜者/增删玩家任一变化 ⇒ 快照 hash 变、仍与帧级相等、互不撞哈希 | **真红** |
| ③ `TestSnapshotHashStableAcrossRestore` | 快照 → `Restore` → 再取快照，前后 hash 同值且等于帧级重算值 | **真红** |
| ④ `TestStateHashBytesSameCaliber` | 用真实模拟器复现会话取值：帧级与快照线上字节相等、为大端 8 字节（首字节=最高字节），并覆盖 0/1/高位/全字节往返 | **真红**（旧实现快照字节来自 JSON 口径，字节不等） |

辅助：`stateMutations()` / `baseState()` / `simOfState()` 抽成共享测试助手，帧级与快照属性测试共用同一变异集。

## 4. Benchmark 前后对照

环境：Apple M3 / `go1.26.5 darwin/arm64` / `GOMAXPROCS=8`；
命令：`go test ./services/battle/internal/biz/simulator/ -run '^$' -bench 'BenchmarkHash|BenchmarkSnapshotHash' -benchmem -count=5`

| 基准 | 前（中位） | 后（中位） | 说明 |
|------|-----------|-----------|------|
| `BenchmarkHash`（帧级） | 114.9 ns/op，0 B/op，0 allocs/op | 108.2 ns/op，0 B/op，0 allocs/op | 未改动，持平 |
| `BenchmarkSnapshotHash`（`Snapshot()` 全路径） | 324.1 ns/op，216 B/op，**7 allocs/op** | 425.9 ns/op，216 B/op，**7 allocs/op** | 见下 |

**为什么 allocs 不是 7 → 0**：`Snapshot()` 里 7 次分配**全部来自 `json.Marshal(State)`**
（`State` 为 map 结构，反射 + 排序 + 缓冲区）；`fnv.New64a` 实测 0 allocs，统一切到 `Hash()` 后仍是 0 allocs
（隔离测量：json.Marshal 408 ns/11 allocs，fnv 16.7 ns/0 allocs）。
`Snapshot.State` 是 `Restore`/结算/迁移所需的**可还原字节**，删不得——若连它一起改成定向编码，
才能把分配降到 1 次（返回的 `[]byte` 本身），与「让 `Snapshot.Hash` 取 `Hash()`」的最小改动无关，故本次不做。
ns/op 上升约 25% 来自「用 `Hash()`（~108 ns，需遍历 map 取键排序）替代 fnv over 88 B JSON（~17 ns）」，
且 `SnapshotEvery` 才触发一次，不在每帧热路径；帧级路径（每帧）未变。

## 5. 影响面与风险

- 客户端：帧级/快照级 `state_hash` 从此同值，跨帧/跨快照比对不再误报失步；**字段类型/长度不变（8 字节）**，协议零改动。
- `DesyncHook.Check` 现在收到的是帧级同口径值。
- 已落库/已在内存的旧快照：`State`（JSON）格式未变，`Restore` 不受影响；其 `Hash` 字段是旧口径数值，
  读到后不参与任何"与帧级比对"的判定（`DesyncHook` 只在产生快照时调用），故无兼容问题；
  回放记录里若存了旧口径 hash，重放核对会以新口径重算，属预期变更。
- 迁移（`BattleMigrationState.SnapshotHash`）传 uint64，跨节点同版本一致；混版本集群的该值不参与校验。

## 6. 明确未做

- 未改 proto 字段、未改 TS/C# 产物、未动 UDP/delta 下行编码。
- 未改 `Snapshot.State` 的编码（仍 JSON），故快照全路径仍是 7 allocs/op（见 §4）。
- 未修 `Battle.Restore` 不恢复 `frame` 的既有小缺口（与哈希无关，另开）。
