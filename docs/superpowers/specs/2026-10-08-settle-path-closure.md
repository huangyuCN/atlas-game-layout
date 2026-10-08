# 结算路径收口：已结束对局的迟到帧 op 与结算通知补投

日期：2026-10-08　范围：`services/battle/**`、`scripts/e2e/**`（模板仓 atlas-game-layout）

## 1. 现象与根因

阶段 3 验收实测：对局结算后客户端**继续发帧 op**（`SendFrameInput` / `Ping`），出现两类缺陷。

### A1 空名单实例被反复重建（主缺陷）

链路：`帧面 handler → IdentityResolver(帧槽验票) → LocalDeliverer → actor 投递`。

结算时 battle actor 自停（`ctx.Stop`），目录归属释放；但客户端仍带着**未过期的票**发帧：

1. 验票通过后**照常投递**；
2. `pkg/actor.Runtime.Tell/Ask` 走 `inner.Local()`，本地未命中即按 `SpawnAuto` **懒激活**重建
   battle actor（空名单：`players` 为空，没人重新 `Create/JoinBattle`）；
3. 重建实例的 lockstep 会话从存储恢复最新快照（`RestoreRequired`），模拟器状态里**胜者已定**，
   于是下一次帧广播立刻再次结算：`Save` 落库 + `PublishSettled` + `CloseBattle`（把客户端
   刚建立的直连又关掉）+ 自停；
4. 下一条迟到 op 重复 1–3。

实测证据（临时探针，绕帧面直接投递一条迟到 `SendFrameInput`）：

```
迟到 op 投递 err=<nil>；重建 alive=true stats={State:Running ...}
1.5s 后 alive=false；结算事件=0x…f2c0（前一次 0x…7b0）；落库 true
```

即：**一条迟到 op → 一次重建 → 一次额外结算 → 一次多余的关连接**；客户端发 N 条就是 N 轮
（验收现场 9–13 轮），客户端被反复丢弃，SDK 拿不到可判定的终止信号。

### A2 结算通知单次投递（次缺陷）

`settle` 只对「结算当刻仍登记在册」的连接投一次 `BattleEndNotify`：

- 未登记的连接直接跳过；
- 数据报面（KCP/UDP）没有重传，丢一个包就等于玩家**永远不知道结果**；
- 连接已失效（推送报 `ErrConnNotFound`）只做注销，不重发。

高丢包下表现为「双方都没收到结算通知」（`-mode damage -loss 20` 偶发失败）。

## 2. 设计

### 2.1 结束留档（墓碑）＝ 懒激活之前的判定依据

新增 `stream.EndedBook`（`services/battle/internal/stream/ended.go`）：结算时写一次
「墓碑 + 胜负 + 参战名单」，帧面在**投递之前**查阅它：

| 关注点 | 结论 |
|--------|------|
| 写在哪 | battle 节点本地的帧面直连注册表（`stream.Registry`）——帧面监听与 actor 属主同节点，无需跨节点同步 |
| 谁写 | battle actor 的 `settle()`（`biz.SettleLedger.RecordEnded`），**在首次推送与 `CloseBattle` 之前** |
| 谁读 | 帧面身份解析器 `ticketIdentity.Resolve`：验票通过后、投递之前；命中即**不登记、不投递**，直接以稳定 reason 拒绝 |
| 为什么能挡住重建 | 拒绝发生在 `LocalDeliverer` 之前，SpawnAuto 根本没有被触发的机会 |
| 幂等 | 同局重复留档以首次为准（结算路径可重入，不覆盖、不延长 TTL） |
| 清扫 | 写入路径与统计路径顺手清理过期条目（表不随对局数增长） |

拒绝的 reason 复用既有业务枚举 **`BattleEnded`（`api/error/v1` 3003，HTTP 409）**，与
`BattleTicketInvalid` / `BattleTicketExpired` 并列——三个 reason 语义互斥、SDK 可判定：
票据问题→重取票；对局已结束→**停止发送**（不重试到超时）。

### 2.2 TTL 取值依据

`EndedTTL = 票据有效期（ticket_ttl，缺省 120s）+ 掉线窗口（offline_timeout，缺省 15s）= 135s`
（`stream.EndedTTL`，装配期由既有配置派生，**不新增配置项**）。

依据：票据过期之后，不再有任何**合法**帧 op 能指向这一局（墓碑再长也无事可做）；
掉线窗口是「同一张票还能回座」的最后期限，留档必须把它整段覆盖。两者都在既有配置里，
随部署调整自动跟随（例如票 45s + 窗口 15s → 留档 60s）。

### 2.3 结算通知的三条投递路径（都幂等、都有界）

| 时机 | 位置 | 次数 |
|------|------|------|
| 结算当刻（首投） | `settle()` → `PublishEnd` | 每玩家 1 次 |
| `CloseBattle` 之前（未确认连接重投） | `stream.Registry.CloseBattle` → `repushEnd` | 每连接 `EndRetries`=2 次（合计 3 次） |
| 玩家迟到 op / 重连后（补投） | `Resolve` → `Registry.ReplayEnd` | 每玩家 `MaxEndReplays`=5 次封顶 |

- `CloseBattle` **每局只被调用一次**（结算是单次的），关闭因此确定性发生一次；迟到 op 走
  「补投 + 拒绝」路径，**不登记、不关闭**。
- 补投载荷与首投逐字一致（`BattleEndNotify{battle_id, winner_player_id}`），重复到达幂等。
- 队伍名单在留档里：名单外玩家既拿不到补投，也拿不到截断前的重建机会。

### 2.4 边界

- **进行中的对局零影响**：`Ended` 恒为假，验票 → 登记 → 投递与改动前逐字一致（有专项用例）。
- **迁移窗口**：留档只在 `settle()` 写；迁移窗口（`MigrationPause`）只暂停掉线计时与帧推进，
  与墓碑互不相交；已结算的局本就不参与迁移。
- **battle_id 复用**：本仓 battle_id 是 `idgen.Battle` 的 UUID（`battle-<32hex>`），实际不复用；
  墓碑按 battle_id 索引，TTL 过后条目消失，即便复用也不会误拒（有 TTL 过期用例）。
- 客户端在结算后继续发帧的成本：一次业务拒绝（无 actor 起停、无落库、无事件）。

## 3. 验收

- 组件（`-race` 绿）：`stream`（留档/补投上限/关闭前重投/只关一次/TTL）、`server`
  （已结束→`BATTLE_ENDED`+补投、不登记；进行中不变）、`actor`
  （结算写留档一次；帧面链路：迟到 op 被拒 + 不重建 + 只关一次 + 补投）。
  敏感性：把 `Ended` 判定短路成假，`TestFrameOpsLateOpsRejectedAfterSettle` 立即变红（迟到 op 被接受）。
- 服务器：`go test ./test/e2e/ -count=1 -v` → PASS=22 / SKIP=1 / FAIL=0；
  `-mode direct -transport kcp|udp|ws` 与 `-mode migrate` 各 exit 0；
  `-mode damage -loss 20` 连跑 10 次 **10/10 exit 0**（每轮双方都收到结算通知）；
  新增 `-mode lateop`（结算后继续发帧，kcp/udp/ws 各 exit 0）断言：迟到 op 稳定拒绝、
  战斗 actor 停止=1、结算事件=1、重连补投到位。
- 模板：`make lint` 四项 0、`go test ./... -count=1` 0 FAIL、`gofmt -l` 空；本批次未改 proto。

### 3.1 损伤形态装置的两处加固（与 A1/A2 语义无关，均为既有假失败源）

1. **入局重试**：`JoinBattle` 成功即算入局（补帧查询 `SyncFrames` 失败不判负），重试 6→10 次。
   此前一次丢包会把「已入局」误报成「入局失败」（`损伤下入局失败（重试 6 次）`）。
2. **全程保活**：客户端按 SDK 契约周期发 `Ping`（1s ≪ `offline_timeout/3`=5s）。
   此前等待结算期间静默 → 数据报面空闲驱逐 → 15s 后判负结算 → **胜者变成另一方**
   （`结束通知胜者 = "…", want "…"`），且必然触发「无出局通知」断言。

两处都会造成约 10% 的假失败；加固后结算断言反而**更严**（要求双方都拿到结果，不再允许单侧丢失）。

## 4. 证据摘要（复现 → 修复）

- 基线复现：把帧面的 `Ended` 判定短路成假，`-mode lateop` 第一条迟到 op 即被接受
  （`未被拒（战斗已结束仍被接受）`）；绕帧面直接投递的探针显示 `重建 alive=true`，
  1.5s 后再次结算并自停（新的结算事件 + 再次落库）。
- 修复后：同一形态 `连续 3 轮迟到 op 全被稳定拒绝（reason=BATTLE_ENDED）`、
  `战斗 actor 停止=1 结算事件=1`、`重连后补投到位（胜者 …）`。

