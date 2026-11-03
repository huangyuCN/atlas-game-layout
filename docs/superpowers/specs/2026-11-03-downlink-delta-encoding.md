# 下行帧广播的增量编码设计（ver=3）

- 状态：**设计稿（只设计、未实现）**
- 日期：2026-11-03
- 范围：battle 直连帧面的**帧广播下行**（`battle.v1.FrameBroadcast`）载荷编码；服务端 + Go/TS/C# 三端 SDK + 协议
- 硬约束：本文件不含任何代码改动、不跑压测、不提交；所有数字除注明「实测」外均为**预算/推算**，落地前必须复核

---

## 0. 一页结论：这项到底省什么、不省什么

**先说清楚，避免又一次「以为能省 CPU 结果没有」。**

| 项目 | 结论 | 依据 |
|---|---|---|
| 下行**字节** | ✅ 省，量级 90%±（见 §3.4 字节账） | 每 tick 全量广播里 8–10 条输入与该帧基线**逐字节相同**（无新输入者重复上一次输入，见 §1） |
| **序列化 CPU** | ⚠️ 省一点，但**有硬上限**，不是主要收益 | 「每帧只编码一次、载荷复用给全部目标」**已经落地**；增量只能把这一次编码本身做小（基准 1.7 µs/op），不可能再省一个量级 |
| **包数 / 帧数** | ❌ **完全不变** | 仍是每连接每 tick 一帧、一次写。这是本项**明确不承诺**的收益 |
| syscall 次数 | ❌ 不变 | 同上。写是必需的系统调用，和 payload 大小无关 |
| tick 频率 / 时延 | ❌ 不变 | 编码路径不引入额外等待；关键帧请求是异步的、不阻塞 tick |
| 链路层 MTU/分片 | 🔸 可能有次要副产物 | 包体从 465 B 级降到几十 B 后，若此前单帧跨 KCP MSS，分段数可能减少——属链路层副产物，**不作收益依据、不做保证** |

**一句话**：本项是**带宽与「每次编码的字节数」优化**，不是包数优化。任何以「减少 pps / 减少 syscall」为由的立项理由都不成立。

**实测基线（来自 `docs/config.md` §7.10，本机 10 目标基准）**：

| 编码 | 帧头 ver | 同一广播体积 | 每帧编码开销 |
|---|---|---|---|
| protojson（缺省） | 1 | 1631 B | ≈12 µs/op、10520 B/op、163 allocs/op |
| protobuf 二进制 | 2 | 465 B（≈1/3.5） | ≈1.7 µs/op、784 B/op、12 allocs/op |

- 生产口径的链路占用：ws 面 41k pps × 1546 B ≈ **603 Mbps**（ver=1）；切 ver=2 后 ≈172 Mbps。
- 本设计（ver=3）在 ver=2 基础上再降到**约 1/10**（§3.4），即 ≈20 Mbps 量级。

---

## 1. 为什么「有东西可省」：现状链路的关键事实

已核对代码，增量成立的前提全部具备：

1. **每 tick 广播的是「全员输入」，不是「本帧新输入」**。`atlas/contrib/lockstep/session_handler.go` 的 `aggregateInputs()` 对 `h.players` 里**每个玩家**都产出一条 `LockstepInput`：本帧有新输入就用新的；没有就**复用 `lastInput` 并把 `Frame` 改写成当前帧**；从未输入过则发一条空 payload 的占位。
   ⇒ 一个 10 人局，**每帧都有 10 条输入**，其中通常 8–10 条与上一帧**逐字节相同**（仅 `frame_id` 被改写）。这才是「10 人局通常只有 0–2 个玩家有新输入 ⇒ 能省 90%+」的真正来源。
2. **每 tick 每连接一帧**。`stream.Registry.PublishFrames` 每帧只编码一次（`r.encode`），产物复用于该帧全部目标；每个目标各写一帧（`pushPayload` → `Port.PushRaw`）。⇒ 增量**不能**把「一次编码 × N 目标」改成「每目标一次编码」，那会把已经修好的 CPU 热点重新引回来（§0 第 2 行）。
3. **帧头已有 `ver` 位**（16 B 头：magic/ver/type/flags/seq/bodyLen），且 `battle.frame_broadcast_protobuf` 已验证「ver 与载荷编码自洽、可并存、可回退」这条路。ver=3 沿用同一机制。
4. **下行有三面**：ws（TCP 承载）、kcp（可靠 UDP）、udp（裸数据报）。可靠性差异决定了恢复机制的分档（§5）。
5. 客户端把载荷当不透明字节：SDK 只把 `(payload, ver)` 交给上层，**补帧基准只认 ver=1**（Go 直连 SDK 写死 protojson）。⇒ 任何新版本都必须**先协商、后下发**，否则未升级客户端会静默丢弃广播。

---

## 2. 目标与非目标

**目标**

- G1 新增载荷版本 **ver=3（增量）**，与 ver=1/ver=2 **并存**，按**协商**选择，未协商到的连接行为零变化。
- G2 在**丢帧可感知、可恢复**的前提下拿到 90% 量级的字节下降（§3.4）。
- G3 恢复机制对可靠面（ws/kcp）与不可靠面（udp）给出**明确结论**，不留「看起来能用」的灰区。
- G4 三端 SDK 的改动面切清楚，能分工、能估人日（§6）。
- G5 不变量可测：**增量合并结果 == 同帧全量编码**（属性测试，§7）。

**非目标（明确不做）**

- N1 不减少包数、不改 tick 频率、不改帧头结构、不引入新的可靠性层。
- N2 不做 per-connection 差分（见 §3.5 的取舍）。
- N3 不做通用压缩（gzip/zstd/字典编码）：帧太小，压缩头与 CPU 不划算；省字节靠结构化差分。
- N4 不在 udp 面启用增量（§5 结论）。
- N5 不做「跨多 tick 合并成一个包」的批量化（会改变包数与节奏，属另一项）。
- N6 不改 `LockstepFrame` 语义、不改模拟器输入契约；客户端拿到合并后的结果必须与今天逐字段等价。

---

## 3. 编码格式（ver=3）

### 3.1 版本与协商

**版本位语义**（帧头 `ver`，1 字节，已有）：

| ver | 载荷 | 形态 | 状态 |
|---|---|---|---|
| 1 | `FrameBroadcast` | protojson | 永续支持（排障/兜底） |
| 2 | `FrameBroadcast` | protobuf 二进制（**全量**） | 已实现，本设计把它同时当作 ver=3 的**关键帧**载体 |
| 3 | `FrameBroadcastDelta` | protobuf 二进制（**增量**） | 本设计新增 |

**协商（三步，全部显式，不猜）**

1. **客户端声明**：`JoinBattleReq` 新增 `repeated FramePayloadVersion accept_payload_versions`（客户端**能解**的全部版本，如 `[1,2,3]`）；缺省空 = `[1]`（老客户端行为零变化）。
2. **服务端选择**：`min(服务端配置允许, 客户端声明)`，取最高者；空交集回落 ver=1（有输出版本可解，不会静默错解）。`JoinBattleReply` 新增 `FramePayloadVersion payload_version`，回执里明确告诉客户端本连接生效版本。
3. **服务端节点级开关**：`battle.frame_broadcast_delta`（默认 `false`）。开启前提 `battle.frame_broadcast_protobuf=true`（关键帧就是 ver=2 全量帧），装配期校验，不自洽即启动失败（与 `NewRegistryWithEncoding` 现有校验同一风格）。

**逐连接按键**：协商结果是**每连接**的（同一局里老客户端 + 新客户端可能共存）。服务端注册表已按 `player_id → Conn{Kind, ConnID}` 存连接，新增每连接 `lastSeq` + `payloadVer` 两个字段即可，不需要新表。

### 3.2 字段级定义

```proto
// FramePayloadVersion 是帧广播载荷版本，与帧头 ver 位一一对应（不得另立字面量）。
enum FramePayloadVersion {
  FRAME_PAYLOAD_VERSION_UNSPECIFIED = 0; // 不得下发
  FRAME_PAYLOAD_VERSION_PROTOJSON   = 1; // ver=1 全量 protojson
  FRAME_PAYLOAD_VERSION_PROTOBUF    = 2; // ver=2 全量 protobuf
  FRAME_PAYLOAD_VERSION_DELTA       = 3; // ver=3 增量（基线见 base_seq）
}

// FrameDeltaField 是「本条增量携带了哪些字段」的自描述列表（枚举优先，不用魔法位图）。
enum FrameDeltaField {
  FRAME_DELTA_FIELD_UNSPECIFIED = 0; // 不得下发
  FRAME_DELTA_FIELD_SERVER_TIME = 1; // server_time_unix_nano 有效
  FRAME_DELTA_FIELD_INPUTS      = 2; // set_inputs / removed_player_ids 有效
  FRAME_DELTA_FIELD_SNAPSHOT    = 3; // snapshot 有效（缺失 = 本帧无快照）
}

message FrameBroadcastDelta {
  uint64 base_seq = 1;   // 基线帧号：**服务端声明**「我假设你已经拥有这一帧的完整状态」
  uint64 seq      = 2;   // 本帧帧号（须 = base_seq + 1）
  repeated FrameDeltaField changed = 3; // 本帧相对基线变化的字段（可空 = 只有帧号推进）
  int64  server_time_unix_nano = 4;     // changed 含 SERVER_TIME 时有效（绝对 UnixNano）
  repeated atlas.game.lockstep.LockstepInput set_inputs = 5; // 变化/新增的输入，按 player_id 整条替换
  repeated string removed_player_ids = 6;                    // 本帧不再出现的玩家
  atlas.game.lockstep.SnapshotMeta snapshot = 7;             // changed 含 SNAPSHOT 时有效
}
```

**字段语义（合并规则，逐条）**

| 字段 | 未出现在 `changed` 中 | 出现在 `changed` 中 |
|---|---|---|
| `server_time` | 沿用基线的 `server_time` | 用本条的绝对纳秒**替换**（不累加，避免漂移） |
| `inputs` | 整体沿用基线的 10 条输入 | **按 player_id 的稀疏替换**：先 `removed_player_ids` 删除，再 `set_inputs` 逐条按 `player_id` 覆盖/追加，未提及的保持基线 |
| `snapshot` | 沿用基线的 `snapshot`（通常两帧都为空） | 用本条替换；**本条缺失 = 本帧无快照**（这是「清空」的表达方式） |
| `battle_id` | **增量永不携带** | —（由关键帧/会话绑定提供，客户端在关键帧处记住） |
| `frame_id` | `= seq`（隐含，不单独传） | — |

**为什么 `changed` 用 `repeated enum` 而不是位图或 proto3 字段存在性**

- proto3 的 `repeated` **没有存在性**，无法区分「本帧 inputs 为空」与「本帧没提 inputs」，而这两者语义不同（前者清空、后者沿用）⇒ 必须有一个显式的「哪些字段跟着来」的声明。
- 位图（`uint32 mask`）能表达，但引入魔法位常量，违反本仓「枚举优先、杜绝魔法值」约定；`repeated enum` 每项 1 B，典型 2 项，成本可忽略且自解释、可扩展。
- 消息字段本身（`snapshot`）保留 presence，用于表达「有/清空」；`set_inputs` 的稀疏性由 `player_id` 键控，不需要额外元数据。

**`base_seq` 不是 `seq` 的冗余**：把「服务端以为客户端拥有哪一帧」写成线上字段，接收方才能比较「我的现实」与「你的假设」。只靠 `seq` 连续性检查能发现「我漏收了一帧」，但发现不了「服务端给这个连接发过关键帧、而我没收到/没采用」——后者只有显式基线能暴露（§4.3）。

### 3.3 变更项的表示（示例）

假设 10 人局，本帧只有玩家 `p-7` 有新输入、`p-9` 掉线出局、快照未到：

```text
base_seq=1023 seq=1024 changed=[SERVER_TIME, INPUTS]
server_time_unix_nano=1762...500
set_inputs=[{frame_id:1024, player_id:"p-7", payload:<32B>}]
removed_player_ids=["p-9"]
```

客户端合并：把基线里 `p-7` 的输入整条替换、删掉 `p-9`、其余 8 条原样保留，`frame_id` 全部改写为 1024（与今天全量帧的语义一致），`server_time` 替换，得到与全量帧**逐字段等价**的 `FrameBroadcast`。

### 3.4 字节账（预算，非实测）

口径：一帧 10 条输入、payload 32 B、`player_id` 按 24 B 估（UUID 形态 36 B 另注）；`server_time` 绝对纳秒 varint ≈10 B；哈希/快照每 `SnapshotEvery` 帧一次（下面按 0 计）。

| 场景 | ver=2 全量 | ver=3 增量 | 比例 |
|---|---|---|---|
| 0 条新输入（多数 tick） | 465 B | ≈18 B | **3.9%** |
| 1 条新输入 | ≈530 B | ≈81 B | 15% |
| 2 条新输入 | ≈595 B | ≈144 B | 24% |
| 关键帧（每 300 帧一次） | 465 B | 465 B（就是 ver=2 帧） | — |

加权（假设 60% tick 无新输入、30% 一条、10% 两条，10 人局 2 人活跃）：
`0.6×18 + 0.3×81 + 0.1×144 + 465/300 ≈ 51 B` ⇒ **约为 ver=2 的 8%**（相对 ver=1 的 1631 B 约 3%）。
若**每 tick 都固定有 2 人输入**（最坏稳定态），比例升到 ≈24%（仍省 76%）；若 `player_id` 是 36 B UUID，每条输入多 24 B，比例上浮约 5–8 个百分点。

> 历史实测「delta 省 96%」的口径与本表一致（相对 protojson、多数 tick 无新输入）。**不要**把它当成「相对 ver=2 也能省 96%」——ver=2 已经把 JSON 的字段名开销吃掉了一大半。

### 3.5 关键帧 = 现有 ver=2 全量帧（不新增第二种全量格式）

**决定**：恢复用的「全量」直接复用 `FrameBroadcast` + protobuf（ver=2），**不在 ver=3 里再造一个 `kind=KEYFRAME` 变体**。

- 好处 1：全量格式与编码路径**已经实现、已测**（含 wire 断言），新代码面小。
- 好处 2：udp 面本来就要发 ver=2，客户端**不需要为 udp 面改任何解码代码**（§5）。
- 好处 3：客户端只需维护一条规则——「收到 ver=2 就把基线重置到该帧的 `frame_id`，收到 ver=3 就按 `base_seq` 校验」，状态机极小。
- 代价：关键帧不能被 ver=3 客户端「顺带」用来表达 delta 之外的语义（如名单全量重建），但名单变化已经用 `removed_player_ids` 覆盖，不需要。

**服务端不做 per-connection 差分**：全局只维护**一条链**（`prev` = 上一 tick 的完整帧）。给某连接补关键帧时，只是把「上一 tick 的全量帧」单独发给它，其他连接照发 delta。两条载荷（全量、增量）每 tick 各**至多编码一次**，且全量只在「有连接需要」时才编码。

---

## 4. 恢复机制（核心风险：丢一个增量就整段错）

### 4.1 失效模型

增量是**有状态编码**：客户端必须拥有 `base_seq` 那一帧的完整状态，否则合并结果错，且**错误会沿链传播**（后面所有 delta 都基于错误基线）。触发源：

1. 传输丢包（udp 面必然发生；ws/kcp 不丢，但连接重建会丢「链」）；
2. 客户端晚加入 / 重连 / 节点迁移后没有基线；
3. 服务端某次写失败（连接抖动）而后恢复；
4. 名单变化导致基线里的输入集合与本帧不一致（本设计用 `removed_player_ids` 消除了这一类）；
5. 客户端实现 bug（收到但没应用）。

### 4.2 三个候选的对比

| 候选 | 覆盖的失效 | 成本 | 结论 |
|---|---|---|---|
| ① 周期性关键帧（每 N tick 全量） | 全部（无需上行） | 每 N 帧多一份全量；恢复时延上界 = N tick | **采用，做兜底**（N=300，10 Hz 下 30 s，字节开销 ≈0.3%） |
| ② 接收方发现序号跳变就请求全量 | 1/2/3/5 | 需一条上行控制 op + 服务端限流；恢复多 1 个 RTT | **采用，做主路径**（可靠面上请求不会丢） |
| ③ 每条增量自带基线序号，不匹配即请求全量 | 2/3/5（且能发现「假设不符」） | 每帧 +2–3 B | **采用，作为判定依据**（①②都要靠它触发） |

三者**不是三选一，是三层**：③ 是判定，② 是快速修复，① 是对「请求也失效」的兜底。

### 4.3 推荐方案：`base_seq` 自校验 + 按需关键帧 + 周期兜底 + 服务端逐连接基线

**客户端状态机（每个战斗会话一份，只保留一帧状态）**

```text
state: full（当前完整帧）, lastSeq（= full.frame_id）, pendingReq（是否已请求、去重）

收到 ver=1/2 全量:  full = decode(bytes); lastSeq = full.frame_id; 交付上层; pendingReq = false
收到 ver=3 增量:
    d = decodeDelta(bytes)
    if d.seq <= lastSeq:            return            // 迟到/重复，静默丢弃
    if d.seq != lastSeq+1 || d.base_seq != lastSeq:
        if !pendingReq: 发 RequestFrameKeyframe(last_seq); pendingReq = true
        return                                          // 丢弃，不猜、不缓冲、不乱序应用
    full = merge(full, d); lastSeq = d.seq; 交付上层
```

要点：**只信 `base_seq`，不做「尽力而为」的容错**。缓冲后续 delta 等关键帧、或按 `seq` 猜基线，都会把「一个可恢复的失步」变成「一段静默花屏」，比丢几帧更糟。

**服务端状态机（注册表内，每连接两字段）**

```text
每 tick（seq = N）:
    delta  = 若启用且 prev != nil：encodeDelta(prev, cur)          // 至多一次
    需要全量的连接集合 = { c | c.payloadVer < 3               // 未协商到增量
                           或 c.lastSeq != N-1               // 新连接/上帧没发成
                           或 c.keyframeRequested           // 客户端请求
                           或 N % keyframeEvery == 0 }      // 周期兜底
    full   = 若有上述任一连接：encodeFull(cur)                     // 至多一次
    逐连接发送（delta 或 full），并置 c.lastSeq = N、c.keyframeRequested = false
```

- **单一全局链**保证「编码次数 ≤ 2/tick」，不会回到「每目标一次序列化」的老路。
- `c.lastSeq` 是服务端**自己的发送记录**，用于避免「给一个没有基线的连接发 delta」——这条检查单独就能挡掉「新连接首帧收到增量」这类必错场景。
- 关键帧请求是**控制 op**：`RequestFrameKeyframe(battle_id, last_seq)`，`access=CLIENT`、幂等、无回执（Tell）。服务端**每连接 ≥200 ms 一次**限流，超频直接丢弃并计数（不回错，避免客户端放大）。

### 4.4 边界与取舍

| 场景 | 处理 |
|---|---|
| 新加入 / 重连 / 迁移 | 连接登记时 `lastSeq = 0` ⇒ 首帧必全量；客户端从 `JoinBattleReply.current_frame` 起对齐，之后接 delta |
| 客户端请求但关键帧在途 | `pendingReq` 去重；服务端下一 tick 必定给全量（`keyframeRequested` 标志） |
| 请求也丢了（udp/极端情况） | 周期关键帧兜底，最坏恢复时延 = `keyframeEvery` tick |
| 名单变化（出局） | `removed_player_ids` 显式删除，**不强制关键帧**（保持链连续；出局本就罕见） |
| `seq` 回退（服务端重启/回滚） | 客户端 `d.seq <= lastSeq` 丢弃并请求全量；服务端新链首帧必全量 |
| 客户端声明了 3 但服务端关闭增量 | 回执 `payload_version=2/1`，客户端按回执走；**永远以回执为准，不以自己声明的为准** |
| 关键帧风暴 | 服务端限流 + 客户端去重 + 周期兜底；指标 `frame_keyframe_request_total` 超阈值告警 |
| 内存 | 服务端每局只多留一帧（`prev`）；客户端每连接一帧。10 人局 ≈1 KB 级，可忽略 |

**为什么不选「每 N 帧全量」单方案**：N 小则省得少，N 大则弱网恢复慢（UDP 上一个丢包要等 30 s）。**为什么不选「只靠请求」**：请求路径本身可能失效（客户端 bug、op 被限流、连接半死），必须有不需要上行的兜底。

---

## 5. 传输面：哪一面允许增量（结论）

| 面 | 可靠性 | 允许 ver=3 | 理由 |
|---|---|---|---|
| ws（TCP） | 有序可靠，ARQ 由 TCP 补 | ✅ | 丢包不落在应用层；只有连接重建才需要关键帧，`lastSeq=0` 已覆盖 |
| kcp（可靠 UDP） | 有序可靠，ARQ 由 KCP 补 | ✅ | 同上；KCP 重传对应用层透明 |
| **udp（裸数据报）** | **不可靠、可丢可乱序** | ❌ **禁用** | 见下 |

**UDP 面禁用增量（明确结论）**，三条理由：

1. **服务端无法知道「客户端有没有基线」**：裸 UDP 没有应用层 ACK，`lastSeq` 只是「我发过」，不是「他收到」。一旦丢一帧，服务端会继续发 delta，客户端只能全部丢弃，直到请求抵达——**恢复时延 ≥ 1 RTT + 1 tick，期间画面停更**，比丢一帧更难看。
2. **恢复会吃掉收益**：数据报面本来就是高丢包场景，若每次丢包都补关键帧，平均值反而接近全量；字节账（§3.4）在丢包率 >2% 时迅速退化。
3. **要求「每包可独立解码」是该面的契约**：udp 面现有语义就是「单包自包含」，破坏它会让 SDK、压测脚本、排障工具全部要区分「这包能不能单独用」。

**落地方式**：不引入「UDP 全局关闭」的开关，而是**按连接 `Kind` 自动降级**——`PublishFrames` 里 `c.Kind == udp` 的连接永远发 ver=2 全量（客户端一行代码都不用改），其余连接按协商发 ver=3。同一局混布多面时，每 tick 至多多编码一份全量（已有连接需要时才会编）。

> 若将来确实想在 udp 上省字节，正确顺序是：先给数据报面加**应用层回执/关键帧请求**（可靠性下沉到应用层），再谈增量。列为未决项（§11），本期不做。

---

## 6. 三端 SDK 的改动面与工作量

**职责切分（关键，决定工作量）**

- **协议层（三端各一份，手写合并器）**：`FrameBroadcastDelta` 的 `merge(base, delta) → FrameBroadcast`。纯函数、无 IO、可属性测试。三端各 ≈120–180 行。
- **SDK 会话层**：每战斗会话的 `lastSeq` / `pendingReq` / 关键帧请求发送（新 op 的 stub 由代码生成器出，`access=CLIENT` 自动进三端 opclient）。三端各 ≈60–120 行。
- **SDK 不碰**：`FrameBroadcast` 的字段解码仍归游戏侧协议包；`ver=3` 只多一条「先合并、再交给上层」的分支。

| 工作项 | 内容 | 人日 |
|---|---|---|
| 协议与格式 | proto（`FrameBroadcastDelta`、两个枚举、`JoinBattleReq/Reply` 扩字段、`RequestFrameKeyframe`）+ 三端生成 + 文档 | 1.0 |
| 服务端 | 增量编码器（diff/基线/关键帧调度）、逐连接 `lastSeq/payloadVer`、按 `Kind` 降级、开关与协商、装配校验、指标 | 2.0 |
| Go SDK | 合并器 + 会话状态机 + 关键帧请求 + ver=2 基线重置（含现有「补帧基准只认 ver=1」的兼容处理） | 0.75 |
| TS SDK | 同上 | 0.75 |
| C# SDK | 同上 | 0.75 |
| 测试 | 属性测试 ×3 端、失步/跳变/重复、跨版本矩阵、wire 断言、模糊、基准 | 1.5 |
| 联调与灰度 | 三端联调、开关灰度、配置文档、压测复核 | 1.25 |
| **合计** | | **≈8 人日（7.5–9.5）** |

说明：服务端占比最高，因为「按连接基线 + 关键帧调度 + 协商」是唯一有状态、且与现有 `PublishFrames` 一次编码/多目标复用逻辑强耦合的部分；三端 SDK 相对轻，因为合并规则简单、op stub 自动生成。**三端可以并行**（协议冻结后互不阻塞），关键路径是「协议 → 服务端 → 任一端联调」。

---

## 7. 不变量与测试

**不变量（每条都要有对应测试，缺一不算完成）**

- **I1 增量合并 == 全量编码**：对同一 tick，`merge(基线, delta)` 与 `encodeFull(cur)` 解码结果**逐字段相等**（inputs 按 `player_id` 归一，`server_time` 精确到纳秒，`snapshot` 有/无都要覆盖）。
- **I2 链式等价**：从关键帧 K 起连续应用 N 个 delta，结果 == 该 tick 的全量帧（对随机 10–1000 步都成立）。
- **I3 序号跳变必请求全量**：注入「丢一帧 / 乱序 / 重复 / `base_seq` 不符 / `seq` 回退」五类事件，客户端必须**一次**请求关键帧、且**不**交付任何错误状态。
- **I4 跨版本矩阵**：`{客户端声明} × {服务端开关} × {连接 Kind}` 的所有组合，下发的 `ver` 与载荷必须自洽；**未声明 3 的连接永不见 ver=3**；udp 永不见 ver=3。
- **I5 包数不变**：同一场景 ver=2 轮与 ver=3 轮的帧数/pps 相等（±0.1%）。这条是**验收断言**，不是收益指标。
- **I6 良性输入**：任意随机字节（含截断、超大、错误 `changed`、`base_seq=0`）不得 panic、不得越界，只能报错或安全丢弃。

**测试设计**

- **属性测试**（三端各一份，同一组种子）：随机生成输入序列（玩家集合增删、payload 变化/不变、快照出现/消失、`server_time` 跳变），断言 I1/I2。生成器必须覆盖「空 inputs」「只有 removed」「`changed` 为空」「关键帧紧跟 delta」四种边界。
- **失步测试**：I3 的五类事件，含「连续两次请求」的去重、限流窗口内不重发。
- **跨版本/装配测试**：I4 用真帧引擎走 wire，断言帧头 `ver`；服务端装配期非法组合（`delta=true` 且 `protobuf=false`）必须启动失败。
- **基准**：`BenchmarkPublishFrameBroadcast10{JSON,Protobuf,Delta}`，`-benchmem`，报告 payload 字节、ns/op、allocs/op；同时给 `-cpuprofile` 看 `proto.Marshal` 占比变化。
- **回归**：ver=1/ver=2 的现有 golden 与 wire 测试**一条都不许改**（新版本只加不改）。

---

## 8. 收益量化方法（怎么证明「省了多少」）

**只报两个数**，其余都是噪声：

1. **每 tick 每连接下行载荷字节**（TX 字节）：`edge_bytes_total` 差值 ÷（连接数 × tick 数），并附 payload 字节直方图。
2. **编码开销**：`BenchmarkPublishFrameBroadcast10` 的 ns/op / B/op / allocs/op（profile 里看 `proto.Marshal` 占比）。

**三轮对照**（同机、同场景、同连接数、同 tick 数，各 60 s）：ver=1 / ver=2 / ver=3，在 4096 连接 × 10 Hz 下跑。

**预期（落地后按此验收，不做承诺的部分已标 ❌）**

| 指标 | ver=1 | ver=2 | ver=3（预期） |
|---|---|---|---|
| 每 tick 下行字节（10 人局，§3.4 口径） | 1631 B | 465 B | ≈51 B |
| 链路占用（41k pps） | 603 Mbps | ≈172 Mbps | ≈20 Mbps |
| 帧数 / pps | 41k | 41k（❌ 不变） | 41k（❌ 不变） |
| 编码 ns/op（10 目标基准） | ≈12 µs | ≈1.7 µs | 待测（**增量 diff 会吃掉一部分收益，可能落在 1.0–1.5 µs**） |
| `broadcast_downlink_ms` p99 | 基线 | 略降 | 与 ver=2 同量级（codec 不是瓶颈） |

**必须显式记录的两句话**

- **包数不会有变化**：每连接每 tick 仍是一帧一次写；pps 与 syscall 数不因本项下降。任何「降 pps」的结论都是测错或误读。
- **序列化 CPU 的上限很低**：编码已经是一 tick 一次（每帧只编码一次早已落地），ver=3 只能把这一次的字节数做小；diff 本身还有成本。**本项的主要收益是带宽，不是 CPU**。

**复核方式**：压测在 `10.10.9.36` 执行；每轮记录 commit、`go version`、GOMAXPROCS、完整命令；结果按 `docs/superpowers/benchmarks/` 约定归档；对比用 `benchstat`。

---

## 9. 风险清单

| # | 风险 | 影响 | 概率 | 缓解 | 触发信号 |
|---|---|---|---|---|---|
| R1 | 客户端基线错（版本/回执没对齐） | 静默丢帧、画面停更 | 中 | 只用回执选定版本；`base_seq` 严格相等；未声明 3 永不发 3 | `frame_keyframe_request_total` 异常升高 |
| R2 | 增量链断裂（弱网/重连/迁移） | 短暂停更后恢复 | 中 | 请求关键帧 + 周期兜底 + 服务端 `lastSeq` 检查 | 同上 + `broadcast_downlink_ms` p99 抬升 |
| R3 | 服务端 diff 成本高于预期 | 编码 CPU 反升 | 中 | 只对 ≤10 条输入做逐条比较（O(players)），不排序不拷贝；基准里单列 diff 耗时 | profile 里 diff 占比 >30% |
| R4 | 基线帧内存被改写 | 花屏/数据错乱（最难查） | 低 | 基线只读：`prev` 持有当帧完整帧；payload 每帧由 `aggregateInputs` 复制（现状已如此），实现时**禁止复用缓冲** | 属性测试 I1/I2 随机失败 |
| R5 | 名单增删语义遗漏 | 客户端多/少一个玩家 | 中 | `removed_player_ids` 显式删除 + I1 覆盖增删场景 | 客户端玩家数与服务端不符 |
| R6 | 关键帧请求被滥用 | 上行放大、CPU 抖动 | 低 | 每连接 ≥200 ms 限流、幂等、无回执、客户端去重 | 请求速率 > 连接数/秒 |
| R7 | 滚动升级期混布 | 部分客户端解错 | 中 | 协商 + 回执 + 默认关闭；先升 SDK、再开开关 | 灰度批次里 ver=3 占比与预期不符 |
| R8 | 「省了字节」被误报成「省了 CPU/包数」 | 立项预期失真 | **高** | 本文 §0/§8 的固定口径；对外汇报只引这两个数 | 汇报里出现 pps 下降 |

---

## 10. 未决问题（实现前需要拍板）

1. **`player_id` 槽位字典**：把每条输入的 `player_id`（24–36 B）换成 1 B 槽位（需在关键帧带 roster + epoch），单帧还能再省 40–80 B。收益真实但引入「名单版本」这一新失效面。建议 **P2**，本期不做（先把链跑稳）。
2. **广播前按 `player_id` 排序**：现状 `for player := range h.players` 顺序随机，导致全量帧的 inputs 顺序每帧不同；排序后可做**逐字节**等价断言（否则只能按集合比对）。改动小、三端无感，建议随本项一起做，但需单独评审（它会改变 ver=1/2 的线上字节序）。
3. **`server_time` 用绝对纳秒还是相对基线的增量**：绝对值简单、可精确重建；增量省约 4 B。当前选绝对值，若字节账吃紧再改。
4. **关键帧周期 `keyframeEvery` 的默认值**：建议 300（30 s）；需结合「客户端请求路径可靠性」与「单局 6000 帧」实测后定。
5. **udp 面未来是否值得**：需先给数据报面加应用层回执/关键帧请求；本期明确不做。

---

## 11. 参考

- 帧广播现状与编码基线：`docs/config.md` §7.10（protojson 1631 B / protobuf 465 B）
- 广播路径：`services/battle/internal/actor/battle_frame.go`（`onFrameResult`）、`services/battle/internal/stream/registry.go`（`PublishFrames`）
- 全员输入聚合（增量成立的前提）：`atlas/contrib/lockstep/session_handler.go`（`aggregateInputs`）
- 帧协议与版本位：`atlas/transport/frame/frame.go`（`Header`、`Version`、`Version2`）
- 协议定义：`api/battle/v1/battle_service.proto`（`FrameBroadcast`、`JoinBattleReq`）、`atlas/api/lockstep/lockstep.proto`（`LockstepFrame`）
- 传输与协商现状：`services/battle/internal/app/graph.go`（`newStreamRegistry`）

---

## 附：设计进展记录（每步一行）

- 15:46 定位模板仓 `atlas-game-layout` 与 `atlas/transport/frame`，确认帧头 16 B、`ver` 位已存在（ver=1/2 并存）。
- 15:49 读通广播链路：`PublishFrames` 每帧一次编码、逐目标各写一帧（包数不动的前提确认）。
- 15:53 关键发现：`aggregateInputs` 每帧下发**全员**输入（无输入者重复上次输入）⇒ 跨 tick 有大量逐字节冗余，增量收益成立。
- 15:57 取到实测基线：protojson 1631 B / protobuf 465 B（§7.10），据此做字节账与链路占用推算。
- 16:00 确认三面可靠性差异与 udp 数据报面语义，定「按连接 Kind 自动降级」。
- 16:03 对比三种恢复候选，定为三层组合（base_seq 判定 + 请求 + 周期兜底）。
- 16:07 确认三端 SDK 现状（载荷不透明、补帧基准只认 ver=1）⇒ 协商必须先于下发。
- 16:12 落笔：字段级格式、恢复状态机、不变量、人日与风险清单。
- 16:20 自检：收益口径（带宽/序列化 vs 包数）写在 §0，包数不变在 §8 复述；无代码改动、未提交。
