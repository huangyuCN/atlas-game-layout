# 灵活组队匹配设计（1-N 人 party + 等人数配对）

- 日期：2026-09-11
- 状态：方向已确认（灵活 1-N 人 / 引擎名册 + PlayerActor 引用 / 成员变更推送 / 引擎原子性本期修）
- 关联：`2026-09-11-matcher-entry-production.md`（匹配入口生产化的延续）

## 1. 语义定义（核心）

- **队伍（party）**：1..N 人的组队单元（N 为规则集容量上限，本期默认 5，可配置）。
  1 人队即 solo——单人票与整队票共用同一套匹配语义。
- **入队**：任意 1..N 人的队伍均可入队（不要求满员）；队长发起整队入队。
- **配对**：撮合函数把候选票（任意大小）组合成**总人数相等的两支队伍**，
  两侧平均等级差 ≤ MaxGap。party(2) 可以对 party(2)，也可以对「solo+solo」；
  party(k) 在凑不齐对面等人数时继续等待。
- **公平性**：party 成员必然同侧（整队票天然同队）；队伍人数相等保证对局公平。

## 2. 分层职责

| 层 | 职责 |
|---|------|
| atlas 引擎 | Party 名册权威（redis，TTL 1h）：Create/Join/Leave/EnqueueAsParty/**Describe**（新增）；**Join/Leave 原子化**（WATCH 事务重试，修并发丢更新）；**容量上限**（join 时原子校验）；**队长顺延**（队长离开交给首位成员） |
| matcher 服务 | party 生命周期 handler（proto rpc）；party→ticket 映射（整队票取消用）；成员变更事件发布（roster 快照）；casual 规则集升级为灵活配对（新 MatchFunction） |
| game PlayerActor | 匹配/组队域方法（新域文件）；在线态持 partyID（models.Player 持久化，重连恢复）；OnStop 联动 LeaveParty（若整队在匹配 → 取消票 → 失败事件 → 整队推送，复用现有 watchTicket 链路） |
| gateway | GatewayMatch 新增 party op + PartyRosterNotify 推送（名册快照，全队下发） |

## 3. 契约变更

### 3.1 matcher.v1（服务协议，调用方仍是 game PlayerActor）

```proto
// Party 生命周期（属性均为服务端权威，调用方为各成员自己的 PlayerActor）
service Matcher {
  // …既有单人匹配三方法…
  rpc CreateParty (CreatePartyRequest) returns (CreatePartyReply);      // 队长建队 → party_id
  rpc JoinParty (JoinPartyRequest) returns (JoinPartyReply);            // 按 party_id 加入（容量原子校验）
  rpc LeaveParty (LeavePartyRequest) returns (LeavePartyReply);         // 离开（队长顺延；空队删除）
  rpc DescribeParty (DescribePartyRequest) returns (PartyInfo);         // 名册快照（轮询兜底）
  rpc QueueParty (QueuePartyRequest) returns (QueuePartyReply);         // 队长发整队入队（1..N 人都可）
}
// PartyInfo { string party_id; string leader_id; repeated PartyMember members; }
// PartyMember { string player_id; int32 level; }
```

- 事件（nats，additive）：`PartyRosterEvent { party_id, leader_id, player_ids[], reason }`
  ——成员加入/离开/建队后发布，主题 `atlas.event.party.roster`；
- 整队成局/失败事件沿用现有 MatchStarted/FailedEvent（player_ids = 全队）。

### 3.2 game.v1 PlayerActor（匹配域扩展，新域文件 player_party.go）

```proto
rpc CreateParty (CreatePartyActorReq) returns (CreatePartyActorReply);      // party_id
rpc JoinParty (JoinPartyActorReq) returns (JoinPartyActorReply);            // { party_id }
rpc LeaveParty (LeavePartyActorReq) returns (LeavePartyActorReply);
rpc QueueParty (QueuePartyActorReq) returns (QueuePartyActorReply);         // { ruleset }（仅队长）
rpc GetParty (GetPartyActorReq) returns (PartyActorReply);                  // 名册快照
```

- 全部 Ask（业务错误需同步回传：PARTY_FULL/PARTY_NOT_FOUND/NOT_LEADER 等）；
- PlayerActor 在线态新增 `partyID`；`models.Player` 增 `party_id`（快照/落库持久，
  重连恢复后仍在队；roster 权威始终在引擎）；
- OnStop 联动顺序：CancelMatch（单人票）→ LeaveParty（matcher 调用，
  整队在匹配中则取消票 → 失败事件推送全队）→ 落库。

### 3.3 gateway.v1（客户端协议）

- `GatewayMatch` 新增同语义投影：CreateParty/JoinParty/LeaveParty/QueueParty/PartyStatus；
- 新推送 `PartyRosterNotify { party_id, leader_id, player_ids[], reason }`（protojson）。

## 4. 撮合函数（game-layout matchfunc）

`FlexTeams{ MaxGap, MaxSide }` 替换 casual 规则集的 LevelClose（1v1 是其 T=1 特例）：

- 确定性可重试（同输入同输出）：候选票按（等级，ticket ID）排序后，
  对侧人数 T 从 1 到 MaxSide 升序尝试——先用固定顺序的子集枚举在候选中找
  「人数和恰为 T」的一侧组合，再在剩余票中找第二侧，两侧平均等级差 ≤ MaxGap
  即成局（Match ID 由两侧票 ID 派生，确定性）；
- party 成员同侧由整队票天然保证；等级取票内平均。

## 5. 错误码（api/error/v1 新增）

`PARTY_NOT_FOUND` / `PARTY_FULL` / `ALREADY_IN_PARTY` / `NOT_PARTY_LEADER` / `NOT_IN_PARTY`。

## 6. e2e 验收场景

`scripts/e2e` 新增 `-mode party`：4 客户端——A 建队 → B 加入（roster 推送×2）→
A 发整队入队；C/D solo 入队 → 配成 2v2（side1={A,B}, side2={C,D}）→ 开局推送 →
帧同步 → 结算闭环；A 退出（roster 推送 + 若在匹配则整队取消）路径以断言覆盖取消推送。

## 7. 兼容性

- 全部 additive（既有单人 1v1 链路不变：2 solo 经 flex 规则仍配 T=1）；
- atlas 引擎改动为：Party 接口加 Describe（additive）、存储原子化（行为不变性保持）、
  容量/队长顺延（新语义）。
