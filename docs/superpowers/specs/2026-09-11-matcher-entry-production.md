# 匹配入口生产化设计（gateway → PlayerActor → matcher）

- 日期：2026-09-11
- 状态：已评审（方向经用户确认：PlayerActor 转发 / 失败推送 / 下线自动取消 / 状态枚举化 / 组队后置）
- 关联：`scripts/e2e/main.go` 第 7 行注释「匹配经 matcher gRPC 直连（v1 简化）」的退役

## 1. 背景与现状

matcher 服务内核已是生产形态（redis 撮合引擎多实例安全、幂等结算、NATS 事件、开局懒激活），
「v1 简化」只存在于**客户端入口**与两个体验闭环：

| # | 临时点 | 生产问题 |
|---|--------|----------|
| 1 | e2e 客户端直连 matcher gRPC 入队 | 无鉴权、无服务发现、暴露内网拓扑；客户端 SDK 无此连接能力 |
| 2 | `QueueMatchRequest.PlayerSummary` 由调用方自报 | 匹配属性可伪造（反作弊硬伤） |
| 3 | gateway 只订阅成局事件 | 失败/超时/取消后客户端只能轮询或干等 |
| 4 | 下线不联动取消 | 票据靠 5min TTL 自然过期，占撮合资源、体验差 |

## 2. 目标链路

```
客户端(C#/TS) ──op(/gateway.v1.GatewayMatch/*)──▶ gateway
   ──PlayerActorClient(集群)──▶ game PlayerActor（权威属性/下线联动）
   ──matcher gRPC（discovery:///atlas.matcher）──▶ matcher 撮合引擎
成局/失败 ──NATS(atlas.event.match.*)──▶ gateway 推送（成局 + 失败都主动推）
```

- `matcher.proto` 头注释的原始设计「game 玩家 actor 经 grpc 调用」就此落地。
- PlayerActor 是匹配入口逻辑的家：**匹配属性从聚合根权威读取**（客户端无法伪造）；
  下线联动取消挂在 PlayerActor.OnStop（登出/挤下线都走自停，一处覆盖）。

## 3. 契约变更

### 3.1 matcher.proto（api/matcher/v1/matcher.proto）

- `MatchState` 枚举新增：`MATCH_STATE_UNSPECIFIED/WAITING/MATCHED/FAILED/NONE`；
- `QueryMatchReply.state`：string → enum（同字段号，wire 不兼容；模板仓内部契约，
  与「Reply 纯数据化」同款一次性切换，两端同步重生成）；
- `MatchFailedEvent` 增加 `ticket_id` 字段（additive），`match_id` 语义修正为
  「已成局的对局 ID，成局前失败为空」（现状把 ticketID 填进 match_id，语义混乱）；
- 注释更新：调用方为 game PlayerActor；`PlayerSummary` 为**服务端权威数据**
  （由 PlayerActor 从聚合根填充，客户端不参与）。

### 3.2 game 侧（api/game/v1/player_actor.proto 新增三个方法）

```proto
service PlayerActor {
  // …既有方法…
  rpc EnterMatchQueue (EnterMatchQueueActorReq) returns (EnterMatchQueueActorReply);
  rpc CancelMatch (CancelMatchActorReq) returns (CancelMatchActorReply);
  rpc GetMatchStatus (GetMatchStatusActorReq) returns (MatchStatusActorReply);
}
```

- 三个都是 **Ask**（returns 具体消息）：入队失败（已在匹配中）等业务错误必须同步回传，
  Tell 的错误不回传调用方；
- `EnterMatchQueueActorReq { string ruleset = 1; }`：只带规则集名；
  `EnterMatchQueueActorReply {}`；
- `CancelMatchActorReq {}` → `CancelMatchActorReply { bool canceled = 1; }`；
- `GetMatchStatusActorReq {}` → `MatchStatusActorReply { matcher.v1.MatchState state;
  string ticket_id; string match_id; }`（跨包引用 matcher.v1，依赖方向 game→matcher）。

### 3.3 gateway 侧（api/gateway/v1/matcher.proto 新文件）

```proto
service GatewayMatch {
  rpc QueueMatch (MatchQueueRequest) returns (MatchQueueReply);
  rpc CancelMatch (MatchCancelRequest) returns (MatchCancelReply);
  rpc MatchStatus (MatchStatusRequest) returns (MatchStatusReply);
}
message MatchFailedNotify { string ticket_id = 1; string reason = 2; }  // 新推送
```

- 请求体对齐 `JoinBattleRequest` 风格：`token + player_id`（gateway 用会话校验后
  的权威 playerID 调 actor）+ `ruleset`（仅入队）；
- **生成范围仅 TCP + WS**（业务通道；KCP/UDP 是战斗通道，不生成死桩）——Makefile
  为 matcher.proto 单列生成行。

### 3.4 Makefile

- `api/game/v1/player_actor.proto` 追加 atlas-actor 生成（既有行内追加）；
- `api/gateway/v1/matcher.proto` 单列一行 `--atlas-tcp_out + --atlas-ws_out`。

## 4. 服务改动

### game
- `biz`：新增 `MatchQueueClient` 接口（Enter/Cancel/Status，贴 matcher.v1 签名）；
- `data/repo`：gRPC 实现（`atlasgrpc.DialInsecure` + `WithDiscovery` +
  `discovery:///atlas.matcher`，etcd client 复用 fx 既有提供器）；
- `internal/actor`：PlayerActor 实现三个方法——EnterMatchQueue 校验在线态
  （ErrPlayerNotOnline 复用）→ 聚合根读 level 组装 PlayerSummary → 调 matcher；
  **OnStop 先 CancelMatch（幂等、失败仅记日志）再落库**；
- `internal/app`：fx 提供器接线（client 依赖进入 NewProps）。

### gateway
- handler 新增 3 方法：token 校验（sess.Validate，对齐 JoinBattle）→
  `g.players` 生成的 client stub 转发，业务错误透传；
- `push.go` 增订 `MatchFailedTopic` → `MatchFailedNotify` 按参战玩家推送；
- `servers.go` 注册 GatewayMatch 的 TCP/WS 桩。

### matcher
- 未知 ruleset 明确报 `ErrInvalidParams`（而非 ErrInternal）；
- `onFailed` 事件发布改为带 ticket_id、match_id 置空（语义修正）；
- 头注释同步。

## 5. e2e 改造

- 删除 matcher 直连（`-matcher` flag、`atlasgrpc.DialInsecure`、`matcherv1` 客户端）；
- 客户端经 gateway 业务通道 op 入队/取消（`NewGatewayMatchTCPClient` /
  `NewGatewayMatchWSClient`，与 auth 同通道复用连接）；
- 成局等待不变（MatchStartedNotify）；失败通知打印（不改变通过条件）。

## 6. 兼容性

- `QueryMatchReply.state` 类型切换为 wire 不兼容：模板仓内部契约一次性切换，
  升级需同时更新两端（先例：actor 分发迁移的 BREAKING 提交）；
- `MatchFailedEvent` 为 additive 演进；
- 其余契约均为新增（additive）。

## 7. 范围取舍

- 组队匹配（PartyTicket）本期不做——atlas contrib/matchmaker 已支持 party，
  后续迭代单独出方案；
- ruleset 白名单本期为单规则集（casual），扩规则属运营配置演进，不阻塞生产化；
- matcher 多实例安全由 redis 后端与 SETNX 去重保证（现状已满足，本设计不引入新状态）。
