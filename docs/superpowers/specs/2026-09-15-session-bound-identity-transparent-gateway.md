# 2026-09-15 客户端身份模型与透传 Gateway 全局重构

## 背景与问题

旧协议在客户端面（gateway.v1）为每个业务请求显式携带 `token + player_id`（15 个消息中 12 个重复），Gateway 是 11 个方法的「会话校验 → PID 组装 → 字段映射」投影层，同一业务在 gateway.v1 与域包各定义一套镜像消息，token 在 Gateway 与 game 各存一份，顶号裁决散在三处。这一惯性来自 Web 无状态模型（HTTP 每请求独立），与游戏长连接 + Actor 模型天然冲突——帧通道（SendFrameInput 按连接反查身份）已局部证明「连接即身份」可行。

## 设计决策

| 决策点 | 结论 |
|---|---|
| token 形态 | 降级为会话凭证：只在 Login 回执出现一次；新增 Resume(token, player_id) 断线免密恢复；业务请求零身份字段 |
| 协议结构 | 域 service + route 注解；删除 XxxActor/GatewayXxx 镜像双套；gateway.v1 命名空间保留并收敛为 Gateway 自身职责（会话生命周期/推送/未来 HTTP、gRPC 多协议接口） |
| 身份注入 | 主体身份 = target PID（Gateway 组装）+ sender（Gateway 注入投递 headers），永不来自客户端 payload |
| 通道绑定 | 「转发成功后绑定连接通道」属于网关会话模型的领域知识，**不进注解协议**——由透传引擎装配期按 op 声明（`Relay.WithChannelBinding`，槽位为 `relay.Slot`） |
| SDK | 三语言 SDK 同构：帧会话槽 + Session 对象；强类型 stub 生成器（protoc-gen-atlas-client）Go 版先行 |
| e2e | 改用 atlas-sdk-go（Session + Invoke）驱动，同时作为 SDK 的集成验收场 |

## 身份模型：连接即会话

```
Connect → Login（唯一带凭证的请求，回执发 token）
→ 会话绑定（Gateway session.Manager：连接 ↔ 玩家 ↔ 凭据）
→ 业务请求零身份字段
→ 断线 → Resume(token, player_id) 免密恢复 → 顶号/登出/过期解绑
```

主体/客体边界：

- **主体身份**（谁在操作）：target PID 的 UID（如 `player:{会话玩家}`，`uid=UID_SOURCE_SESSION`）+ sender（操作者，Gateway 注入）。客户端 payload 里没有身份字段可填，伪造面直接消失——比「每请求校验 token」更强，不是更弱。
- **客体参数**（操作目标）：`battle_id` / `party_id` 是业务参数，由注解声明寻址来源（`uid=UID_SOURCE_FIELD + uid_field`），语义为「加入/查询某个实体」；操作者由 sender 补齐。
- **「修改他人数据」永不暴露客户端面**：`access=INTERNAL`，客户端 op 不存在。

sender 机制（atlas 主仓 contrib/actor）：`core.WithSender(pid)` 选项随投递信封传输，跨节点经 `Envelope.Headers["x-atlas-sender-pid"]` 约定键往返，接收侧 `ActorContext.Sender() (types.PID, bool)` 读取；无 sender（服务端内部调用/定时器/自消息）返回零值与 false。

## 协议结构

```
api/
  atlas/v1/route.proto        ← Atlas 框架注解（route 扩展定义）
  gateway/v1/session.proto    ← Gateway 自留：Register/Login/Resume/Logout/Heartbeat + KickedNotify
  game/v1/player.proto        ← 管理面 grpc Player（google.api.http REST，保持不变）
  game/v1/player_service.proto ← PlayerService：客户端 op + actor 分发共用一份
  battle/v1/battle.proto      ← 管理面 grpc Battle + 结算事件
  battle/v1/battle_service.proto ← BattleService（帧通道与集群内部同构）
  matcher/v1/matcher.proto    ← 撮合引擎内部契约（不变）
  matcher/v1/match_events.proto ← nats 事件（不变）
```

### route 注解（service 级默认 + rpc 级覆盖）

```proto
service PlayerService {
  option (atlas.route.v1.service_route) = {
    actor: "player"          // target PID 的 type 段
    access: ACCESS_CLIENT    // service 内 rpc 默认客户端可达
    uid: UID_SOURCE_SESSION  // 默认寻址：UID 取登录会话身份
  };
  rpc EnterMatchQueue (EnterMatchQueueReq) returns (EnterMatchQueueReply);   // 零注解
  rpc JoinParty (JoinPartyReq) returns (PartyReply);                          // party_id 是客体参数
  rpc GrantItem (GrantItemReq) returns (GrantItemReply) {
    option (atlas.route.v1.rpc_route) = { access: ACCESS_INTERNAL };          // 例外才写
  }
}

service BattleService {
  option (atlas.route.v1.service_route) = {
    actor: "battle"
    access: ACCESS_CLIENT
    uid: UID_SOURCE_FIELD      // battle 域：UID 在消息里
    uid_field: "battle_id"
  };
  rpc JoinBattle (JoinBattleReq) returns (JoinBattleReply);   // 通道绑定由网关装配声明（非注解）
  rpc SendFrameInput (FrameInputReq) returns (google.protobuf.Empty);         // Empty 即 Tell
}
```

注解是 proto option（结构化声明），生成器与引擎读 option；数量上 service 级 3-4 行 + 例外 rpc 各 1 行，业务 rpc 本体零注解。

## 生成物（protoc 插件）

| 插件 | 产物 | 消费方 |
|---|---|---|
| protoc-gen-atlas-actor | `<service>_actor.pb.go`（Server 接口 + Unimplemented 兜底 + 分发桩 + `XxxClusterClient` 集群互调 stub）+ `<service>_route.pb.go`（`XxxServiceRouteTable` 透传路由表） | game/battle 服务端实现；Gateway 装配 |
| protoc-gen-atlas-client | `<service>_client.pb.go`（`XxxClient` 强类型 SDK stub：仅导出 access=CLIENT 方法） | 客户端 Go 项目（模板 e2e） |
| 多传输插件（tcp/ws/kcp/udp） | 仅 gateway.v1.Session 生成传输桩；域 service 不再生成传输桩（透传引擎运行时注册） | gateway 会话 handler |

`XxxClusterClient`（集群内部互调）与 `XxxClient`（客户端 SDK stub）命名区分，避免同包撞名。

## Gateway 三件事

1. **会话生命周期**：Register/Login/Resume/Logout/Heartbeat 本地实现；顶号裁决单点化（Session.Manager 路由覆盖 + Kicked 推送 + nats 跨实例通知），game 不再持有 token 副本。
2. **透传引擎**（relay.go）：查表 → 登录态校验（连接绑定或帧会话槽）→ 组 target PID → 注入 sender → 集群 Ask/Tell → 原样回执；「转发成功后绑定会话通道」（如 JoinBattle）由装配期 `Relay.WithChannelBinding(op, slot)` 声明——槽位集合是网关会话模型的领域知识，不进注解协议。新增域 op 时 Gateway 零代码（表由 protoc 生成，access=CLIENT 自动注册）。
3. **推送 relay**：nats 事件（MatchStartedEvent 等）→ 按 redis 路由表向玩家连接下发 Notify 帧（op = 消息完整名）。

客户端 operation 直接指向域 service：`/game.v1.PlayerService/EnterMatchQueue`——客户端协议面就是逻辑服的 service 面，Gateway 只是执行引擎。

## 帧协议会话槽（wire 破坏性变更，三语言 SDK 同步）

```
┌──────────┬──────┬──────┬────────┬───────┬───────────┐
│ magic(4) │ ver  │ type │flags(1)│rsv(1) │ seq(4)│bodyLen(4)│  大端，头 16B
└──────────┴──────┴──────┴────────┴───────┴───────────┘
┌───────────────────────────────────────────────┐
│ opLen(2) │ operation │ [sessionLen(2) │ session] │ payload │  body
└───────────────────────────────────────────────┘
```

- flags 位于帧头第 7 字节（原 rsv），bit0 = `FlagSession`；未知位（0xFE 掩码）非零即协议非法。
- 无连接传输（UDP/KCP）请求帧在凭据非空时置位并携带会话槽（每帧验证身份）；TCP/WS 按连接绑定、不置位。
- 服务端引擎解析会话槽写入 `Atlas-Frame-Session` 请求头，Gateway 透传引擎按凭据反查玩家身份。
- 三语言 SDK（atlas-sdk-go / ts / csharp）同构实现，必须同版本发布。

## SDK 会话层（三语言同构）

`Session` 对象：凭据保管（token/playerId）、`Login/Register/Resume/Logout`（默认 op `/gateway.v1.Session/*`，可覆盖）、内置会话心跳（无 payload，凭据空跳过）、断线自动 Resume（无凭据不算失败）、被挤下线推送（`/gateway.v1.KickedNotify`）自动清凭据、向通道装配凭据提供者与自动恢复钩子。强类型 stub（protoc-gen-atlas-client，Go 版本批）叠加其上；ts/csharp 的 stub 生成器为后续批次，期间用 `Invoke(op, req) + Session`。

## 协议不变式

1. 主体身份（target UID、sender）只由服务端注入，客户端 payload 无身份字段可填。
2. 客户端面只允许「我发起」语义；修改他人数据的操作 access=INTERNAL。
3. 未打 access=CLIENT 的 rpc：客户端 op 不存在、SDK 不导出、透传表不收录。
4. 客户端分发只走生成包：proto 源文件不出服务端。

## 实施记录

- atlas 主仓：sender 全链路（core.SendOption.WithSender → Message.Sender → Envelope headers 往返 → ActorContext.Sender）、route 注解定义、relay 包、actor 插件（注解驱动过滤 + 路由表生成）、transport 帧层 flags/会话槽。
- atlas-sdk-go/ts/csharp：帧会话槽 + Session 对象 + 测试（go：frame+client 全绿；ts：164 用例；csharp：162 用例）。
- atlas-game-layout：proto 重写（3 新文件，删除 gateway 客户端面/actor 镜像）、gateway 透传引擎 + Session handler 重写、game/battle/matcher 适配、e2e SDK 驱动。

## 后续批次

- ts/csharp 的强类型 stub 生成器（生成器加语言支持）。
- gateway.v1 下的 HTTP/gRPC 多协议接口（预留命名空间已就绪）。
