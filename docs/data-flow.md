# 数据流图解：这次重构后一条消息是怎么走的

> 配套设计文档：`docs/superpowers/specs/2026-09-15-session-bound-identity-transparent-gateway.md`
> 本文不讲为什么，只讲「数据怎么流」——每张图对应一个真实场景，图中每个框都是代码里真实存在的一跳。
> **阶段 3（2026-09-29）起战斗帧直连接入层**：第 4–6 幕与备忘按直连口径重写，网关只剩业务通道
>（tcp/ws + http + grpc）；业务通道（第 1–3 幕、第 5 幕顶号）的叙述保持不变。

## 0. 先记住五个词

| 词 | 是什么 | 在代码里 |
|---|---|---|
| **会话** | 登录后「连接 ↔ 玩家 ↔ 凭据」的绑定关系（阶段 3 起只剩业务通道一条） | gateway 的 `session.Manager`（redis 路由表 + 本地连接表） |
| **透传引擎** | 把客户端业务 op 原样转发到域 `rpc/` 平面 Edge 接口（gRPC）的执行器 | gateway 的 `server/relay.go`（`Relay.Forward`） |
| **路由表** | 「op → 目标 actor 规则」的清单，由 proto 注解自动生成 | `api/*/v1/*_service_route.pb.go` 里的 `XxxServiceRouteTable` |
| **接入层** | 战斗帧的 L4 转发器：验票 → 查 actor 目录选属主 → 转发到 battle 帧面；不解析帧协议正文 | 模板 `services/edge`（框架 `contrib/edge`） |
| **战斗票据** | 成局时逐人签发的 AEAD 凭据（绑 `player_id` + `battle_id`）：接入层用它选后端，battle 用它认人 | 框架 `contrib/edge/ticket`（接入层与 battle 共持同一把 32 字节密钥） |

身份去哪了：**客户端消息里没有 token、没有 player_id**。客户端发什么，服务器就照原样转发——服务器只负责「证明你是谁」和「找谁」。

---

## 1. 全景图：一次「入队匹配」的完整路径

```
 客户端（SDK）                     Gateway                          game 服务（PlayerActor）
┌─────────────┐              ┌──────────────────┐              ┌──────────────────────┐
│ Invoke(     │              │  ①登录态识别      │              │                      │
│  "/game.v1  │   帧(TCP)    │    (查会话)       │              │   ④分发桩 type switch│
│  .PlayerService                     │    │        │   ⑤调用业务方法        │
│  /EnterMatch│─────────────▶│  ②查透传路由表    │─ gRPC ──────▶│  (消息里没有身份字段) │
│  Queue",    │              │  ③组装 metadata身份│ 域 Edge 面   │                      │
│  req)       │              │  ④按方法寻址投递  │  (Ask)       │  ⑥回执原样返回        │
│             │◀─────────────│  (凭据不可见)     │◀─────────────│                      │
└─────────────┘   响应帧      └──────────────────┘   回执帧      └──────────────────────┘
```

关键：①②③④ 都在 Gateway 内部完成，客户端 payload 原封不动穿过去；game 的业务方法拿到的消息就是客户端发的那个消息。

---

## 2. 第 1 幕：登录——整个协议里唯一带「凭证」的请求

```
 客户端                    Gateway                                    game（PlayerActor）
 ───────                   ────────                                   ──────────────────
 │                                                                                   │
 │  Login{player_id, password}                                                       │
 │───────────────────▶│                                                             │
 │                    │  签发 token（随机数）                                        │
 │                    │                                                             │
 │                    │   LoginReq{player_id, password, token, gateway_instance}    │
 │                    │────────────────────────────────────────────────────────────▶│
 │                    │                                校验密码（不存 token！只校验）│
 │                    │◀────────────────────────────────────────────────────────────│
 │                    │  LoginReply{player}                                         │
 │                    │                                                             │
 │                    │  ★ 会话绑定：session.Bind(玩家, 连接, token)                  │
 │                    │    - redis 路由表：player_id → {token, 实例ID, 连接ID}         │
 │                    │    - 本地连接表：connRef → player_id（按连接反查身份）          │
 │                    │    - 凭据表：token → player_id（UDP 帧槽反查身份）             │
 │                    │  ★ 若已有旧会话 → 挤下线（推送 KickedNotify + 断旧连接）        │
 │                    │                                                             │
 │◀───────────────────│                                                             │
 │  LoginReply{player_id, token, player}                                             │
 │  （token 只在这里出现这一次，之后客户端存进 Session 对象，业务请求不再发它）          │
```

要点：

- token 只出现**一次**：登录回执。之后它躺在客户端 `Session` 对象里、服务端 redis 路由表里，再也不会出现在任何业务消息里。
- game 侧**不再保存 token**（旧实现会在 redis 存第二份并做新旧比对——删了）。谁该下线、谁顶了谁，全部由 Gateway 的会话管理器裁决，game 只收结果。

---

## 3. 第 2 幕：业务请求透传——「入队匹配」六步流水线

客户端发：`Invoke("/game.v1.PlayerService/EnterMatchQueue", {ruleset: "casual"})`
消息体：`{ruleset: "casual"}`——就这一个字段。

```
 透传引擎 Relay.Forward 的六步（server/relay.go）

 ┌────────────────────────────────────────────────────────────────────────┐
 │ ① 登录态识别                                                            │
 │    TCP/WS：连接绑定 → connRef("conn:123") → sess.PlayerByRef → 玩家     │
 │    UDP/KCP：帧会话槽(Atlas-Frame-Session) → sess.PlayerByToken → 玩家   │
 │    都没有 → 直接回 INVALID_TOKEN（不用到业务层）                          │
 │                                                                        │
 │ ② 查透传路由表                                                          │
 │    op "/game.v1.PlayerService/EnterMatchQueue"                          │
 │      → {actor: "player", uid来源: 会话, 可达性: CLIENT}                  │
 │    （这个条目是 protoc-gen-atlas-actor 从注解生成的，不是手写的）          │
 │                                                                        │
 │ ③ 组装调用身份（metadata 三键，进请求头不进载荷）                       │
 │    x-atlas-player-id  = 会话玩家（"42"）                                │
 │    x-atlas-sender-pid = player:42（发起者；客户端不可影响）             │
 │    x-atlas-request-id = 帧头请求 ID（观测头；幂等 op 接收侧作去重键）   │
 │                                                                         │
 │ ④ 按方法寻址投递（gRPC 一元调用）                                       │
 │    opcall.DeliverRemote → opgrpc.NewInvoker → 域 rpc/ 平面的 Edge 接口  │
 │    （按面的 scheme 选端点：客户端 op 走 grpc-edge，服务间调用走 grpc）  │
 │                                                                         │
 │ ⑤ 域侧解析 PID 并投递到 actor（解析与懒激活都在托管方）                 │
 │    Edge 方法体 opcall.PIDFrom：会话身份或 uid_field → PID{player, 42}   │
 │    returns 是 Empty → Tell（单向）；是具体消息 → Ask（等回执）          │
 │                                                                        │
 │ ⑥ 原样回执                                                             │
 │    actor 的回执对象直接编码回客户端，不映射不投影                          │
 └────────────────────────────────────────────────────────────────────────┘
```

为什么客户端不用填 player_id？**②③ 已经替它填好了**——客户端声明不了「我是谁」，只有 Gateway 能从会话里查出来。这就是「主体身份永不来自 payload」。

---

## 4. 第 3 幕：业务层怎么「接收」——actor 侧的拆包

上面 ⑤ 投出的消息到了 game 服务，落地路径：

```
 域 Edge 接口（gRPC 入站）                       game 进程
 ───────────────────────                       ─────────────────────────────────────────
 │                             │ PID{player,42} 的 cell 收到信封                         │
 │                             │                                                        │
 │                             │  入站解码：type_url → 具体消息对象                        │
 │                             │    （生成的 decode 表，按消息名查 Unmarshal）             │
 │                             │    EnterMatchQueueReq{ruleset}                          │
 │                             │                                                        │
 │                             │  分发桩 OnAsk（生成代码，静态 type switch）               │
 │                             │    case *EnterMatchQueueReq →                           │
 │                             │      p.EnterMatchQueue(ctx, req)                        │
 │                             │    └─ ctx：ActorContext                                 │
 │                             │        ctx.Self()  = PID{player, "42"}  ← 我是谁        │
 │                             │        ctx.Sender()= PID{player, "42"}  ← 谁发起（网关注入）│
 │                             │                                                        │
 │                             │  业务方法返回回执 → 原路编码回包 ─────────────────────────▶ │
 └─────────────────────────────┘                                                        ─
```

三个读身份的口，注意区分：

| ctx 里读什么 | 值 | 用途 |
|---|---|---|
| `ctx.Self()` | PID{player, "42"} | **我处理的是谁的聚合根**（ actor 只处理自己的数据，由 PID 保证） |
| `ctx.Sender()` | PID{player, "42"}（空/缺时返回 false） | 操作发起者——服务端内部调用（如 matcher 通知）时为空 |

---

## 5. 第 4 幕：战斗直连——成局推送带地址与票，帧不再经网关

阶段 3（2026-09-29）起，战斗场景仍是唯一有「**第二条连接**」的地方，但它不再拨网关，而是拨**接入层**
（`services/edge`，无状态 L4 转发）：网关只剩业务通道（tcp/ws + http + grpc），会话也收敛为**单通道**
——`session.Manager` 里一个玩家只有登录时绑定的那一条连接，不再有 Battle 槽。

```
 客户端                                    接入层 services/edge              battle 属主节点
 ┌────────────────┐  ①成局推送（网关按人下发）┌────────────────────┐          ┌──────────────────────┐
 │ MatchStarted   │◀─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │ ②hello 验票（AEAD） │          │ ④帧槽验票 → 身份进 ctx │
 │ Notify{        │                          │ ③查 actor 目录      │ L4 转发  │ ⑤路由表查条目          │
 │  endpoints[],  │  帧连接(KCP/UDP/WS)       │   → 属主 node_id    │─────────▶│ ⑥opcall.Deliver       │
 │  battle_ticket}│─────────────────────────▶│   → 帧面实例+端口    │          │   → 本地 battle actor │
 └────────────────┘                          └────────────────────┘          └──────────────────────┘
```

六跳各自的真实实现：

| 跳 | 谁做 | 做什么 | 代码 |
|---|---|---|---|
| ① 出票与推送 | matcher → battle actor → 网关 | matcher 调 `IssueEntryTicket`（INTERNAL），battle 按参战名单**逐人签票**（票绑 `player_id` + `battle_id`）并回填接入层各面地址；网关 `onMatchStarted` **逐人构造 payload** 下发（票是身份凭据，禁止群发） | `services/matcher/internal/infra`、`services/battle/internal/actor/battle_ticket.go`、`services/gateway/internal/server/push.go` |
| ② 客户端取地址 | SDK | 从 `endpoints[]` 里按**自己的传输面**取接入层地址（TS 只有 ws）；**缺该面即报错**，不猜端口、不读本地配置 | 三 SDK 的直连会话 |
| ③ hello 握手 | SDK → 接入层 | WS 面：HTTP 升级请求带票（`?ticket=` 或 `X-Atlas-Ticket`）；KCP/UDP 面：首包 `"ATLH"｜version｜票长｜票` → 接入层回 flow-id（8 字节大端），此后双向每个数据报都带 `flow-id(8)｜载荷` 前缀 | `contrib/edge/{hello,flowid}.go` |
| ④ 定位属主 | 接入层 | 用同一把 32 字节 AEAD 密钥解票得 `battle_id` → 查 actor 目录（`<ns>/<pid>`）得属主 `node_id` → 按 `node_id` 选 battle 注册的 **`battle-frame`** 实例，读元数据里的传输面端口（端口不写死、不进票据） | `services/edge/internal/resolver` |
| ⑤ 帧面验票 | battle 帧面 | 帧会话槽（`Atlas-Frame-Session`，base64url 无填充）带同一张票 → 验票 → 校验 `player_id`/`battle_id` 归属 → 身份进 ctx（帧协议零改动） | `services/battle/internal/server/identity.go` |
| ⑥ 本地投递 | battle | 路由表查 op 条目 → `opcall.Deliver`（**按 PID 寻址**）投给本地战斗 actor；业务 op 走网关的是 `opcall.DeliverRemote`（**按方法寻址**），两条路径不混用 | `services/battle/internal/server/frames.go` |

两处拒绝语义**不同层**，别混：

- **接入层**：L4 无应用层回执——拒票/后端不可用只体现为**断开 + 指标**
  `edge_ticket_rejected_total{reason}`（`ticket_invalid` / `ticket_expired` / `backend_unavailable` /
  `hello_malformed` / `rate_limited` / `stream_limit`），reason 枚举在模板 `lib/consts`，不进 proto；
- **battle 帧面**：帧有回执，票据问题回结构化错误（`BATTLE_TICKET_INVALID` / `BATTLE_TICKET_EXPIRED`）。

网关侧只剩业务面：`server.kcp`/`server.udp` 两节删除、`battle.v1.BattleServiceRouteTable` 不再合并，
经网关发战斗 op 在帧引擎层**明确失败**（`TRANSPORT_NOT_FOUND`，不是静默丢弃）。

> 消息里的 `battle_id` 仍是**客体参数**（操作目标），不是身份——你是谁由帧槽票据说了算，打哪场仗由 `battle_id` 说了算。

---

## 6. 第 5 幕：帧输入——「你是谁」来自帧槽票据

```
 客户端                    接入层                     battle 帧面              battle actor
 │  帧（flow-id 前缀/裸帧）│                         │ 帧槽验票 → 玩家 42        │
 │  Invoke("/battle.v1.   │─── L4 转发（不解析 ───▶│ PID{battle,"b-9"}        │
 │   BattleService/       │     帧协议正文）        │──────Tell, sender= ─────▶│ ctx.Sender().UID()="42"
 │   SendFrameInput",     │                         │        player:{42}       │ （消息里没有玩家，也不需要）
 │   {battle_id:"b-9",    │                         │                          │
 │    input:{...}})       │                         │                          │
 │◀──── 空回执(Tell) ─────│◀────────────────────────│◀───────回执───────────────│
```

旧实现在这里靠 gateway 改写 `Input.PlayerId` 防伪造；新实现消息里根本没有这个字段可伪造——**防线内移**
到 battle 侧：收到的消息若没有 sender（服务端内部直调漏注入）直接按协议错误拒绝。接入层全程只认自有的
hello 段与 flow-id，**不解析帧协议正文**（operation/回执/错误都由 battle 侧处理）。

---

## 7. 第 6 幕：推送下行——业务推送走网关，战斗域通知走直连

**业务类推送**（成局通知、被顶下线、掉线联动等）不变，仍按会话路由到网关持有的那一条连接：

```
                        battle 服务 / matcher 服务（业务侧）
                              │  发推送（业务代码里一行）：
                              │  pkgnats.PublishEnvelope(nc, 玩家ID, op, payload)
                              │  主题：atlas.<ns>.push.<playerID>（`<ns>` = runtime.namespace 派生的命名空间），信封 {type, payload}
                              ▼
                    ┌───────────────────────┐
                    │        NATS           │
                    └─────────┬─────────────┘
                              ▼
              gateway StartRelay 订阅 atlas.<ns>.push.>
                              ▼
              ┌───────────────────────────────┐
              │ 本实例持有玩家 42 的连接吗？     │
              │ （查 redis 路由表 → 是/否）     │
              └───────────┬───────────────────┘
                          ▼
              PushRaw(connID, op, payload)   ← 阶段 3 起会话只剩单通道：
                                               业务 op 与业务推送都走登录时绑定
                                               的那条连接（旧 Battle 槽已删除）
                              ▼
                    ┌─────────────────────┐
                    │  客户端 Notify 帧     │
                    │  cli.On(op, ...):   │
                    │  - "/gateway.v1.KickedNotify"    │
                    │  - "/game.v1.MatchStartedNotify" │
                    └─────────────────────┘
```

**战斗域通知**（帧广播 `FrameBroadcast`、战斗结束 `BattleEndNotify`、出局 `PlayerOutNotify`、补帧）
自阶段 3 起**不再经 NATS 与网关**：battle 侧维护「`player_id` → 直连连接」注册表
（握手/JoinBattle 登记、断连注销、重连时新连接接管旧连接），用帧引擎既有推送原语直发：

```
  battle actor ──▶ 直连连接注册表（player_id → 连接）──▶ 帧引擎推送原语：
                   stream.Registry                       StreamEngine.Push / PushRaw / PushToAll
                                                         DatagramEngine.PushTo
                              ▼
                    客户端直连连接上的 Notify 帧（SDK 用 WithClientNotifyHandler 收）
                    op 同样是消息完整名："/battle.v1.FrameBroadcast" 等，payload = protojson
```

结算后 battle 广播 `BattleEndNotify` 并**关闭该局全部直连**（接入层流随之回收），不留悬挂连接。

撮合事件（成局/失败）走的是另一类主题（`atlas.<ns>.event.match.*`）：gateway 订阅事件 → 转成 Notify 消息（`game.v1.MatchStartedNotify`）→ 再经 `atlas.<ns>.push.<玩家ID>` 按玩家下发（成局通知里的接入层地址与 `battle_ticket` 就随这条路径到达客户端）。**推送 op = 消息完整名（`/` 开头）**，客户端订阅用它，与业务 Invoke 的 op 同一个字符串空间。

---

## 8. 第 6 幕：顶号与断线恢复

```
 顶号（B 在别处登录同一账号）
 ┌─────┐            ┌──────────┐            ┌────────────┐
 │  A  │            │ Gateway  │            │ game actor │
 │(旧) │            │ (会话管理)│            │  (玩家42)   │
 └──┬──┘            └────┬─────┘            └─────┬──────┘
    │  B 登录             │                        │
    │────────────────────▶│ 裁决：redis 路由表已有 42 → 挤旧
    │                     │ 覆盖路由(新token) + CancelMatch/LeaveParty
    │◀── KickedNotify ────│                        │
    │◀── 断连接            │  跨实例：nats 控制通道通知旧实例
    │                     │                        │
    │                     │  LogoutMsg{reason}（无 token，不再比对）│
    │                     │──────────────────────▶│ 保存数据 → 停机
    └─────────────────────┘                        │

 断线重连（A 掉线后回来）
    │  Resume{token, player_id}（免密，凭据即会话槽原值，不轮换）
    │──────────────────▶│ 路由表校验归属 → 重绑新连接 → 业务继续
```

关键语义：顶号的「谁该下线」只有 Gateway 能裁决（它持有唯一权威的路由表）；game 收到的 `LogoutMsg` 不带 token——**收到即保存并停机**，没有「比对一下再决定停不停」的中间态。

**与战斗直连的关系（阶段 3 起）**：顶号与被顶都发生在**业务链路**，触达不到直连帧连接——战斗中的直连**不因业务会话被顶而断开**（本局继续，掉线计时不启动），被顶的旧会话只是不能再经业务面操作该局。

---

## 9. 对照表：身份字段搬家记

| 旧协议 | 新协议 | 搬到哪里 |
|---|---|---|
| 每请求带 `token` | 消息里没有 | 业务：登录绑定一次；战斗帧：每帧走帧会话槽（SDK 把 `battle_ticket` 填进 `Atlas-Frame-Session`，业务无感） |
| 每请求带 `player_id` | 消息里没有 | 业务 op：target PID（Gateway 从会话组装）+ 发起者 sender；战斗帧：battle 帧面验票得 `player_id`，随**本地投递**作 sender |
| gateway 改写业务消息防伪造 | 不改写（原样转发） | 消息里没有身份字段可伪造；跨实体操作（打哪场仗）是客体参数 |
| gateway 手写 11 个投影 handler | 零手写 | 透传引擎 + 注解路由表（protoc 生成） |
| 战斗帧经网关的「战斗通道」（阶段 3 前，2026-09-29 已删） | 客户端直连接入层 → battle 帧面 | 票据即凭据：接入层验票选后端、battle 验票认人；网关不再持有战斗通道 |
| game 存第二份 token + 比对裁决 | game 不存 token | 顶号裁决单点在 Gateway 会话管理器 |

## 10. 一句话备忘

- **业务请求路径**：SDK Invoke → 网关透传引擎（认身份、查路由表、组装 metadata 身份）→ gRPC 调域 Edge 面 → 域侧解析 PID（懒激活）→ actor 分发桩 → 业务方法。
- **战斗帧路径**：成局推送带「接入层各面地址 + 本局票据」→ 客户端按传输面取地址 → hello 验票 → 接入层查目录选属主 → L4 转发到 battle 帧面 → 帧槽验票 → 本地 actor 投递（不再经网关）。
- **推送路径**：业务类 `PublishEnvelope` → NATS → 持连接的 Gateway → Notify 帧；战斗域通知（帧广播/结束/出局）走 battle 侧连接注册表 + 帧引擎推送原语，直发客户端。
- **身份只有两个来源**：业务连接绑定（TCP/WS）与帧槽票据（KCP/UDP/WS 直连面）——客户端 payload 从头到尾没有参与。
