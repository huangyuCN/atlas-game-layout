# 数据流图解：这次重构后一条消息是怎么走的

> 配套设计文档：`docs/superpowers/specs/2026-09-15-session-bound-identity-transparent-gateway.md`
> 本文不讲为什么，只讲「数据怎么流」——每张图对应一个真实场景，图中每个框都是代码里真实存在的一跳。

## 0. 先记住三个词

| 词 | 是什么 | 在代码里 |
|---|---|---|
| **会话** | 登录后「连接 ↔ 玩家 ↔ 凭据」的绑定关系 | gateway 的 `session.Manager`（redis 路由表 + 本地连接表） |
| **透传引擎** | 把客户端业务 op 原样转发到 actor 的执行器 | gateway 的 `server/relay.go`（`Relay.Forward`） |
| **路由表** | 「op → 目标 actor 规则」的清单，由 proto 注解自动生成 | `api/*/v1/*_service_route.pb.go` 里的 `XxxServiceRouteTable` |

身份去哪了：**客户端消息里没有 token、没有 player_id**。客户端发什么，服务器就照原样转发——服务器只负责「证明你是谁」和「找谁」。

---

## 1. 全景图：一次「入队匹配」的完整路径

```
 客户端（SDK）                     Gateway                          game 服务（PlayerActor）
┌─────────────┐              ┌──────────────────┐              ┌──────────────────────┐
│ Invoke(     │              │  ①登录态识别      │              │                      │
│  "/game.v1  │   帧(TCP)    │    (查会话)       │              │   ④分发桩 type switch│
│  .PlayerService                     │    │        │   ⑤调用业务方法        │
│  /EnterMatch│─────────────▶│  ②查透传路由表    │─────────────▶│  (消息里没有身份字段) │
│  Queue",    │              │  ③组装 target PID │  集群投递    │                      │
│  req)       │              │  ④注入 sender     │  (Ask)       │  ⑥回执原样返回        │
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
 │ ③ 组装 target PID                                                      │
 │    actor="player" + uid=会话玩家 → PID{player, "42"}                    │
 │    → 决定消息发给谁：**玩家 42 自己的 actor**                             │
 │                                                                        │
 │ ④ 注入 sender（发起者身份）                                             │
 │    sender = PID{player, "42"}（同款玩家身份）                            │
 │    放进投递信封的 headers（x-atlas-sender-pid），不进业务消息              │
 │                                                                        │
 │ ⑤ 集群投递 Ask/Tell                                                    │
 │    returns 是 Empty → Tell（单向）；是具体消息 → Ask（等回执）             │
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
 集群（NATS AskRequest）                        game 进程
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

## 5. 第 4 幕：战斗——「channel 绑定」到底绑了什么

战斗场景是唯一有「**第二条连接**」的地方：客户端在业务通道之外，再拨一条战斗通道（KCP/UDP，低延迟）。所以会话管理器里一个玩家有两个连接槽：

```
 session.Manager（会话）
 ┌─────────────────────────────────────────────────────┐
 │ Session{ playerID: "42"                             │
 │          token:    "tok-…"                          │
 │          Biz:    conn:101 (TCP，登录时绑定)           │
 │          Battle: conn:77  (KCP，JoinBattle 后才有)    │  ← 这就是「channel 绑定」
 │        }                                            │
 └─────────────────────────────────────────────────────┘
```

`JoinBattle` 的转发多一步**绑定副作用**：

```
 客户端(KCP连接)                    Gateway                              battle actor
 │  Invoke("/battle.v1.            │                                     │
 │   BattleService/JoinBattle",    │  ①登录态（帧槽验证）                  │
 │   {battle_id: "b-9"})           │  ②查表：uid来源=消息里的 battle_id    │
 │────────────────────────────────▶│  ③组装 PID{battle, "b-9"}  ← 客体寻址 │
 │                                 │  ④sender = PID{player,"42"}         │
 │                                 │  ⑤Ask 战斗 actor：                    │
 │                                 │     资格裁决 → 回执帧元信息            │
 │                                 │  ⑥★ 转发成功后：                      │
 │                                 │    sess.Bind(玩家42, 本连接,          │
 │                                 │              槽位=Battle)  ◀── 装配期声明│
 │                                 │      （graph.go 里一行                 │
 │                                 │       WithChannelBinding(op, 槽)）    │
 │◀────────────────────────────────│                                     │
 │  JoinBattleReply{帧元信息,当前帧,快照}                                 │
```

绑定之后，**帧广播这类战斗推送就知道往哪条连接发**（优先走 Battle 槽的连接），而不是混进业务连接。它是网关会话模型的领域知识，所以在网关装配代码里声明一行，不进注解协议——加观战、语音通道时改网关，不动协议。

而消息里的 `battle_id` 是**客体参数**（操作目标），不是身份——你是谁由 sender 说了算，打哪场仗由 battle_id 说了算。

---

## 6. 第 5 幕：帧输入——「你是谁」只在 sender 里

```
 客户端                  Gateway                         battle actor
 │  Invoke("/battle.v1.  │                               │
 │   BattleService/      │  帧槽验证 → 玩家 42            │
 │   SendFrameInput",    │  PID{battle,"b-9"}            │
 │   {battle_id:"b-9",   │──────Tell, sender=──────▶     │
 │    input:{...}})      │        player:{42}            │
 │                       │                ctx.Sender().UID() = "42"
 │                       │                （消息里没有玩家，也不需要）      │
 │◀──── 空回执(Tell) ─────│◀───────回执────────────────────│
```

旧实现在这里靠 gateway 改写 `Input.PlayerId` 防伪造；新实现消息里根本没有这个字段可伪造——**防线内移**到 battle 侧：收到的消息若没有 sender（服务端内部直调漏注入）直接按协议错误拒绝。

---

## 7. 第 6 幕：推送下行——「按会话路由的两条通道」

```
                        battle 服务 / matcher 服务（业务侧）
                              │  发推送（业务代码里一行）：
                              │  pkgnats.PublishEnvelope(nc, 玩家ID, op, payload)
                              │  主题：atlas.push.<playerID>，信封 {type, payload}
                              ▼
                    ┌───────────────────────┐
                    │        NATS           │
                    └─────────┬─────────────┘
                              ▼
              gateway StartRelay 订阅 atlas.push.>
                              ▼
              ┌───────────────────────────────┐
              │ 本实例持有玩家 42 的连接吗？     │
              │ （查 redis 路由表 → 是/否）     │
              └───────────┬───────────────────┘
                          ▼
              PushRaw(connID, op, payload)   ← 按「哪个通道」发：
                                              battle 类 op 优先走 Battle 槽连接，
                                              其他走 Biz 槽连接
                              ▼
                    ┌─────────────────────┐
                    │  客户端 Notify 帧     │
                    │  cli.On(op, ...):   │
                    │  - "/battle.v1.FrameBroadcast"      │
                    │  - "/gateway.v1.KickedNotify"       │
                    └─────────────────────┘
```

撮合事件（成局/失败）走的是另一类主题（`atlas.event.match.*`）：gateway 订阅事件 → 转成 Notify 消息（`game.v1.MatchStartedNotify`）→ 再经 `atlas.push.<玩家ID>` 按玩家下发。**推送 op = 消息完整名（`/` 开头）**，客户端订阅用它，与业务 Invoke 的 op 同一个字符串空间。

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

---

## 9. 对照表：身份字段搬家记

| 旧协议 | 新协议 | 搬到哪里 |
|---|---|---|
| 每请求带 `token` | 消息里没有 | 会话：登录绑定一次；UDP/KCP 每帧走帧会话槽（SDK 自动填，业务无感） |
| 每请求带 `player_id` | 消息里没有 | target PID（Gateway 从会话组装）/ 战斗类 op 由 sender 注入 |
| gateway 改写业务消息防伪造 | 不改写（原样转发） | 消息里没有身份字段可伪造；跨实体操作（打哪场仗）是客体参数 |
| gateway 手写 11 个投影 handler | 零手写 | 透传引擎 + 注解路由表（protoc 生成） |
| game 存第二份 token + 比对裁决 | game 不存 token | 顶号裁决单点在 Gateway 会话管理器 |

## 10. 一句话备忘

- **请求路径**：SDK Invoke → 透传引擎（认身份、找目标、带上 sender）→ 集群 → actor 分发桩 → 业务方法。
- **推送路径**：业务侧 `PublishEnvelope` → NATS → 持连接的 Gateway → 会话路由选通道 → Notify 帧。
- **channel 绑定** = 「这条连接从此归玩家会话的战斗槽管」，JoinBattle 成功后由网关绑定（装配声明一行）。
- **身份只有两个来源**：连接绑定（TCP/WS）与帧会话槽（UDP/KCP）——都出自 Gateway 的会话管理器，客户端 payload 从头到尾没有参与。
