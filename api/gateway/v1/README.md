# api/gateway/v1 — 客户端协议（唯一客户端命名空间）

本目录是**客户端 op 的家**：SDK 只消费 `gateway.v1` 命名空间，gateway handler
是协议与集群内部面的转发桥（会话校验 → PID 组装 → actor 请求）。

## 协议边界约定（一句话规则）

| 协议面 | 位置 | 服务命名 | 特征 |
|---|---|---|---|
| 客户端 op（SDK 消费） | `api/gateway/v1/*_client.proto`（本目录） | `GatewayXxx` | 携带 token 鉴权位；gateway 校验会话并防伪造 player_id 后转发 |
| 集群内部方法 | 域包 `*_actor.proto` | `XxxActor` | 无鉴权位，身份由集群 PID 决定；服务端能力（如 GrantItem）不得出现在客户端面 |
| 管理面（grpc/http） | 域包（如 `player.proto`） | 与域同名 | 运维/活动渠道，google.api.http 注解 |

- 客户端协议事实源（三件套）：
  - `player_client.proto`（GatewayPlayer：注册/登录/心跳/登出/数据同步）
  - `match_client.proto`（GatewayMatch：匹配 + 灵活组队 + 推送）
  - `battle_client.proto`（GatewayBattle：战斗通道 + 帧广播/结束推送）
- 依赖单向：`gateway.v1` 可引 `game.v1`/`matcher.v1`/`battle.v1`/`common.v1`
  的类型（BackpackItem/MatchState/JoinBattleReply/PlayerSummary），反向禁止。
- 推送消息（Notify）按连接路由，只能住在客户端面文件；事件（Event）住在
  `match_events.proto` 等服务间契约文件，二者分属不同信任边界。
