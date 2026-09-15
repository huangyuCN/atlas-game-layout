# api/gateway/v1 — Gateway 进出口命名空间

本目录是 **Gateway 自身职责的协议之家**：会话生命周期（Session 服务）与
Gateway 专属推送（KickedNotify），以及未来将在此定义的 HTTP / gRPC 多协议
管理接口。业务 op **不在本目录**——它们直接指向各域 service 的统一契约
（客户端 op 与 actor 分发共用一份），Gateway 按注解生成的路由表透传。

## 协议边界约定（连接即会话，主体身份永不来自客户端 payload）

| 协议面 | 位置 | 服务命名 | 特征 |
|---|---|---|---|
| 会话生命周期（Gateway 自留） | `session.proto`（本目录） | `Session` | Register/Login/Resume/Logout/Heartbeat 本地实现；token 只在 Login 回执出现一次；四传输生成桩 |
| Gateway 专属推送 | `session.proto` 的 KickedNotify | —（消息名寻址） | op = `/gateway.v1.KickedNotify` |
| 域统一契约（客户端 op + actor 分发） | 域包 `*_service.proto`（如 `game/v1/player_service.proto`） | `XxxService` + `atlas.route.v1` 注解 | access=CLIENT 为客户端 op（SDK 导出、透传注册）；INTERNAL 为服务端内部；身份由 target PID + sender 承载，消息无 token/player_id |
| 管理面（grpc/http） | 域包（如 `game/v1/player.proto`） | 与域同名 | 运维/活动渠道，google.api.http 注解 |

- 业务 op 直接指向域 service（如 `/game.v1.PlayerService/EnterMatchQueue`），
  Gateway 透传引擎按路由表（protoc-gen-atlas-actor 从注解生成）运行时注册，
  新增域 op 时 Gateway 零代码。
- 推送消息按域归属：撮合推送（MatchStartedNotify 等）在 `game.v1`，帧广播/
  战斗结束在 `battle.v1`，KickedNotify 在本目录；op 均为消息完整名。
- 依赖单向：本目录可引 `common.v1`（及域包类型），域包不反向引用本目录。
- 设计文档：`docs/superpowers/specs/2026-09-15-session-bound-identity-transparent-gateway.md`。
