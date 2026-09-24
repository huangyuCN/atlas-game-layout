# 观测占位（M7）

本目录提供本地观测栈（Prometheus + Loki + Grafana）的 compose 与配置。
**当前状态**：只登记**指标命名口径与面板位**（统一设计 v2 的 M7 预留位），
面板与告警随管理面（P7）、限流（M3）、背压（M4）一起接线——本轮不写实现。

## 指标命名表（唯一口径，代码与面板同源）

| 指标 | 类型 | 标签 | 产生点 | 含义 |
|------|------|------|--------|------|
| `gateway_requests_total` | Counter | `op`（路由表 operation）、`result`（success/error） | `services/gateway`（自留会话接口 + 业务 op 透传共用） | 全部入口 QPS 与失败率；面板按 `op` 汇总 |
| `gateway_push_total` | Counter | `op`、`result` | `services/gateway` 推送下发（含挤下线） | 下行推送量与失败率 |
| `actor_messages_total` | Counter | `kind`（ask/tell）、`result` | actor 运行时（框架 `contrib/actor/core`） | 单机消息吞吐与失败 |
| `actor_mailbox_depth` | Gauge | `actor_type` | actor 运行时 | 邮箱积压（配合 M4 背压观察） |
| `actor_remote_delivery_total` | Counter | `result`（ok/404/error） | actor 集群（框架 `contrib/actor/cluster`） | 跨节点投递与 404 重路由 |
| `battle_frames_total` | Counter | 聚合（不按 battle_id，避免高基数） | `services/battle` | 帧推进速率 |

约定：

- 指标名与标签名不出现业务实体 ID（避免高基数）；需要定位时用 trace（`trace_id`）关联。
- `op` 标签取值必须来自**生成的路由表**（`relay.RouteEntry.Operation`）或**会话协议描述符**
  （`gatewayv1opclient.SessionProtocolOps`），禁止手抄字面量（否则面板与路由表漂移）。

## 面板位（占位，未接线）

| 面板 | 口径 | 依赖 |
|------|------|------|
| 入口 QPS / 失败率 | `sum(rate(gateway_requests_total[1m])) by (op, result)` | 现有指标即可 |
| 推送时延与失败 | `rate(gateway_push_total[1m])` + trace 关联 | 现有指标即可 |
| actor 积压与背压 | `actor_mailbox_depth` + 拒绝计数 | M4 背压中间件 |
| 限流命中 | 限流拒绝计数（按 `op`） | M3 限流中间件 |
| 管理面审计 | GM 动作计数与失败率（按 `action`） | P7 管理面 |
