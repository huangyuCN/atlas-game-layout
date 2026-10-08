# 观测占位（M7）

本目录提供本地观测栈（Prometheus + Loki + Grafana）的 compose 与配置。
**当前状态**：指标命名口径、面板与**阶段 3 告警规则已落地**（见下方「阶段 3 落地」）；
M7 预留的管理面/限流/背压面板仍随 P7、M3、M4 接线。

## 阶段 3 落地（接入层 + 战斗域）

| 文件 | 内容 |
|------|------|
| `rules/atlas-alerts.yml` | 9 条告警规则（2 组：`atlas-edge-stage3` 6 条 / `atlas-battle-stage3` 3 条），阈值与 `docs/config.md` §7.5 同源 |
| `grafana/provisioning/dashboards/json/atlas-stage3.json` | 面板：活跃流（含 80% 阈值线）、在线玩家、拒票/拆流（按 reason）、探活失败、掉线判负/回座、转发字节、下行失败 reason（Loki） |
| `prometheus-config.yaml` | `rule_files` 指向 `/etc/prometheus/rules/*.yml`；`atlas` job 增加 **edge 9154**（接入层指标的唯一来源） |
| `promtail-config.yaml` | 增加 **edge.log**（`service="edge"`）日志源——接入层是拒票/拆流/探活失败的第一现场 |
| `docker-compose.yaml` | prometheus 挂载 `./rules` 到 `/etc/prometheus/rules:ro` |

**怎么验**（不重启即可校验规则语法；改动规则后重启 prometheus 生效）：

```bash
# 1) 语法与表达式校验（9 rules found 即通过）
docker run --rm --entrypoint promtool -v "$PWD/rules:/r:ro" prom/prometheus:v3.1.0 check rules /r/atlas-alerts.yml
# 2) 加载确认（宿主机 9095 → 容器 9090）
curl -s http://127.0.0.1:9095/api/v1/rules | python3 -m json.tool | head -40
# 3) 抓取面确认（edge 9154 必须是 up，否则 edge 规则组恒不触发）
curl -s 'http://127.0.0.1:9095/api/v1/targets?state=active'
```

**已知缺口**：`TRANSPORT_DOWNLINK_FAILED` 是错误 reason（`atlas/errors/class.go`）而非指标名，
服务端无计数器（帧引擎的 `transport/metrics` 全局实现恒为 `NoopMetrics`），故无法写 Prometheus
规则；替代观测路径是 `atlas-stage3` 面板的 Loki 日志面板。要变成可告警指标需在框架或
`services/edge` 打点（见 `rules/atlas-alerts.yml` 顶部说明）。

## 指标命名表（唯一口径，代码与面板同源）

| 指标 | 类型 | 标签 | 产生点 | 含义 |
|------|------|------|--------|------|
| `gateway_requests_total` | Counter | `op`（路由表 operation）、`result`（success/error） | `services/gateway`（自留会话接口 + 业务 op 透传共用） | 全部入口 QPS 与失败率；面板按 `op` 汇总 |
| `gateway_push_total` | Counter | `op`、`result` | `services/gateway` 推送下发（含挤下线） | 下行推送量与失败率 |
| `actor_messages_total` | Counter | `kind`（ask/tell）、`result` | actor 运行时（框架 `contrib/actor/core`） | 单机消息吞吐与失败 |
| `actor_mailbox_depth` | Gauge | `actor_type` | actor 运行时 | 邮箱积压（配合 M4 背压观察） |
| `actor_remote_delivery_total` | Counter | `result`（ok/404/error） | actor 集群（框架 `contrib/actor/cluster`） | 跨节点投递与 404 重路由 |
| `battle_frames_total` | Counter | 聚合（不按 battle_id，避免高基数） | `services/battle` | 帧推进速率 |
| `admin_audit_finalize_failed_total` | Counter | 无标签（聚合） | `services/game` 管理面（`internal/biz/handler/admin_audit.go`） | 审计收尾（写终态）失败次数：记录停在 PENDING，同键重试报「操作在途」，需人工按 `created_at` 核查补单；恒为 0 才是健康 |
| `edge_streams_active` | Gauge | — | 框架 `contrib/edge`（`metrics.go`） | 接入层活跃流数（并发流上限保护；`edge.max_streams` 是配置不是指标，阈值写死在规则里） |
| `edge_bytes_total` | Counter | — | 同上 | 双向累计转发字节 |
| `edge_ticket_rejected_total` | Counter | `reason`（`ticket_invalid`/`ticket_expired`/`backend_unavailable`/`hello_malformed`/`rate_limited`/`stream_limit`） | 同上 | 新流拒绝计数 |
| `edge_streams_teared_down_total` | Counter | `reason`（`backend_closed`/`client_closed`/`idle_timeout`/`probe_failed`/`rehello`/**`takeover`**/`drain`/`owner_changed`） | 同上 | 拆流计数 |
| `edge_probe_failures_total` | Counter | — | 同上 | 数据报面探活失败计数 |
| `battle_reconnects_total` | Counter | — | `services/battle`（`internal/actor`） | 掉线窗口内回座计数 |
| `battle_offline_timeouts_total` | Counter | — | 同上 | 掉线超时判负计数 |
| `battle_online_players` | Gauge（**拉取式**） | — | `services/battle`（`internal/app/graph.go` 登记，值取自直连注册表） | 本节点直连在册玩家数；拉取式保证急停/重启不漂移 |

约定：

- 指标名与标签名不出现业务实体 ID（避免高基数）；需要定位时用 trace（`trace_id`）关联。
- `op` 标签取值必须来自**生成的路由表**（`relay.RouteEntry.Operation`）或**会话协议描述符**
  （`gatewayv1opclient.SessionProtocolOps`），禁止手抄字面量（否则面板与路由表漂移）。

## 面板位（M7 预留，仍未接线）

> 阶段 3 的面板已落地（见上方「阶段 3 落地」）；下表是 M7 剩余预留位。

| 面板 | 口径 | 依赖 |
|------|------|------|
| 入口 QPS / 失败率 | `sum(rate(gateway_requests_total[1m])) by (op, result)` | 现有指标即可 |
| 推送时延与失败 | `rate(gateway_push_total[1m])` + trace 关联 | 现有指标即可 |
| actor 积压与背压 | `actor_mailbox_depth` + 拒绝计数 | M4 背压中间件 |
| 限流命中 | 限流拒绝计数（按 `op`） | M3 限流中间件 |
| 管理面审计 | GM 动作计数与失败率（按 `action`） | P7 管理面 |
