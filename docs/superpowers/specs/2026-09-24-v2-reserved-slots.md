# 统一设计 v2 预留位（M3–M7）

> 上游：框架仓 `docs/superpowers/specs/2026-09-23-unified-design-v2.md`、子计划 `…-v2-p3-template-migration.md` 步骤 10。
> 本文件登记**只留位置、本轮不实现**的能力位；每个位点标注「位置 / 本轮状态 / 下一轮归属」。

| 位点 | 位置 | 本轮状态 | 下一轮归属 |
|------|------|----------|------------|
| **M3 限流** | `api/atlas/v1/route.proto` 的 `RateLimit`（`qps` / `burst` / `scope`）+ rpc 级 `rate_limit` 注解 | 只声明不执行：注解已进路由表（`RouteEntry`），网关与 actor 侧均不读 | 限流中间件按路由表声明在装配期生效（网关入口 + actor 入站各一层） |
| **M4 背压** | `protobuf/configs/server.proto` 的 `Server.GRPC.max_inflight` / `Server.HTTP.max_inflight` | 字段已就位，取值为 0 = 不限制（当前行为）；无读取方 | 背压中间件（在途信号量 + 快速失败）接入 `pkg/middleware`，按服务配置开启 |
| **M5 多端** | `api/gateway/v1/session.proto` 的 `LoginRequest.client_end` / `ResumeRequest.client_end` | 字段只登记不参与裁决：会话行为仍**单端**（新登录顶掉旧会话） | 会话管理器引入端维度（同玩家多端并存、按端踢人与推送路由） |
| **M6 事务性发件箱** | `services/{game,battle}/internal/data/`（outbox 落点，与聚合根同库写入） | 未建表、未接线：结算/审计事件仍为「先落库后发布」的尽力而为 | outbox 表 + 投递器（至少一次 + 消费侧幂等），与 M7 审计动作名对齐 |
| **M7 观测** | `deploy/observability/`（指标命名表与面板占位，见该目录 `README.md`） | 只登记指标口径与面板位；已有 `gateway_requests_total{op,result}` 与 actor 运行时指标实际打点 | 面板与告警规则随管理面（P7）与背压/限流指标一起接线 |

**约定**：预留位一律**不写实现代码**（避免半成品与「配了没生效」）；启用某位点时，同窗口补齐：协议/配置注释更新、消费方接线、验收用例（含"未配置时行为不变"的回归断言）。
