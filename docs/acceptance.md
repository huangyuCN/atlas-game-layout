# 验收清单核对表

对照设计规格（Atlas 仓库 `docs/superpowers/specs/2026-08-14-atlas-game-layout-design.md`）§10
六条验收标准逐项核对。状态：✅ 达成 / ⚠️ 部分达成 / ❌ 未达成。

## 1. 开箱即用 ✅

> `atlas new 项目 -r atlas-game-layout` → `make compose && make run-all` → `scripts/e2e`
> 走通 注册→登录→匹配→战斗→结算

- `atlas new -r` 生成兼容 `services/*` 布局（M9）：Atlas 集成测试
  `TestE2E_GameTemplateBuilds` 真实模板生成 → 五服务 `go build ./...` 通过
- `make compose`（`deploy/docker-compose/compose.yaml`）与 `make run-all` 本地实测
- `scripts/e2e` 双客户端闭环 standalone 五进程连续多轮通过（M8）

## 2. 只订协议即可开发 ✅

> 新增协议 = 改 proto → `make proto` → 填生成桩，不改装配/路由/gateway

- 分层落地（M3–M7）：协议生成代码自动注册服务端，业务只填 `internal/biz/handler`
  并在 `assemble` 挂载；路由/会话/推送无需改动
- `make proto` 全家桶（go/grpc/http/tcp/udp/kcp/ws/errors/openapi）全链路重跑无多余 diff（M9 验证）

## 3. CLI 可扩展 ✅

> `atlas add actor/locator/lockstep` 模块可直接接入

- 游戏模板多服务布局下 `atlas add actor <entity> -s <service>`（`--service` 为游戏布局必填）：
  模块生成到 `services/<service>/internal/<pkg>/`，可编译，并输出接入提示
  （`<pkg>.Module` 追加到 `bootstrap.Assemble` 的 extra `fx.Option` + import 路径）
- 非游戏布局保持 Kratos 风格根 `internal/<pkg>` 生成，行为不变
- 验证：Atlas 集成测试 `TestAdd_GameLayoutService`（生成 + 编译 + 缺 `--service` 报错）、
  `gamelayout` 布局识别单测、命令行端到端实测（生成 → `go build ./services/...` 通过）

## 4. 双端形态 ✅

> 双形态（TCP 业务通道 + KCP 直连帧面）与（WS 业务通道 + WS 直连帧面）均有示例并通过闭环

> 原始设计表述为「双通道（TCP+KCP）与单通道（WS）」；阶段 3（2026-09-29）起这里的「通道」指
> **客户端业务通道形态**：战斗帧一律直连接入层 → battle 帧面，网关不再有第二条（KCP）战斗连接。

- `scripts/e2e -mode dual`（TCP 业务 + KCP 直连帧面）与 `-mode single`（WS 业务 + WS 直连帧面）均闭环通过（M8）
- 其余形态同批验证：`party` / `fault` / `kick` / `freeze` / `direct`（`direct` 不经接入层，直连 battle 帧端口）
- 传输面集成测试全绿（`test/e2e/transports_test.go` 的 tcp/ws/http + 直连 KCP/UDP 帧面）

## 5. gateway 分布式 ✅

> 双 gateway 实例下，跨实例挤下线与跨实例推送均通过 e2e 专项

- `TestE2ECrossGatewayKick`：跨实例挤下线 + 旧令牌失效 + PlayerActor 存活
- `TestKickCrossInstance` / `TestPushOnlyOwnerDelivers`：跨实例挤下线与「仅持有连接的实例下发」推送专项

## 6. CI 绿 ⚠️ 部分达成

> 模板仓库单测 + 集成测试持续通过

- 本机 CI 同款检查全绿：`gofmt -l` 无输出、`go vet ./...`、`go test ./...`、`make build`
- `.github/workflows/ci.yml` 已定义：gofmt / vet / 单测 / 构建 + SSH 集成 job（10.10.9.36，
  常驻 etcd/redis/nats + 一次性 mongo）
- 留白：GitHub 实跑待仓库推送远端；本轮集成服务器 SSH 不可达（连接被对端关闭），
  集成 job 未实跑验证

## 7. 阶段 3 增量：战斗帧直连（2026-09-29）✅

> 成局后客户端凭「接入层地址 + 战斗票据」直连接入层，接入层按 `battle_id` 查目录选属主并 L4
> 转发到 battle 帧面；战斗帧不再经网关（规格与计划见 Atlas 仓
> `docs/superpowers/specs|plans/2026-09-29-stage3-direct-battle-connect-*.md`）。

- **三面直连闭环**：KCP / UDP / WS 三个帧面各自跑通「hello → JoinBattle → SendFrameInput → 收帧广播」；
  接入层回环由 SDK 直连回环用例覆盖（模板脚本的 `direct` 形态不经接入层）
- **否定例**：过期票 / 被篡改票 / 非参战玩家 / 错误 `battle_id` 均被拒——接入层断开并计
  `edge_ticket_rejected_total{reason}`，battle 帧面回结构化错误（`BATTLE_TICKET_INVALID`/`BATTLE_TICKET_EXPIRED`）
- **网关瘦身断言**：`TestE2EGatewayRejectsBattleOp`（经网关发战斗 op 明确失败，非静默）+
  `TestE2EGatewayNoBattleFrameListeners`（网关不再监听 KCP/UDP）
- **掉线与重连**：`TestE2EBattleOfflineTimeout` / `TestE2EBattleReconnect`（窗口内回座补帧一致，超时判负结算）
- **服务器 e2e**：`go test -count=1 ./test/e2e/` 22 通过 + 1 跳过（`TestDaemonProbe` 未设
  `ATLAS_DAEMON_ADDR`）；`scripts/e2e` 六种形态（dual/single/party/fault/kick/freeze）+ `direct` 全部闭环
- **待办**：`scripts/loadtest` 仍按旧「网关 KCP 战斗通道」驱动，**当前不可用**，待阶段 3 批次 8 改写为
  直连驱动；迁移（rebalance/rollout）、网络损伤、压测与 ADR 同属批次 7/8 待办
