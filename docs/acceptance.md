# 验收清单核对表

对照设计规格（Atlas 仓库 `docs/superpowers/specs/2026-08-14-atlas-game-layout-design.md`）§10
六条验收标准逐项核对。状态：✅ 达成 / ⚠️ 部分达成 / ❌ 未达成。

## 1. 开箱即用 ✅

> `atlas new 项目 -r atlas-game-layout` → `make compose && make run-all` → `scripts/e2e`
> 走通 注册→登录→匹配→战斗→结算

- `atlas new -r` 生成兼容 `services/*` 布局（M9）：Atlas 集成测试
  `TestE2E_GameTemplateBuilds` 真实模板生成 → 四服务 `go build ./...` 通过
- `make compose`（`deploy/docker-compose/compose.yaml`）与 `make run-all` 本地实测
- `scripts/e2e` 双客户端闭环 standalone 四进程连续多轮通过（M8）

## 2. 只订协议即可开发 ✅

> 新增协议 = 改 proto → `make proto` → 填生成桩，不改装配/路由/gateway

- 分层落地（M3–M7）：协议生成代码自动注册服务端，业务只填 `internal/biz/handler`
  并在 `assemble` 挂载；路由/会话/推送无需改动
- `make proto` 全家桶（go/grpc/http/tcp/udp/kcp/ws/errors/openapi）全链路重跑无多余 diff（M9 验证）

## 3. CLI 可扩展 ⚠️ 部分达成

> `atlas add actor/locator/lockstep` 模块可直接接入

- 实测 `atlas add actor <entity>`：生成自编译的 fx 模块（handler/usecase/messages/module），
  可按 `bootstrap.Assemble` 的 extra `fx.Option` 接入任意服务的装配
- 留白：生成落点为仓库根 `internal/`（Kratos 风格），与 `services/<svc>/internal` 分层不一致；
  游戏模板专用装配（actor Props 注册 / 懒激活副本 / 帧传输）需手工搬运，未提供自动接入

## 4. 双端形态 ✅

> 双通道（TCP+KCP）与单通道（WS）均有示例并通过闭环

- `scripts/e2e -mode dual`（TCP 业务 + KCP 战斗）与 `-mode single`（WS 单通道）均闭环通过（M8）
- 五协议传输集成测试全绿（`test/e2e/transports_test.go`，tcp/ws/kcp/udp/http）

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
