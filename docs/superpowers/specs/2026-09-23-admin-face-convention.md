# 管理面（Admin Face）契约规范

> 上游：子计划 `docs/superpowers/plans/2026-09-23-v2-p7-admin-face.md`（P7）、统一设计 v2 §2 支柱 P7 / §5 决策 4·7、跨子计划裁决 **R12**。
> 打样实现：契约 `api/admin/game/v1/admin.proto`；实现 `services/game/internal/biz/handler/admin.go`（薄：校验 + 幂等 + 审计 + 转发）；
> 审计落库 `services/game/internal/data/{models,repo}/audit.go`；GM 工具 `pkg/gm`（SDK）+ `cmd/gmctl`（命令行）。
> **本文是「新增管理面」的照抄模板**：新服务/新域按 §5 清单逐项落地即可，任一节都不得跳过。

## 1. 信任边界与三条铁律

管理面是**内网 GM/运维面**，与边缘（Edge）/域（Domain）面并列的第三个面，信任边界自内向外：

```
接入层 Edge（客户端可达）  api/gateway/v1/session.proto + 域 CLIENT op
        │ 网关透传（路由表 + actor 寻址）
域面 Domain（服务端内部）   api/game/v1/player_service.proto 的 ACCESS_INTERNAL op
        │ 实现 = 聚合根单写者（PlayerActor）→ biz.PlayerService / biz.PlayerStateAccess
        │ ▲ 管理面**不绕过**域面：只调用 biz.PlayerStateAccess（→ 聚合根）
管理面 Admin（内网 GM/运维）api/admin/<domain>/v1/admin.proto —— **仅** internal listener
        GM 工具（pkg/gm / cmd/gmctl）→ AdminHandler：校验 + 幂等 + 审计 + 转发
```

| 铁律 | 内容 | 违反的后果 |
|------|------|-----------|
| ① 只做「校验 + 幂等 + 审计 + 转发」 | 业务写一律经域面的 INTERNAL op（→ 聚合根单写者）；**禁止**在管理面直连 mongo/redis 改业务数据，**禁止**直连存储改档 | 绕过聚合根 → 与域面并发写冲突、状态机被破坏、审计与实际不一致 |
| ② 只挂 internal listener | 注册点只出现在 internal 面的注册路径；edge 面**零**管理面方法 | 管理面被客户端可达 = 任何人可发道具 |
| ③ 注释必写三段 | 用途 / 边界 / 鉴权前提（模板见 §4） | 后来者误以为「有鉴权」「可以直接改档」 |

**鉴权前提（必须写进注释与本文，R12 定稿）**：本轮内网明文、**无任何鉴权**，且**不加** IP 白名单 / 内网 ACL；`operator` 由调用方自报、**可伪造**——审计里的「谁」只在「内网可信」假设下成立。任何能连到 internal listener 的进程都能发道具。**将来必须补**（三步）：① internal listener 端口隔离 + mTLS（`server.grpc.tls.client_auth` 字段已具备）；② 接授权服务（M11）后由鉴权主体**注入** `operator`，请求里的 `operator` 被覆盖或直接拒绝（**不得再信任请求字段**）；③ GM SDK 的 `operator` 改为凭据/令牌派生，`gm.WithOperator` 收窄为内部调试用。第 ② 步之前若内网边界发生变化（跨机房、VPN 收敛、云上同 VPC 混布），须重新评估。

## 2. 目录与服务命名

```
api/admin/<domain>/v1/admin.proto                契约（无 atlas.route.v1、无 google.api.http 注解）
api/admin/<domain>/v1/doc.go                     包注释（首词 Package <pkgname>）
services/<svc>/internal/biz/handler/admin.go     实现（可拆 admin_audit.go；单文件 ≤500 行、单函数 ≤50 行）
services/<svc>/internal/data/models/audit.go     审计记录模型（mongo 集合 admin_audits）
services/<svc>/internal/data/repo/audit.go       审计仓储（幂等真源 + 游标分页）
pkg/gm/                                          GM SDK（拨号/信封/幂等键/审计翻页）
cmd/gmctl/                                       GM 命令行（stdlib flag，JSON 输出；`main_test.go` 用真实 gRPC 打本机随机端口）
```

| 约定 | 取值 | 说明 |
|------|------|------|
| 包名 | `admin.<domain>.v1` | proto `package`；Go 包名 `admin<domain>v1`（`api/admin/game/v1` → `admingamev1`） |
| 服务名 | `AdminService`；同仓多域时用 `<Domain>AdminService` | 避免多域合并进同一注册表时同名冲突（**未决**，见 §7） |
| 统一信封 | `AdminContext context = 1` | 每个请求消息的**第一个字段**，见 §3.1 |
| 读模型 | 管理面**自带**消息（如 `BackpackItem`），不复用域 proto | 契约不寄生在域 proto 上：域 proto 演进不应牵动管理面；handler 做一次显式字段映射 |
| 注解 | **不加** `atlas.route.v1`、**不加** `google.api.http` | 加了会产出 actor 桩 / 客户端 SDK / 对外 REST 路由，等于把管理面暴露给客户端 |
| 生成物 | 只要 `--go_out` + `--go-grpc_out` + `--openapi_out` | 标准 gRPC 面（不经网关通道）；隔离靠 Makefile 变量，不指望插件自行跳过（§5.1） |

## 3. 契约约定

### 3.1 统一信封 `AdminContext`

```proto
message AdminContext {
  string operator = 1;         // 操作者标识（GM 账号/工具名）；将来由鉴权主体注入
  string idempotency_key = 2;  // 写操作必填：请求级幂等键（GM 重试安全，唯一索引兜底）
  string reason = 3;           // 工单/纠纷单号或说明（审计可读性）
  bool dry_run = 4;            // true = 只校验不改档（仍写审计，但不占用幂等键）
}
```

- 每个请求消息都带 `AdminContext`，字段号固定为 1；新增信封字段只能追加，不复用旧号。
- 读操作**不要求**幂等键（`QueryXxx` 不带）；写操作**必填**（服务端校验，缺即 `AdminIdempotencyKeyMissing`）。
- 客户端 SDK 侧先拦缺 `operator`（`pkg/gm` 在发请求前返回 `errorv1.ErrAdminOperatorMissing`），避免产生无主审计；服务端仍须独立校验（客户端不可信）。

### 3.2 幂等键（写操作）

| 场景 | 服务端行为 | 回执 / 错误 |
|------|-----------|------------|
| 首次请求 | 审计占位（`PENDING`）→ 执行 → 收尾 `SUCCESS`/`FAILED` | `audit_id` + `applied` + `replayed=false` |
| 同键**同参**重复 | 读首次记录，直接返回首次结果，**不重复执行** | `replayed=true`、`applied` 取首次值 |
| 同键**改参** | 拒单 | `AdminIdempotencyKeyReused`（409，10xx 段） |
| 同键且首单仍 `PENDING` | 拒单（不并发执行、不覆盖） | `AdminOperationInFlight`（409） |
| `dry_run=true` | 只校验 + 存在性检查，收尾 `SKIPPED_DRY_RUN`，**不写 `idempotency_key`** | 不占键 → 随后可用**同键**真发放 |
| 指定 `WithIdempotencyKey`/`-idempotency-key` | 原样透传 | 重试（含 SDK 自动重试）必须复用同一个键 |

- 幂等去重真源是**唯一稀疏索引** `uniq_idempotency_key`（见 §5.3），不是「先查后写」——并发下唯一索引才是不变量。
- 幂等键的生成者是调用方（GM 工具/SDK），服务端只做去重；缺省由 `pkg/gm` 用 `lib/idgen.New("gm")` 生成 `gm-<32位hex>`。
- **同一次调用内的重试必须复用同一个键**（SDK 已保证：键在发起调用前生成一次，重试只重发请求对象）。跨调用想复用同一逻辑操作时，显式传同一个键。

### 3.3 dry-run

- 语义 = **只做参数校验（含单次上限）+ 存在性检查**，不写业务数据；仍写一条审计（`dry_run=true`、`result=SKIPPED_DRY_RUN`）。
- **边界（必须写进注释）**：dry-run **不做并发竞争预演**（预演不等于预留，TOCTOU 仍存在）——「预演通过」**不等于**「一定成功」，GM 不得据此当作预留或成功凭证。
- dry-run 不占用幂等键，故不能用来「防重」；同键的 dry-run 可以重复执行。

**存在性口径（apply 与 dry-run 同源，2026-09-25 补记）**：两条路径都**经在线玩家 actor**
只读查询聚合根内存快照（`biz.PlayerStateAccess.GetPlayer` → `PlayerActor.GetPlayer`），
**都不读库**，因此：

- 玩家离线或尚未加载 → `PLAYER_NOT_ONLINE`（1007）——它**不区分**「不存在」与「存在但离线」；
- **dry-run**：把该原因写进回执 `violations`（dry-run **不算失败**，不报错；审计记
  `SKIPPED_DRY_RUN`，不落 reason/message），GM 据此判断"本次预演是否通过"；
- **apply**：把该错误直接上抛，审计记 `FAILED`（含 reason/message）。

**离线玩家发放**（给离线玩家补发/邮件补偿）不在本轮范围，归后续邮箱/outbox 能力；在那之前
GM 工具应把 `PLAYER_NOT_ONLINE` 读作「目标不在线，本次未发放（审计已记失败）」。
`PLAYER_NOT_FOUND` 仅在"actor 加载成功但档案缺失"这类异常态出现，正常链路不可达。

### 3.4 分页（所有列表接口统一，游标 token）

| 约定 | 取值 |
|------|------|
| 请求字段 | `uint32 page_size = N; string page_token = N+1;`（`AdminContext` 之后） |
| 首页 | `page_token` 为空 |
| `page_size` | 缺省 20、上限 200；超上限报 `InvalidParams`（**不静默截断**） |
| 末页 | `next_page_token` 为空 |
| 排序键 | 稳定且唯一（如 `created_at` 降序 + `_id` 兜底）——不稳定排序 + 游标会出现漏读/重读 |
| token | **不透明游标**，调用方不得解析、不得跨接口复用、不得自己拼 |
| 客户端 | 迭代器按 `next_page_token` 翻页，末页（空 token）停止；**空页但 token 非空要继续翻**（不得提前停止）；服务端返回未推进的同一 token 视为违约，客户端中止翻页（`pkg/gm.ErrStuckCursor`） |

### 3.5 批量（本轮只定规范、打样未实现）

写操作需要批量时，统一形态：`<Action>BatchRequest{ AdminContext context; repeated Entry entries; }`。

- **请求级一个幂等键**（不是逐项一个）；整单一条审计记录，逐项结果写入审计 `detail`。
- 回执 `repeated EntryResult results`（逐项成败与 reason）；**部分成功不回滚**（调用方按逐项结果补单）。
- 上限建议 100，超限报 `InvalidParams`；批内单项的失败不得改变整单的审计结果语义（整单按「有失败即 `FAILED` + 逐项 detail」记）。

### 3.6 错误码复用

- 管理面专用 reason 占 **10xx 段**（`api/error/v1/errors.proto`）：`AdminOperatorMissing=1012[400]`、`AdminIdempotencyKeyMissing=1013[400]`、`AdminIdempotencyKeyReused=1014[409]`、`AdminOperationInFlight=1015[409]`、`AdminGrantCountExceeded=1016[400]`；**1012–1099 预留给管理面**（新增只能用这一段的下一个空号）。
- 其余场景**复用**现有通用 reason，不新造同义码：参数非法 `InvalidParams`、目标不存在 `PlayerNotFound`、内部错误 `Internal`。
- 判定一律用生成物 `errorv1.Is<Reason>(err)` / `Reason<Reason>()`，**不得**在业务代码里比较 reason 字面量或 magic number（`cmd/protoc-gen-atlas-errors` 产出 `errors_errors.pb.go`）。
- 校验失败（缺 operator/幂等键、超上限、参数非法）**一律拒单**：不写审计、不改档（唯一例外是业务执行失败——那要写 `FAILED` 审计）。

### 3.7 审计（谁、何时、对谁、做了什么、结果如何）

- 一次管理面**写**请求一条记录（`dry-run` 也留痕）；**成功与失败都写**；含 `trace_id`（链路可追）。
- 审计占位失败 → **拒单**（不写审计就不改档，硬要求）；收尾失败 → 记 ERROR 日志 + 指标 `admin_audit_finalize_failed_total`，保留 `PENDING`。
- 字段与结果状态见附录 A；`params` 只落脱敏 + 截断后的摘要，`params_hash` 用于「同键改参」判定，**不落敏感原文**。

## 4. 注释必写三段（照抄模板）

新增管理面时，proto 文件头与 Go 实现（handler / SDK）**必须**写这三段；缺任一段视为未完成。

### 4.1 proto 文件头模板

```proto
// 用途：<服务/域> 的 GM/运维管理面（<动作清单>）；仅限内网管理工具调用，
//   不经网关、不暴露给客户端。
// 边界：本面只做「校验 + 幂等 + 审计 + 转发」，业务写仍收敛到域面
//   （<域 service> 的 ACCESS_INTERNAL op → <聚合根> 单写者）；
//   禁止在本面直连 mongo/redis 改业务数据，禁止生成 actor/HTTP/客户端 SDK 产物
//   （由 Makefile 的 API_ADMIN_PROTOS 隔离：只进 --go_out/--go-grpc_out/--openapi_out）。
// 鉴权前提：本轮内网明文、无鉴权、**不加 IP 白名单/ACL**（R12），operator 为调用方自报、可伪造；
//   将来接外部服务必须由鉴权主体注入 operator（请求字段被覆盖或拒绝）并启用 mTLS——补法见 §1。
syntax = "proto3";
```

### 4.2 Go 实现注释模板

```go
// Package handler 里的管理面实现（admin.go）：
// 用途：<域> 的 GM/运维管理面服务端实现（发道具、查玩家/背包/审计）。
// 边界：只做校验 + 幂等 + 审计 + 转发；业务写经 biz.<Xxx>Access 收敛到聚合根，
//   本层不直连 mongo/redis 改业务数据、不复制域面业务规则。
// 鉴权前提：本轮内网无鉴权且不加白名单/ACL（R12），operator 由调用方自报、可伪造；
//   将来由鉴权主体注入 operator（见 docs/superpowers/specs/2026-09-23-admin-face-convention.md §1）。
```

方法级注释同样要说清「副作用在哪里发生」（例如：「本方法只写审计，业务写由 biz.PlayerStateAccess 转发」）。

## 5. 新增管理面照抄清单

按顺序做，每步都有可验证的产出。

### 5.1 契约与 Makefile 隔离

1. 新增 `api/admin/<domain>/v1/admin.proto`（§2 命名 + §4.1 注释三段）+ `doc.go`（首词 `Package …`）。
2. `Makefile` 的 `API_ADMIN_PROTOS` 追加该 proto；确认它**只**出现在三处调用行：`--go_out`、`--go-grpc_out`、`--openapi_out`。
   **绝不**进 `--atlas-http_out` / `--atlas-actor_out` / `--atlas-client_out` / 四传输插件调用行——`atlas-http` 在 `omitempty=false` 时会为无注解 service 产出兜底 REST 路由，隔离必须由 Makefile 保证，不依赖插件默认行为。
3. `make proto` 后做负面断言：产物只有 `admin.pb.go` / `admin_grpc.pb.go` / `doc.go`；**不得**出现 `*_http.pb.go` / `*_actor.pb.go` / `*_client.pb.go` / `*_rpc_adapter.pb.go`（打样断言见 `api/admin/game/v1/admin_generation_test.go`）。
4. 生成一致：`make proto && git status --porcelain api/` 无 diff。

### 5.2 错误码

1. `api/error/v1/errors.proto` 在管理面号段（1012–1099）追加所需 reason + `[(atlas.errors.code) = <HTTP 码>]`。
2. `make proto` 重生成 `errors_errors.pb.go`；在 `api/error/v1/errors_test.go` 补断言（code / reason / `biz_code` metadata / `Is*` 判定）。
3. 复用优先：能用 `InvalidParams`/`PlayerNotFound`/`Internal` 表达的场景不要新增码。

### 5.3 审计模型与索引

1. `services/<svc>/internal/data/models/audit.go`：`AuditRecord`（字段见附录 A）+ 结果状态枚举（`AuditResult`：`PENDING`/`SUCCESS`/`FAILED`/`SKIPPED_DRY_RUN`，**用常量集合，杜绝魔法字符串**）。
2. `services/<svc>/internal/data/repo/audit.go`：`AuditRepo` 接口（`Insert` / `Finalize` / `FindByIdempotencyKey` / `List`（游标分页） / `EnsureIndexes`）+ mongo 实现；唯一索引冲突归一为可判定的「幂等命中」错误，**不是 500**。
3. 集合 `admin_audits` 的索引（启动期 `EnsureIndexes`，幂等）：

   | 索引名 | 键 | 选项 | 用途 |
   |--------|----|------|------|
   | `uniq_idempotency_key` | `{idempotency_key: 1}` | **unique + sparse** | 幂等去重真源；dry-run 不写该字段，故**必须稀疏**（否则多条缺字段文档互撞） |
   | `idx_created_at` | `{created_at: -1}` | — | 列表分页排序键 |
   | `idx_target_created` | `{target_type: 1, target_id: 1, created_at: -1}` | — | 按目标追查 |
   | `idx_operator_created` | `{operator: 1, created_at: -1}` | — | 按人追查 |

4. `pkg/mongo.Index` 需支持 `Sparse bool`（`EnsureIndexes` 调 `SetSparse`）；缺省 `false` = 行为不变。文档字段 `idempotency_key` 必须 `omitempty`（dry-run 不落字段）。

### 5.4 单次上限配置项与校验位置（R12：本轮做）

1. **配置**：服务自有配置放本服务 `services/<svc>/internal/conf/conf.proto`（新增 `Admin{ uint32 max_grant_count = 1 }` 与 `Bootstrap.admin = N`）+ `services/<svc>/configs/config.yaml` 的 `admin.max_grant_count`。约定：缺省或 `<=0` → 回退默认 **100**，**不提供「关闭上限」**的取值（`biz.DefaultMaxGrantCount` / `biz.NormalizeMaxGrantCount`）。
2. **注入**：经 `biz.AdminOptions{MaxGrantCount: …}`（装配点见 §5.5）注入 handler，不在 handler 里读配置。
3. **校验位置（钉死）**：**审计占位之前**——`count > max` 立即拒单返回 `AdminGrantCountExceeded`：底层 `PlayerStateAccess` **零调用**、审计**零写入**（超限不是「失败的写操作」，不该留 FAILED 审计）。`dry_run=true` **同样受限**。
4. 用例（先红后绿）：`count=max+1`（含 `dry_run=true`）→ 超限 + 无副作用 + 无审计；`count=max` 通过；配置缺省/0 → 上限 100。

### 5.5 fx 接线点与 listener 注册

| 接线点 | 做什么 |
|--------|--------|
| `services/<svc>/internal/app/data.go` | 提供审计仓储（`newMongoAuditRepo` → `*repo.MongoAuditRepo`，启动期建索引）；在 `providers` 里同时以接口形式提供（`fx.As(new(repo.AuditRepo))`） |
| `services/<svc>/internal/app/graph.go` | 把上面的 provider 登记进依赖图（打样：`newMongoAuditRepo` 一行 + 注释指向管理面） |
| `services/<svc>/internal/app/biz.go` | 注入 `biz.AdminOptions`（上限、审计号生成器、时钟）并装配 `NewAdminHandler`（→ `biz.AdminService`） |
| `services/<svc>/internal/server/server.go` | **只**在 **internal 面**注册：用 `serverutil.GRPCFaces.Internal`（`GRPCFaces{Edge, Internal}` 的 internal 成员）调 `admingamev1.RegisterAdminServiceServer(faces.Internal, handler)`；edge 面注册路径**零**管理面 |

**安全边界（必须核验，不是靠约定）**：

- `grep -rn "RegisterAdminServiceServer" --include=*.go .` → 只应命中 internal 注册路径（打样：`services/game/internal/server/server.go`）。
- 用 **edge listener 地址**连接调 `AdminService/GrantItem` → `codes.Unimplemented`；用 internal 地址 → 正常（e2e 用例之一，见 §5.7）。
- 若因排期临时把管理面挂在非 internal listener 上：**只允许临时态**，且必须在同一原子窗口内迁到 internal，**不得作为终态交付**。

### 5.6 GM SDK 与命令行

1. `pkg/gm`（SDK）：`New(grpc.ClientConnInterface)` / `NewWithClient(AdminServiceClient)` / `Dial(ctx, endpoint)`；选项 `WithOperator` / `WithIdempotencyKey` / `WithReason` / `WithDryRun` / `WithTimeout` / `WithRetry`；四个方法 `GrantItem` / `QueryPlayer` / `QueryBackpack` / `QueryAudits`（返回迭代器 `Iterator`：`Next`/`Close`，`io.EOF` 结束）。
2. **客户端侧先拦缺 `operator`**（发请求前返回 `errorv1.ErrAdminOperatorMissing`），但不替代服务端校验。
3. 新增域时按域生成 SDK 或扩展现有 `pkg/gm`（多域共用一个「信封 + 幂等键 + 翻页」内核，别复制粘贴）。
4. `cmd/gmctl`（命令行）：**stdlib `flag`**，不引 cobra 等新依赖；子命令 `grant-item` / `query-player` / `query-backpack` / `query-audits`；回执 JSON 到 stdout（错误到 stderr、退出码 1）；`-operator` 缺省读环境变量 `GM_OPERATOR`，都没有则报错提示；`Makefile` 增 `build-gmctl`（产物 `bin/gmctl`）。
5. 输出语义：按 protojson 规则（字段用 proto 原名、零值字段也输出、**64 位整数以字符串**下发如 `"created_at": "1712345678901"`——脚本消费时注意类型）；全局选项（`-addr`/`-operator`/`-timeout`/`-dry-run`/`-reason`/`-idempotency-key`）写在**子命令之前**。

### 5.7 e2e 用例（每条都要有）

| 用例 | 断言要点 |
|------|---------|
| 发放成功 | `applied=true`、`replayed=false`、`audit_id` 非空；随后查背包数量正确 |
| 审计可查 | 管理面 `QueryAudits(target_id=…)` 投影字段齐全（`operator`/`action`/`result=AUDIT_RESULT_SUCCESS`（协议 enum 名，见 §附录 A）/`trace_id`/`created_at`）；**并用 mongo 直查** `admin_audits` 断言 `params_hash`/`finished_at≠0`/`idempotency_key` |
| 幂等 | 同键同参连发两次：第二次 `replayed=true`、数据不叠加、该键仅 1 条 `SUCCESS` |
| dry-run 不落库 | 背包不变、审计 `dry_run=true`/`SKIPPED_DRY_RUN`、mongo 记录**无** `idempotency_key` 字段；随后**同键**真发放成功（证明未占键） |
| 失败也审计 | 对不存在目标发放 → `PlayerNotFound` + 审计 `FAILED` + `reason` 落库 |
| 单次上限 | `count=max+1`（含 `dry_run=true`）→ `AdminGrantCountExceeded`、无副作用、**无审计**；`count=max` 成功 |
| **安全边界** | edge 地址调 `AdminService` → `codes.Unimplemented`；internal 地址 → 正常；配置里 internal 面未启用时管理面整体不可达 |

### 5.8 静态门禁

```bash
gofmt -l .                 # 0 输出
make lint                  # 包名前缀 + Go doc 注释 + 重复代码，0 违规
go build ./... && go vet ./...
go test -count=1 ./pkg/gm/ && make build-gmctl && ./bin/gmctl -h
```

另加两条负面断言（防管理面泄漏给客户端）：

```bash
ls api/admin/<domain>/v1/*_http.pb.go api/admin/<domain>/v1/*_actor.pb.go   # 应不存在
grep -rn "RegisterAdminServiceServer" services/<svc>/internal/server/       # 只应命中 internal 面
```

## 6. 本轮不做（R12 定稿，不得悄悄做一半）

| 项 | 本轮状态 | 说明 |
|----|---------|------|
| 鉴权 / 白名单 / ACL | **不做** | 内网前提；只靠端口隔离 + 事后审计追责。补法见 §1（三步） |
| 批量接口（`BatchGrantItem` 等） | **只写规范，不实现** | 形态见 §3.5；打样 `GrantItem` 为单目标 |
| 审计 `PENDING` 扫尾任务 | **不做** | 保留 `PENDING` 语义：收尾失败的行不自动清理；同键重试报 `AdminOperationInFlight`（409），超时 `PENDING` 由运维按 `created_at` 手工核查 |
| GM 权限细分（谁能发什么 / 每日总量 / 道具白名单） | **不做** | 只有单次上限（§5.4）+ 事后审计；建议与 M11 授权一起做，避免两套判定 |
| 独立审计库 / 归档 / 合规保留期 | **不做** | 与业务同库（`mongo.database`）；跨服务统一审计查询入口只写规范不实现 |
| dry-run 并发预演 / 预留 | **不做** | 语义边界见 §3.3 |

**`PENDING` / `AdminOperationInFlight` 的既定语义（写实现时必须遵守）**：审计占位成功即进入 `PENDING`；业务执行后**必须**收尾为 `SUCCESS`/`FAILED`/`SKIPPED_DRY_RUN`（`defer` 内写，成功失败都写）。若进程在收尾前崩溃/被杀，记录**永久停在 `PENDING`**（本轮不扫尾）——此时同键重试一律 `AdminOperationInFlight`（**不重放、不覆盖**），需要人工按 `created_at` 核查后补单（换新键）。

## 7. 风险与未决

1. **鉴权后置**：见 §1；本轮所有「谁」的结论都建立在「内网可信」上。
2. **服务命名**：`AdminService` 在多域合并进同一注册表时会同名冲突——建议统一按 `<Domain>AdminService`（打样沿用 `AdminService`，请评审确认）。
3. **审计存储**：与业务同库，量大后争资源；独立库/归档/保留期待业务确认。
4. **稀疏唯一索引**：`uniq_idempotency_key` 依赖 `pkg/mongo.Index.Sparse`；若将来要「dry-run 也防重」或加 TTL/partialFilter，需要 `Index` 再支持 `PartialFilterExpression`。
5. **清理**：`admin_audits` **只增不删**——审计不随回滚/发布清理；确需清除仅限 `db.admin_audits.dropIndex("uniq_idempotency_key")` 这类索引操作。

## 附录 A：审计记录字段与结果状态

| 字段（bson） | 含义 | 备注 |
|--------------|------|------|
| `_id` | 审计号 | 回执给调用方，供补单/纠纷/内审引用（`idgen` 生成） |
| `operator` | 谁 | 本轮调用方自报；将来取鉴权主体 |
| `action` | 做了什么 | 完整 gRPC 方法名（取生成物 `*_FullMethodName`，不手写字符串） |
| `target_type` / `target_id` | 对谁 | 有限集合枚举（如 `player`/`item`） |
| `params` / `params_hash` | 参数摘要 / 摘要哈希 | 脱敏 + 截断；哈希用于「同键改参」判定 |
| `idempotency_key` | 幂等键 | `omitempty`；dry-run **不写**（稀疏索引） |
| `dry_run` | 是否预演 | 与 `result=SKIPPED_DRY_RUN` 配对 |
| `result` | 结果 | `PENDING` / `SUCCESS` / `FAILED` / `SKIPPED_DRY_RUN`（落库值）；协议投影用 enum `AuditResult`（`AUDIT_RESULT_*`，protojson 下发枚举名——**协议状态字段一律 enum**，客户端不解析裸字符串） |
| `reason` / `message` | 失败 reason / 描述 | 成功为空；reason 用 `errorv1` 常量 |
| `trace_id` | 链路关联 | otel span context；「审计能否追到调用链」靠它 |
| `created_at` / `finished_at` | 占位时间 / 收尾时间 | unix ms；`finished_at=0` = 未收尾（`PENDING`） |

| 结果状态 | 何时写 | 终态 |
|----------|--------|------|
| `PENDING` | 审计占位（执行副作用之前） | 否 |
| `SUCCESS` | 业务副作用成功 | 是 |
| `FAILED` | 业务副作用失败（失败也留痕） | 是 |
| `SKIPPED_DRY_RUN` | `dry_run=true`，只校验未改档 | 是 |

## 附录 B：安全边界核验命令

```bash
# ① 注册点唯一：只在 internal 面
grep -rn "RegisterAdminServiceServer" --include=*.go .
# ② 管理面未产出客户端可达产物（0 命中）
ls api/admin/*/v1/*_http.pb.go api/admin/*/v1/*_actor.pb.go api/admin/*/v1/*_client.pb.go 2>/dev/null
# ③ 零词缀门禁不误伤管理面
grep -rnE "ActorServer|OpClient|RPCAdapter" api/admin/ || echo "管理面无词缀命中"
# ④ 生成一致（重生成无 diff）
make proto && git status --porcelain api/
```
