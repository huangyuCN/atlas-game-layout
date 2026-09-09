# atlas-game-layout 项目开发指引

## 项目概述

本仓库是面向**游戏单仓**的参考实现与脚手架模板（基于 [Atlas](https://github.com/huangyuCN/atlas)），
以 **gateway / game / matcher / battle** 四服务覆盖 注册 → 登录 → 匹配 → 战斗 → 结算 的完整游戏闭环，
内置五协议接入、actor 集群、lockstep 帧同步、分布式会话与可编程装配（fx）。

**模板使用方式**：
- 用 `atlas new my-game -r https://github.com/huangyuCN/atlas-game-layout` 以本模板生成自己的项目；
- 本仓自身的代码既是**实现**也是**模板内容**——新项目 fork 本仓代码后在此基础上改业务。

**与 Atlas 的关系**：本仓依赖 [Atlas](https://github.com/huangyuCN/atlas)（Go 基础底座）与
[cow](https://github.com/huangyuCN/cow)（业务公共件）；业务层只依赖 Atlas 自身接口与 cow，
Atlas 类型仅在 `pkg/` 装配层使用。`go.mod` 以本地 `replace` 指向 `../atlas` 与 `../cow`
（见文末「依赖约定」）。

---

# Agent 约定（全局）

以下约定与 Atlas 生态其它仓库（[atlas](https://github.com/huangyuCN/atlas) 主仓等）一致，为便于模板使用已全文照抄于此。
若与主仓冲突，以主仓 AGENTS.md 为准。

## Superpowers 文档根目录

- **默认文档根目录**：`docs/superpowers/`
- **禁止**：将文档放到 `.superpowers/`（该目录只允许本地临时产物）
- **其他 Agent/Skill 生成的设计文档**：除 superpowers 文档外，其他 Agent 或 Skill 生成的设计文档也必须放到 `docs/` 目录下（按项目约定选择子目录）。

如需新增：
- 计划写到 `docs/superpowers/plans/`
- 设计写到 `docs/superpowers/specs/`
- 架构决策（ADR）写到 `docs/superpowers/adr/`（约定见该目录 `README.md`）
- brainstorming/草图写到 `docs/superpowers/brainstorm/`
- **经确认的 benchmark 对比日志**写到 `docs/superpowers/benchmarks/`（约定见该目录 `README.md`）

## 开发规范（全局）

- **Go 版本**：使用 **Go 1.26** 开发；杜绝使用已过时/弃用（deprecated）的接口、函数与用法（以 `go doc` / 官方 release notes 为准）。
- **开发位置**：不要在 git worktree 中开发（例如 `.worktrees/`）；直接在当前分支/当前工作目录开发与提交。
- **Go 缓存目录位置**：`go build` / `go test` / `go mod` 等操作产生的构建缓存（`GOCACHE`）与模块缓存（`GOMODCACHE`）**一律放在系统临时目录**（如 `$TMPDIR` 下的专用目录），**禁止在项目根或仓库内创建 `.gocache` / `.gomodcache` 等缓存目录**；任务或会话结束时必须清理。原因：项目根内的大规模缓存目录（成千上万文件）会触发 IDE（IntelliJ IDEA / GoLand）文件监视与索引的海量刷新，导致卡顿甚至整机卡死。若受沙箱等环境限制确需在项目内使用，用完后**必须立即删除**且不得提交。
- **集成/压测服务器**：`10.10.9.36`（SSH 用户 `shimmer-bi`，本机已配公钥免密，`ssh shimmer-bi@10.10.9.36` 直连）。**所有需要外部依赖的测试（etcd / redis / consul / nacos / nats / mongo 等）与压力测试优先在该服务器执行**，不在本地运行。服务器常驻外部依赖容器端口：etcd `12379`、redis `16379`（哨兵 `26379/26380`）、nats `14222`、mongo `27017`。使用方式：将代码同步到服务器（rsync）后在其上执行 `go test`；测试代码中依赖地址按服务器端口填写（如 `127.0.0.1:12379`）。
- **Git 提交与推送**：未经用户**明确同意**，不得执行 `git commit` 或 `git push`。每次提交或推送前须说明拟纳入的变更摘要（或要点）与建议的提交说明，经用户确认后再执行。查看类操作（如 `git status`、`git diff`、`git log`）不受此限。
- **交互语言**：所有对话回复、代码审阅意见、提交说明草稿、文档撰写**一律使用中文**。尽量用通俗易懂的语言表达；减少英文缩写的使用，必须使用时在括号中注明中文释义或英文全称（如「CAS（Compare-And-Swap，比较并交换）」）。代码标识符、命令、路径除外。
- **术语与缩写**：避免使用生涩难懂的术语；如必须使用术语或英文缩写，**首次出现**时需在括号中补充中文释义或英文全称，后续可仅使用缩写。
- **TDD**：采用 **TDD** 开发模式，**测试先行**（先写/更新测试，再实现业务代码，最后重构）。
- **性能与基准测试**：复杂逻辑需要补充 **benchmark**（`*_test.go` 中的 `BenchmarkXxx`）。实现后必须提醒开发者确认性能是否达标（包含关键指标与可复现实验方式）。
- **基准测试结果归档与对比（每次跑完 benchmark 后）**：
  1. 将本次输出与**上一次已归档**（或上一次提交的基线 commit）结果对比，使用 `benchstat old.txt new.txt` 或等价方式生成差异。
  2. 在回复或 PR 描述中用 **Markdown 表格**展示对比（至少含：基准名、前次 ns/op（或 B/op、allocs/op）、本次、相对变化或 `benchstat` 的 `vs base`）。
  3. **询问**使用者是否**保留本次结果**作为后续对比基线。
  4. 若使用者确认保留：将本次对比表及元数据（日期、`go version`、机器/OS、`GOMAXPROCS`、**commit**、完整 `go test -bench=...` 命令）**追加**写入 `docs/superpowers/benchmarks/` 下对应主题的日志文件（约定见该目录 `README.md`）；不得把仅用于临时对比的冗长原始 `.txt` 提交进仓库，除非团队明确要求。
- **规模与复用（新增/修改代码时遵守）**：
  - **单文件**：同一源文件行数**不超过 500 行**（含空行与注释）；若逼近上限，应拆分为多个文件或子包，并保证职责清晰。
  - **单函数**：同一函数**不超过 50 行**（含空行与注释）；超出则拆分为多个函数或提取步骤，避免单块过长。
  - **调用链追踪自检（跨模块/跨服务变更必须执行）**：修改跨模块、跨进程（如集群路由、消息投递、RPC 调用）的逻辑后，**必须**从入口点出发，跟踪完整的调用链并验证每一跳的关键假设（PID 格式、地址映射、消息编解码、端口/主题命名等）是否匹配。仅 `go build` 通过不足以证明链路正确。
  - **公共抽象**：多处重复或可被清晰命名的逻辑，**必须**提取为包内/跨包的**公共函数**（或小型类型与方法），避免复制粘贴；提取时保持命名与现有代码风格一致。
  - **命名不得以包名开头**：包内导出的函数名、类型名、变量名杜绝以包名作为前缀。调用侧在使用时本身带有包名限定（如 `session.NewManager`），若函数名再以包名开头将形成冗余（`session.SessionNewManager`），且会触发 IDE 警告 "Name starts with the package name"。正确做法：`session.NewManager`（而非 `session.SessionNewManager`）。提交前可用 `make lint`（scripts/check-pkgname）自动检查全部导出标识符（函数/类型/变量/方法）是否以包名开头。
- **代码注释语言**：所有手写代码注释必须使用中文，包括 Go doc 注释、行内注释、复杂逻辑说明和测试意图说明。允许保留英文的情况仅限专有协议字段、外部标准名、错误码、指标名、trace attribute 名、第三方 API 原文，以及 protobuf/OpenAPI/工具生成文件中的生成注释。

---

# 项目结构与包说明

## 顶层架构总览

```
atlas-game-layout/              ← Go module: github.com/huangyuCN/atlas-game-layout
├── services/                   ← 四服务（每服务独立可运行）
│   ├── gateway/                ← 接入层：五协议接入 + 分布式会话 + 挤下线 + 下行推送
│   ├── game/                   ← 玩家业务（cow 聚合根 + 三级缓存）
│   ├── matcher/                ← 撮合（matchmaker + 等级相近规则）
│   └── battle/                 ← 战斗（actor + lockstep 会话 + 结算）
├── api/                        ← 协议定义（proto + 生成代码），按服务分目录
├── lib/                        ← 代码级公共定义（无框架依赖）：consts / idgen / session / errors / gametime
├── pkg/                        ← 每服务公共装配（fx.Module 化）：actor / redis / nats / mongo / etcd / registry
│                                / bootstrap（配置+日志+App 组装）/ fxkit（跨服务 fx 泛型）/ serverutil
├── protobuf/                   ← 内部配置 proto（configs/*）
├── deploy/                     ← 中间件 docker-compose
├── scripts/e2e                 ← 双客户端闭环脚本（双形态）
├── scripts/loadtest            ← 帧通道压测（KCP vs WS）
├── test/e2e                    ← 进程内端到端测试（真中间件，不可达自动跳过）
├── third_party/                ← 第三方 proto 依赖
└── docs/superpowers/           ← 设计文档（计划 / 规格 / ADR / benchmark）
```

## 核心架构约定：服务分层与装配

每服务统一「**唯一装配之家**」形态，是模板最关键的架构约束：

```
services/<svc>/
├── assemble/        ← ★ 装配之家：assemble.go 集中列出全部组件清单（fx 模块）
├── cmd/             ← 进程入口（main），仅调用 assemble
├── configs/         ← config.yaml（端口/中间件地址等运行配置）
└── internal/
    ├── app/         ← 进程形态（atlas.App）组装：组件清单 → App 生命周期
    ├── conf/        ← 配置结构体（读取 config.yaml）
    ├── biz/         ← 业务逻辑（usecase + handler：实现生成的服务接口）
    ├── data/        ← 数据访问（repo/models）
    ├── infra/       ← 基础设施接入（如 redis/nats client 装配，gateway/battle 有）
    ├── server/      ← 传输层服务注册（tcp/ws/kcp/grpc/http 等）
    ├── actor/       ← actor 集群接入（game/battle 有）
    └── session/     ← 分布式会话（仅 gateway）
```

**铁律**：
- `internal/{conf,infra,biz,data,server,actor,session}` 只做各自职责，**不含装配逻辑**；
- 所有依赖图（fx 提供者、端口、实例化顺序）**只写在 assemble/app**；
- 进程形态（`atlas.App`）与嵌入式形态（fx 编程式启动）**共用同一张依赖图**（app 与 assemble 同源）；
- 新增依赖/组件 → 只在 assemble.go 声明，不改各 internal 子包间的直接构造。

## actor 方法组织约定

game/battle 的业务 actor（`services/*/internal/actor/`）采用**按域分文件**组织，同一 receiver 跨文件（Go 原生支持）：

| 文件 | 职责 |
|------|------|
| `player.go` / `battle.go` | 类型、装配（NewProps）、生命周期（OnStart/OnStop）、横切钩子 |
| `player_auth.go` / `battle_session.go` | 认证/会话域方法（Register/Login/Logout / Create/Join/Reconnect/GetState） |
| `player_bag.go` / `battle_frame.go` | 背包/帧同步域方法（GrantItem/GetBackpack/GetPlayer / FrameInput/onFrameResult/checkSettle） |

**铁律**：
- 业务 actor **实现生成的 `<Service>Server` 接口**（方法签名 `(ctx core.ActorContext, req *X) (*Y, error)`）；分发由生成桩接管——**不手写 OnTell/OnAsk/switch 分发/decodeEnvelope**；
- 错误**上抛**（`return nil, err`，结构化 error 经集群 error 通道往返），**不包 `Ok:false` 回执**；
- 本地消息（如定时快照 `tickSnapshot`）经 `core.WithLocalTell[T]` 类型路由注册；
- 生命周期（OnStart/OnStop）经生成桩 `DispatchBase` 断言转发；
- 方法按业务域拆文件，单文件 ≤500 行、单函数 ≤50 行。

**新增 actor 接口三步流程**：① proto 加 `rpc` → ② `make proto`（生成桩）→ ③ 在对应域文件实现方法。

## api/ — 协议定义

`api/<svc>/v1/*.proto` 是协议事实源（按服务分目录：gateway/game/matcher/battle/common/error）：

- 改协议 → `make proto` → 生成桩落在同目录（.pb.go / _grpc.pb.go / 各传输生成代码 / openapi）；
- 新增业务接口流程：改 proto → `make proto` → 在 `services/<svc>/internal/biz/handler/` 实现生成的服务接口 → 在 `assemble` 挂载；
- **手写代码与生成代码同目录**：生成文件有明确生成头（Code generated），不得手改；手写文件注释用中文。

## lib/ — 代码级公共定义

无框架依赖的纯定义与工具，供 services/pkg 引用：

| 子目录 | 职责 |
|--------|------|
| `consts/` | 常量（op 名、端口、默认值等）|
| `errors/` | 结构化错误定义 |
| `idgen/` | ID 生成 |
| `session/` | 会话相关纯定义 |
| `gametime/` | 游戏时间工具 |
| `metrics/` | 指标定义 |

## pkg/ — 每服务公共装配

fx.Module 化的可复用装配件，跨服务共享：

| 子目录 | 职责 |
|--------|------|
| `actor/` | actor 集群接入装配 |
| `redis/` `nats/` `mongo/` `etcd/` | 中间件 client 装配（fx 提供者）|
| `registry/` | 服务注册/发现装配 |
| `config/` | 配置加载 |
| `log/` | 日志装配 |
| `bootstrap/` | 配置加载 + 日志 + App 组装（进程形态样板）|
| `fxkit/` | 跨服务 fx 泛型提供器 |
| `serverutil/` | 传输层 Server 装配辅助 |

> 依赖分层：`lib/` < `pkg/` < `services/*/internal/*` < `assemble`。
> `pkg/` 可依赖 Atlas 与 cow（装配层允许）；`lib/` 不依赖框架。

## deploy / scripts / test — 运行与验证

| 目录 | 职责 |
|------|------|
| `deploy/docker-compose/` | 中间件编排（etcd 12379 / redis 16379 / nats 14222 / mongo 27017）|
| `scripts/e2e/` | 双客户端闭环验证（`make e2e`；dual/single 形态）|
| `scripts/loadtest/` | 帧通道压测（`go run ./scripts/loadtest ...`）|
| `test/e2e/` | 进程内端到端测试（真中间件，不可达自动 Skipf 跳过）|

## 常用命令（开发速查）

```bash
# 构建 / 运行
make build                # 构建四服务到 ./bin
make run-all              # 一键起四服务（前台交错输出，Ctrl-C 全部退出）
make compose              # 起中间件（etcd/redis/nats/mongo）

# 测试
go test ./...             # 单元测试（内存/内嵌中间件）
go test ./test/e2e/       # 端到端（需真中间件；不可达自动跳过）
make lint                 # 命名规范检查（导出标识符不得以包名开头）

# e2e 闭环 / 压测
make e2e                          # TCP 业务 + KCP 战斗（dual 形态）
make e2e -e E2E_MODE=single       # WS 单通道
go run ./scripts/loadtest -transport kcp -inputs 500 -window 5s

# Proto 代码生成
make proto-tools          # 收集/构建 protoc 插件到 ./bin
make proto                # 生成全部 proto 产物（go/grpc/多传输/errors/openapi/配置）
```

---

## 依赖约定（本地开发）

- Go 1.26+、Docker、`protoc`、Atlas CLI（`atlas upgrade` 安装工具链）
- `go.mod` 以本地 `replace` 指向 `../atlas` 与 `../cow`——本地开发需保持这两个源码树与
  本仓处于同级目录（`.template-workspace/` 即标准布局）；CI 中由 workflow 按固定版本检出替换
- Atlas 源码树当前以 **feat/actor 分支**为编译基准（模板使用 actor 新命名、Notify 帧下行等能力）

## 代码库实践说明

- 本仓是从 `atlas new -r atlas-game-layout` 生成项目的**模板**：改代码时兼顾「业务实现」与
  「模板通用性」两面（通用能力放 `pkg/`/`lib/`，业务放 `services/*/internal/biz/`）。
- 若改动影响模板或生成 API，需要同时检查 `make proto` 的生成产物与 Atlas 的 protoc 插件
  （`cmd/protoc-gen-atlas-*`）。
- README 中有一些命令示例可能落后于当前布局；应以本文档为准。
