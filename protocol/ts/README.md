<!-- 由 scripts/gen-protocol.sh 从 protocol/README.md 复制生成，改动请改该源文件。 -->

# Atlas 游戏协议包（atlas-game-protocol）

本目录是**游戏自己的协议包**：把本仓 `api/` 下三个游戏域（`battle` / `game` / `matcher`）的消息，
从 `.proto` **生成**成三端（Go / TypeScript / C#）各自可独立发布的包。
**全部是生成物**——改协议只改 `.proto` + 重跑一个脚本，三端同时更新，**没有任何手写消息类**。

## 一、这是什么

| 端 | 目录 | 包标识 | 运行时依赖 |
|---|---|---|---|
| Go | `protocol/go/` | module `github.com/huangyuCN/atlas-game-layout/protocol` | 仅 `google.golang.org/protobuf`（protobuf 运行时） |
| TypeScript | `protocol/ts/` | npm `@huangyucn/atlas-game-protocol` | **零**（纯 `interface` / `type` / `const`，不引 protobuf 运行时、不引 SDK） |
| C# | `protocol/csharp/` | NuGet `HuangyuCN.AtlasGame.Protocol` | 仅 `System.Text.Json`（表达 protojson 字段特性；不引 protobuf 运行时） |

**生成范围**（三个游戏域，7 个 proto）：

| proto | proto 包 | 内容 |
|---|---|---|
| `api/battle/v1/battle.proto` | `battle.v1` | 战斗域基础消息（结算、票据、接入层端点等） |
| `api/battle/v1/battle_service.proto` | `battle.v1` | 战斗业务 op（加入/帧输入/补帧/Ping）+ 帧广播与终态推送 |
| `api/battle/v1/migration.proto` | `battle.v1` | 战斗迁移控制面消息（服务间，非客户端 op） |
| `api/game/v1/player.proto` | `game.v1` | 玩家域消息（玩家数据、背包等） |
| `api/game/v1/player_service.proto` | `game.v1` | 玩家业务 op（登录/匹配/组队...）+ 成局等推送 |
| `api/matcher/v1/matcher.proto` | `matcher.v1` | 匹配状态消息 |
| `api/matcher/v1/match_events.proto` | `matcher.v1` | 匹配事件/失败原因等 |

**每个包还带 3 个契约域依赖副本**（同为生成物，权威定义仍在原处，本目录不维护）：

- `api/atlas/v1/route.proto` → Go 侧 `route` 包（`atlas.route.v1` 注解；生成的 Go 消息对它有
  依赖登记，必须存在于模块内）；
- `api/lockstep/lockstep.proto` → `atlas.game.lockstep`（`SessionMeta` / `LockstepInput` /
  `SnapshotMeta` 等，框架仓权威定义）；
- `api/common/v1/common.proto` → `common.v1`（`PlayerSummary`，本仓权威定义）。

原因：三个游戏域的消息**字段类型直接引用**这些契约域消息（例如 `JoinBattleReply.meta` 是
`atlas.game.lockstep.SessionMeta`）。协议包要能独立发布，就必须自持这些类型——否则 Go 端要
`require` 整个框架仓 / 本仓大模块（违背「只依赖 protobuf 运行时」），TS/C# 也没有跨包类型映射机制。
`google/protobuf/opclient/timestamp_*` 同理（`SessionMeta.server_time` 是 WKT `Timestamp`）。

**产物内容**（三端一一对应）：

- 消息 DTO（Go 是 `protoc-gen-go` 的标准结构体；TS/C# 是 `protoc-gen-atlas-client` 的零运行时
  DTO，protojson 语义：camelCase 字段名、64 位整数与 bytes 以字符串下发、枚举以枚举名下发）；
- 枚举（C# 另配 `<Enum>XConst` 字符串常量，兼容未知枚举名）；
- op 常量：`<Service>ProtocolOps`（客户端可达 op 名）、`<Service>PushOps`（服务端推送 op 名）。

**不含**：调用 stub、传输、会话、心跳、重连、序列化器实现。这些是 SDK 的职责（见 §四）。

## 二、怎么生成

唯一入口，幂等（连跑两次产物零 diff）：

```bash
make proto-tools           # 首次：准备 protoc-gen-go（及 Atlas 插件依赖）
bash scripts/gen-protocol.sh
```

脚本做的事：`protoc` 三趟 → `protocol/go/api/**/*.pb.go`、`protocol/ts/src/api/**/*_pb.ts`、
`protocol/csharp/api/**/*_client.g.cs`，再渲染 TS 入口 `protocol/ts/src/{index,<域>}.ts`。

| 环境变量 | 默认 | 用途 |
|---|---|---|
| `ATLAS_DIR` | 同级 `../atlas` | 框架仓根（提供 `api/lockstep/lockstep.proto`、`api/atlas/v1/route.proto` 与插件源码） |
| `ATLAS_BIN` | `$ATLAS_DIR/bin` | protoc 插件目录（找不到 `protoc-gen-go` 时从这里取） |
| `PROTOC` | `protoc` | protoc 可执行文件（生成物头部记录其版本，跨版本重生成会有 diff） |

要点：

- 插件 `protoc-gen-atlas-client` **总是从框架仓源码现建**（`go build` 到本仓 `bin/`）。它带着
  生成协议包必需的 `emit_stub=false`（只出 DTO 与 op 常量，不引 SDK 内核）；复用旧二进制会直接
  报 `no such flag -emit_stub`。
- **协议改动流程**：改 `api/**/*.proto` → `make proto`（服务端 Go 产物）→
  `bash scripts/gen-protocol.sh` → 提交两者 → 按 §三 发版（三端同版本号）。
- **禁止手改生成物**（文件头有 `DO NOT EDIT`）。要改就改 `.proto` 或本脚本。
- 本文件（`protocol/README.md`）是手写源文件；脚本会把它复制成 `protocol/ts/README.md`
  （npm 只把包根的 README 打进 tarball）。**改完本文件要重跑脚本**，否则 CI 的漂移门会红。
- CI 门禁：`.github/workflows/ci.yml` 里 `scripts/gen-protocol.sh` + `git status` 检查「重生成无 diff」，
  生成物与 proto 不一致即红；同一处还会单独编一次 `protocol/go`（嵌套 module，父仓 `go build ./...`
  覆盖不到它）。

## 三、三端怎么发布

三端**同一个版本号**（当前 `0.1.0`），协议变更即发版（机械动作，不改手写代码）。

### Go（tag 必须带 `protocol/` 前缀）

Go module 在子目录 `protocol/`，按 Go 的多模块规范，发布 tag 形如 `protocol/v0.1.0`：

```bash
git tag protocol/v0.1.0
git push origin protocol/v0.1.0
# 消费方
go get github.com/huangyuCN/atlas-game-layout/protocol@v0.1.0
```

```go
import battlev1 "github.com/huangyuCN/atlas-game-layout/protocol/api/battle/v1"
```

（本地联调可 `replace github.com/huangyuCN/atlas-game-layout/protocol => ../atlas-game-layout/protocol/go`。）

### TypeScript

```bash
cd protocol/ts
npm install          # 仅 typescript（构建期）
npm run build        # tsc → dist（ESM + d.ts）
npm publish          # 或 pnpm publish
```

```ts
import { battle } from "@huangyucn/atlas-game-protocol";
import type { FrameBroadcast } from "@huangyucn/atlas-game-protocol/battle";
```

`package.json` 已声明 `main`/`types`/`exports`（`.`、`./battle`、`./game`、`./matcher`、`./common`、
`./lockstep`）与 `files`（只发 `dist` 与 README）。

### C#

```bash
cd protocol/csharp
dotnet build                 # 编译（0 warning）
dotnet pack -c Release       # 出包：bin/Release/HuangyuCN.AtlasGame.Protocol.0.1.0.nupkg
dotnet nuget push bin/Release/HuangyuCN.AtlasGame.Protocol.0.1.0.nupkg \
  --source https://api.nuget.org/v3/index.json --api-key "$NUGET_API_KEY"
```

```csharp
using HuangyuCN.AtlasGame.Protocol.Battle.V1;   // battlev1
using HuangyuCN.AtlasGame.Protocol.Game.V1;     // gamev1
using HuangyuCN.AtlasGame.Protocol.Matcher.V1;  // matcherv1
```

TFM 与 C# SDK 对齐（`netstandard2.1`，Unity 优先）。命名空间统一加
`HuangyuCN.AtlasGame.Protocol` 前缀，与 SDK 的 `Atlas.*` 错开，避免同集里同名类型相撞。

## 四、与 SDK 的分工

| | 三端 SDK（`atlas-sdk-go` / `atlas-sdk-ts` / `atlas-sdk-csharp`） | 本包（`atlas-game-protocol`） |
|---|---|---|
| 职责 | **传输 + 会话**：拨号、帧协议、KCP/UDP/WS/TCP、重连、心跳、终态语义、序列化器 | **游戏消息**：`battle` / `game` / `matcher` 三个域的消息 DTO、枚举、op 常量 |
| 契约消息 | 会话生命周期、`frame` / `edge` / `session` / `common` / `route` / `lockstep` | 只带**依赖副本**（见 §一），不拥有定义 |
| 发版节奏 | 极少（传输/契约变更） | 频繁（每次协议变更，机械发版） |
| 依赖关系 | 不依赖本包 | 不依赖 SDK 与 protobuf 运行时（C# 仅 `System.Text.Json`） |

调用姿势：SDK 的会话内核负责「把字节送出去、把字节收回来」，本包给出「那些字节是什么类型、
op 名怎么写」。强类型调用 stub（`new PlayerService(invoker)` 这类）需要 SDK 的 `Invoker` 契约，
属于传输层，留在 SDK 侧（下一步把本包接进三端 SDK 时处理）；本包只出 DTO + 常量，因此两端
版本线互不牵连。

## 五、已知的归属可疑项（下一步要裁）

生成时确认：以下内容目前挂在三个游戏域里，但**从语义归属看是契约域/服务端面**，是否继续随
客户端协议包发布需要裁定（本次按「三个游戏域全量生成」执行，未改任何 proto）：

1. `battle.v1.EdgeEndpoint`（+ `EdgeTransport` 枚举）：接入层（`contrib/edge`）契约类型，定义在
   `battle_service.proto` 内，被成局推送引用。按「契约域留在 SDK」的口径，它们更像 `edge` 域。
2. `atlas.game.lockstep.*` 与 `common.v1.PlayerSummary`：被游戏消息字段直接引用，因此被复制进本包
   （见 §一）。若后续要「本包只含游戏自有消息」，就要把这些引用改成契约包里的类型（Go 需
   `require` 契约模块、TS/C# 需跨包类型映射），是一次结构性调整。
3. `battle.v1` 迁移控制面（`migration.proto` 的 `PrepareBattleMigration*` / `MigrateBattle*`）：
   **服务间**消息，非客户端 op。它不会出现在 `*ProtocolOps` 里，但作为 DTO 出现在客户端包里
   属于额外体积（且对外暴露了内部控制面模型）。
4. `BattleService` 的 INTERNAL 方法（建战/查状态/发入场票/迁移等）同域同文件：op 常量已按
   `access=CLIENT` 过滤，不会外泄；但对手方消息类型仍在包内。
