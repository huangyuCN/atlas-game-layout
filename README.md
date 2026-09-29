# atlas-game-layout

基于 [Atlas](https://github.com/huangyuCN/atlas) 的游戏单仓模板：**gateway / game / matcher / battle** 四服务，
覆盖 注册 → 登录 → 匹配 → 战斗 → 结算 的完整游戏闭环，内置五协议接入、actor 集群、lockstep 帧同步、
分布式会话与可编程装配。

## 三步开箱

```bash
# 1. 用 Atlas CLI 生成项目（以本模板生成自己的仓库）
atlas new my-game -r https://github.com/huangyuCN/atlas-game-layout

# 2. 起中间件并一键运行四服务（前台交错输出，Ctrl-C 全部退出）
cd my-game
make compose     # Docker：etcd 12379 / redis 16379 / nats 14222 / mongo 27017
make run-all     # 四服务按 services/*/configs/config.yaml 端口启动

# 3. 另开终端跑 e2e 闭环脚本（双客户端双形态）
make e2e                          # TCP 业务 + KCP 战斗（双通道）
make e2e -e E2E_MODE=single       # WS 单通道
```

通过标准：脚本以 `e2e 闭环通过（dual/single 形态）` 结尾，非零退出即失败。

## 目录结构

```
api/            # 协议定义（proto + 生成代码），按服务分目录
lib/            # 代码级公共定义：consts / idgen / session / errors / gametime
pkg/            # 每服务公共装配（fx.Module 化）：actor / redis / nats / mongo / etcd / registry /
                # bootstrap（配置加载+日志+App 组装） / fxkit（跨服务 fx 泛型提供器） / serverutil
services/       # 四服务：
                #   gateway  —— 五协议接入 + 分布式会话 + 挤下线 + 下行推送
                #   game     —— 玩家业务（cow 聚合根 + 三级缓存）
                #   matcher  —— 撮合（matchmaker + 等级相近规则）
                #   battle   —— 战斗（actor + lockstep 会话 + 结算）
                # 每服务统一「唯一装配之家」形态：internal/app 集中列出全部组件清单，
                # 进程形态（atlas.App）与嵌入式形态（assemble，fx 编程式启动）共用同一张
                # 依赖图；internal/{conf,infra,biz,data,server} 只做各自职责，不含装配逻辑。
deploy/         # 中间件 docker-compose
scripts/e2e     # 双客户端闭环脚本（双形态）
scripts/loadtest# 帧通道压测（KCP vs WS）
test/e2e        # 进程内端到端测试（真中间件，不可达自动跳过）
```

## 只订协议即可开发（开发指南）

新增一个业务接口**不需要**碰装配/路由/gateway：改 proto → `make proto` → 填生成桩。

### 域协议的四平面布局（v2）

一份域 proto（`api/game/v1/player_service.proto`、`api/battle/v1/battle_service.proto`）经 Atlas 插件
一次生成四类产物，按平面分子包隔离——**根包只留消息与路由表**：

| 平面 | 产物位置 | 内容 |
|------|----------|------|
| 根包（消息 + 路由表） | `api/<域>/v1/*.pb.go`（`*_route.pb.go` 是路由表） | `--go_out` 消息；`<Service>RouteTable`（网关透传引擎查表用） |
| `actor/` | `api/<域>/v1/actor/*.pb.go` | 业务接口 `<Service>` + `Unimplemented<Service>` 兜底 + 分发桩 + 解码表 + `<Service>ClusterClient`（集群互调） |
| `rpc/` | `api/<域>/v1/rpc/*.pb.go` | Edge（`access=CLIENT`）/ Internal（`access=INTERNAL`）双接口 + 自带 `ServiceDesc`/`Register` + 类型化客户端 |
| `opclient/` | `api/<域>/v1/opclient/*.pb.go`（另有 `_pb.ts` / `_client.g.cs`） | 会话通道 CLIENT op 的强类型 SDK stub（三语言） |

- 生成物**零词缀**：类型名即 service 名（`PlayerService` / `NewPlayerService` / `PlayerServiceClusterClient`）；
- 域 proto **不进** `--go-grpc_out`（`rpc/` 平面自持 gRPC 管线）；无 route 注解的 proto（会话等）仍走标准产物；
- Edge / Internal 各自 `Register…` 到两个独立 listener（端口隔离即信任边界），注册错面**编译期**即失败；
- **每个 rpc 必须使用独立请求消息**：actor 分发按消息类型路由，生成期校验重复输入。

### 新增一个 op 三步流程

以给 game 增加「背包查询」为例（已有 `GetBackpack` 可参考）：

1. **定义协议**：在 `api/game/v1/player_service.proto` 的 `PlayerService` 增加 rpc，并按需标注
   `atlas.route.v1`（`access=CLIENT` 是客户端 op、`INTERNAL` 是服务间调用；uid 来源决定 actor 目标 PID）；
2. **生成代码**：`make proto`（Atlas 全家桶插件：go / actor / client / http / tcp / udp / kcp / ws / errors / openapi）；
3. **填生成桩**：业务逻辑实现生成的 `actor/` 平面接口（`services/game/internal/actor/`，battle 为
   `services/battle/internal/actor/`），错误直接 `return nil, err` 上抛；接入层在 `services/<域>/assemble`
   挂载两个 gRPC 面即可（Edge/Internal 的 `Register`/`ServiceDesc` 由生成产物自带）。

> **注意（wire 不兼容）**：actor 消息已**去信封化**——不再有 `PlayerActorMsg`/`BattleActorMsg`
> oneof 包装，请求/回执直接是业务消息，业务错误经集群 error 通道往返。**服务端与客户端需整仓同步升级**，
> 旧信封二进制不兼容，不能混布。
>
> proto 工具链：`make proto-tools` 优先收集 `ATLAS_BIN` 现成插件（`atlas upgrade` 安装目录），
> 缺插件且存在 Atlas 源码时回退源码构建；均不可用时按报错指引执行 `atlas upgrade`。

## 帧通道压测

```bash
go run ./scripts/loadtest -transport kcp -inputs 500 -window 5s   # KCP 战斗通道
go run ./scripts/loadtest -transport ws  -inputs 500 -window 5s   # WS 战斗通道
```

输出 `SUMMARY` JSON：帧输入往返延迟（P50/P95/P99、吞吐）与帧广播下行（fps、延迟）。
基线归档见 Atlas 仓库 `docs/superpowers/benchmarks/game-template-frame-channel-*.md`。

## 测试与 CI

```bash
go test ./...            # 单元测试（内存/内嵌中间件）
go test ./test/e2e/      # 端到端（真 etcd/redis/nats/mongo；不可达时 Skipf 跳过）
```

CI（`.github/workflows/ci.yml`）：gofmt / vet / 单测 / 构建；集成测试 job 经 SSH 在集成服务器
（10.10.9.36，常驻 etcd 12379 / redis 16379 / nats 14222）执行 `test/e2e`。

## 依赖约定

- Go 1.26+、Docker、`protoc`、Atlas CLI（`atlas upgrade` 安装工具链）
- Atlas 源码树当前以 **feat/actor 分支**为编译基准（模板使用 actor 新命名、Notify 帧下行等能力，
  该分支领先 main；主树 `git checkout feat/actor` 即可），`go.mod` 以本地 replace 指向
  Atlas（`../atlas`）与 [cow](https://github.com/huangyuCN/cow)（`../cow`）；
  CI 中由 workflow 按固定版本检出替换，发布到远端仓库时请改用对应伪版本
- 中间件端口约定见 `deploy/docker-compose/compose.yaml` 与 `services/*/configs/config.yaml`
