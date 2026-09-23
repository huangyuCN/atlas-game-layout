# Actor 节点身份与「只发不接」发送方排查（守护形态 ACTOR_NOT_FOUND 根因）

- 日期：2026-09-22
- 状态：**排查完成，方案待定**（使用者确认「专门来一轮排查」，方向见 §5）
- 范围：`atlas-game-layout` 守护形态跨服务 actor 调用（gateway → game/battle）；对照 `atlas/contrib/actor/{ingress,cluster,core}`。
  本轮只诊断与取证，不含代码改动。

## 1. 现象

守护形态（`~/atlas-game-layout/bin/*` 四进程 + 真 etcd/NATS）下，经 gateway（TCP 9001）注册/登录**必失败**：

| 观察 | 证据 |
|------|------|
| 最小客户端 register 5/5 失败，错误 `ACTOR_NOT_FOUND: process not found` | 探针输出（10.10.9.36） |
| 偶发「无错误但 `player_id` 为空」 | 同上（第 1 次尝试：`rerr=nil, playerID=""`） |
| gateway 与 game 两侧同时为同一 PID 打租约丢失 | `/tmp/gateway.log`、`/tmp/game.log`：`cluster: lease lost, freezing activation … pid=player:probe-…` |
| 进程内形态（`scripts/e2e`）一直正常 | `test/e2e` 12/12、`scripts/e2e -mode dual` 闭环 |

## 2. 两条「客户端」写法的分层（不是不一致的地方）

| 维度 | 集群内发送方（模板采用） | ingress（`contrib/actor/ingress`） |
|------|--------------------------|-----------------------------------|
| 定位 | 一个**完整的 actor 运行时节点**（`cluster.Runtime` + Locator + NATS + Directory + 租约） | **集群外入口**（L4）：NATS request-reply + 7 个显式操作 |
| 调用面 | `Local().Tell/Ask(pid, 强类型对象)`；目录 Lookup、epoch 栅栏、重投、懒激活全在运行时 | `pid 字符串 + []byte + typeURL`；owner 缓存（TTL 30s）与陈旧错误码自失效由调用方承担 |
| 生成物 | `XxxClusterClient`（`core.ActorInvoker`，强类型） | `api/actor/ingress/v1/client` SDK，**零 actor 运行时依赖** |
| 治理 | 无 | 鉴权/限流/审计/`RequireAuth` 门闩/信封与 payload 上限/worker 池 |
| 懒激活前提 | 发送方**本地必须注册该 Type 的 Props**（`canLazySpawn`） | ingress 节点同样要登记 Props——但框架自带的 ingress 节点登记的是**真实 handler**（`contrib/actor/loadtest/nodeserver.go:160`） |

结论：模板走「运行时 + 生成 ClusterClient」符合框架分层（ingress 自我定位就是集群外入口，`ingress/README.md:3`）。
**真正的不一致在第三处**：模板为满足 `canLazySpawn`，发明了「只发不接副本」——
把 Props 的语义「本节点**可托管**该类型」偷换成「本节点**可以问**该类型」（`pkg/actor/replica.go`）。

## 3. 偷换语义的代价（代码链 + 本轮实测）

1. 懒激活候选来自服务发现：`candidates = Discovery.GetService(serviceName)`（`cluster/delivery.go:403-417`），
   选点 `pickLeastProcs`（`cluster/placement.go:132-155`）**既不校验候选是否真的托管该类型，也不排除本机**。
2. 副本 Props 让「本机」成为**合法 spawn 目标**：`SpawnRemote(self)` → `spawnAt` 用副本 Props
   `Claim + local.Spawn` 成功 → 产生**幽灵实例**（`delivery.go:468-483`）。
3. 幽灵实例的 handler 是空实现：`OnTell → nil`、`OnAsk → (nil, nil)`（`pkg/actor/replica.go:16-17`）——
   消息被**静默吞掉**，正是「无错误但 player_id 为空」的来源。
4. 幽灵与真实宿主争同一 PID 的目录租约 → 双方各打一次 `lease lost, freezing activation`
   （`cluster/directory_renew.go:95-104,136-145`、`cluster/runtime_lease.go:22-31`）→ 目录残留错误归属 →
   后续请求命中 `ACTOR_NOT_FOUND`（`types/errors.go:108-110`，不可重试，立即失败）。
5. **触发条件（守护形态 100% 复现）**：四个进程的 `runtime.id` 都缺省 = 主机名
   （`services/*/configs/config.yaml` 无 `id`）→ actor NodeID 相同 → 节点 subject
   `atlas_actor.node.<nodeID>.{in,spawn,control,drain}` 是**普通订阅、非 queue group**
   （`cluster/transport_nats_control.go:46,115,172,207`）→ 一条节点消息**广播给同主机全部进程**。

## 4. 决定性实验（10.10.9.36）

| 场景 | 结果 |
|------|------|
| 四进程共享 NodeID = 主机名（当前模板默认） | etcd：`/atlas/services/test/{battle,game,gateway,matcher}/shimmer-bi-MS-7D17`（同一实例 ID）；探针 register 5/5 失败 |
| 显式配 `runtime.id: gateway-1 / game-1 / matcher-1 / battle-1` | etcd 四个不同实例 ID；探针**一次成功**：`OK PLAYER_ID=p-3b1aef6b…`；随后路由 TTL 读数 22s（= 配置 30s − 已过 8s，配置生效） |
| 进程内形态（ID 显式：`game-e2e`/`gw-e2e`/`battle-e2e`/`matcher-e2e`） | 一直正常，与「ID 唯一」假设一致 |

实验后已恢复服务器配置（`id` 注释掉）并重启四进程，回到基线。

## 5. 三个方向（待使用者选定）

| 方向 | 内容 | 收益 | 代价 |
|------|------|------|------|
| **A（推荐）框架补齐「只发不接」一等公民** | `Props` 分离「可托管」与「可发送」语义（如独立 API `RegisterRemoteType`）；placement 只选**真正托管该类型**的节点、禁止把 spawn 打回非宿主；模板删除 `pkg/actor/replica.go` | 能力谎报消失，故障不可能再发生；对模板是删代码 | 需要节点能力广告（registry metadata 或独立键）；框架侧改动较大 |
| **B 跨服务调用改走 ingress** | gateway/matcher 不再内嵌 actor 运行时，改用 `api/actor/ingress/v1/client`；域服务（game/battle）装配 ingress server | 与框架 L4 定位一致；白拿鉴权/限流/审计/上限 | 强类型对象退化为 `[]byte`；SDK 目前**不填 headers**，`RequireAuth` 会拒（需先补框架）；域服务新增组件与治理配置 |
| **C 最小修复** | 只修身份与选点：actor NodeID 每进程唯一 + placement 不过本机 | 改动最小，立刻消除当前故障 | 副本 hack 仍在，语义谎报未除 |

**三个方向都必须先做身份层修复**：actor NodeID 必须**每进程唯一**（今天它与 registry 实例 ID 混用，
而 registry 只需「每服务唯一」——同主机多进程必然撞 subject）。可选做法：NodeID = `<service>-<instanceID>`，
或模板各服务 config.yaml 显式配 `runtime.id`，或在 `pkg/actor` 构造运行时统一加服务名前缀。

### 5.1 决策（2026-09-22 讨论后确定）

- **跨服务调用走 gRPC，不走 ingress**：gateway / matcher 是**调用方**（不内嵌 actor 运行时），
  game / battle 各自是 actor 集群并通过 gRPC 入口（front door）暴露业务契约；ingress 保留给集群**外**调用方。
  理由：与业界主流一致（Orleans external client、Dapr placement、游戏后端大厅/战斗集群对等 RPC），
  且调用方无需懂 actor 寻址（PID / 懒激活 / epoch 不进边界）。
- **帧路径返回战斗服地址、客户端直连**：匹配回执携带入口地址 + 一次性票据；容器化下入口可以是
  实例地址（Agones 的 hostPort + 节点地址模型）或前置的稳定连接层，SDK 视其为不透明地址，两者可互换。
- **身份层修复（本文件 §4 实验验证的方案）作为阶段 0 先做**，与架构改造解耦。
- 设计与实施计划已落到 `atlas` 仓：
  `docs/superpowers/specs/2026-09-22-actor-cluster-boundary-design.md`、
  `docs/superpowers/plans/2026-09-22-actor-cluster-boundary-plan.md`（本文件的排查结论是它们的输入）。

## 6. 与「会话租期配置化」的关系

无关。本轮会话租期改动（提交 `7a1d777`）不触碰 actor/分发/relay 任何代码；该故障在改动前即存在，
只是此前没有客户端真正走守护形态的注册链路（`test/e2e` 与 `scripts/e2e` 都跑进程内形态，ID 恰好唯一）。
