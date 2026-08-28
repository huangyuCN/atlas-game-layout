# 四服务装配结构统一重构（唯一装配之家）

> 状态：已完成（2026-08-28）。本文档整合打样与平推两阶段的计划与执行结果，
> 作为「唯一装配之家」形态的设计依据留存；后续新服务按此形态落地。

## 背景与目标

模板初期每个服务的装配逻辑同时存在于两处：

- **进程形态**：`cmd/main.go` → `bootstrap.Assemble` → fx 模块（`server.Module` 的 fx.Provide 清单，且散落在 `server/deps.go`、`internal/infra` 等多处）；
- **嵌入形态**：`services/*/assemble/assemble.go` 全手写接线（每服务约 200–300 行），每个错误分支重复一遍资源释放链（game 有 7 处），TTL 类常量与 fx 图各声明一份。

后果：每新增依赖要改两处、两套风格，参数必然漂移；`server` 包职责被污染；各服务同名子包语义不一致（`infra` 在 game 是客户端构造、在 battle 是 biz 接口的 NATS 实现）。

目标：**装配清单收敛到每服务唯一一处且分层可读；两条启动路径共用同一张依赖图；对外 API 借机收紧**。原则：主角是业务（biz/actor），infra/data/server 都是围绕它的适配器，「谁依赖谁」只在唯一一处出现。

## 最终形态

```
services/<svc>/
├── cmd/main.go          # bootstrap.Assemble(name, &cfg, app.Module) —— 3 行
├── internal/app/        # ★ 唯一装配之家
│   ├── graph.go         #   var Module：fx.Provide 分层清单（infra→data→biz→server）+ Invoke（actor 注册 / 生命周期 / 资源回收）
│   ├── infra.go         #   外部客户端与集群运行时构造（纯函数，可离线单测）
│   ├── graph_test.go    #   fx.ValidateApp 静态校验（含 servers 组消费者，见「测试盲区」）
├── internal/server/     # 回归单一职责：协议 Server 构造与路由挂载，不含装配逻辑
├── internal/{conf,biz,actor,data}/  # 业务不变；infra 包内仅保留 biz 接口的外部实现（如 NATS 通知器）
└── assemble/            # 嵌入式瘦壳：newBootstrap(Options) 映射配置 → fx 编程式启动 → ServeAsync → 注册实例
```

两条路径的关系：**同一批构造函数，两种驱动**——进程形态由 atlas.App 管信号与启停，嵌入形态由 `serverutil.ServeAsync` 后台启动并就绪探测；组件与资源的逆序回收统一由 fx 生命周期 Hook（`registerResources`）接管。

## 关键设计决策

| 决策 | 说明 |
|------|------|
| `pkg/fxkit` 泛型提供器 | 各服务 conf 均引用公共配置消息，`fxkit.NewEtcdClient[*conf.Bootstrap]` 一行声明 etcd 客户端与注册器；缺 `registry.etcd` 配置时快速失败（原先静默不注册，actor 集群本就依赖 etcd，早失败优于晚失败） |
| `pkg/bootstrap` 收缩 | `AssembleLoaded` 只做日志初始化 + Supply(cfg) + App 模块；etcd 条件装配下沉给各服务模块自行声明 |
| 配置单源 | 装配图只认 `*conf.Bootstrap` 一种输入；嵌入式用 `newBootstrap(Options)` 把参数映射成配置，两条路径共享全部构造函数 |
| 测试注入改走 `fx.Decorate` | battle 的 `BattleCfg`（战斗参数）、matcher 的 `SinkOverride`（成局观察方）以顶层装饰器覆盖 Provider 输出；装饰器恒挂载、nil 时透传，行为零差异 |
| gateway 双 servers 组 | `servers` 组五台全供生产形态；新增 `embed_servers` 子组四台供嵌入式——WS 因单通道形态经「`/ws` 路径 + httptest」包装，不能被独立 Start |
| matcher tick 循环 ctx | 撮合运行时 OnStart 必须用进程级 context（fx 生命周期 ctx 有超时，会杀掉长驻循环），原语义与注释完整保留 |
| 时间参数单点化 | game 的 sessionTTL/snapshotTTL/snapshotTick 收敛至 `internal/app/tuning.go`，消除双份常量漂移 |

## 实施过程要点（TDD）

1. 每服务三个测试：`newBootstrap` 映射单测（纯函数覆盖空值回退）、`fx.ValidateApp` 静态校验、e2e 兜底。
2. `serverutil.ServeAsync` 先测试后实现（真起 `atlashttp`/`atlasgrpc` Server 验证启动、就绪探测与失败回收；atlas 传输层 Start 不会因 ctx 取消自行退出，回收必须显式 Stop——与 atlas App 的「等取消 → 调 Stop」goroutine 模式对齐）。
3. **测试盲区修复**：`fx.ValidateApp` 不展开 `servers` 组的依赖链，接口绑定缺失（如 battle 的 `biz.BattleNotifier`、matcher 的 `biz.PlayerTicketMapper`）在单测漏检、直到服务器真连接 e2e 才暴露。修复：四个服务的图校验测试统一追加 servers 组消费者（`fx.Invoke(fx.Annotate(func([]transport.Server) {}, fx.ParamTags(...)))`），本机即可覆盖传输层构造依赖链。

## 对外接口变化（BREAKING CHANGE）

- `Options` 收紧（均无仓库内调用方）：game/matcher 移除 `GRPCAddr`、game 移除 `SessionTTL`、gateway 移除 `TCPAddr/SessionTTL`；句柄字段（Runtime/GRPCURL/HTTPURL/Actors/Service 等）与 `Stop` 签名不变，e2e 零改动。
- `pkg/bootstrap.Assemble` 的调用方式不变；etcd/注册器改由服务模块声明。
- 缺 `registry.etcd` 配置的进程形态从「静默不注册」变为启动报错。

## 验证结果

- 本机：40 个包单测全绿，gofmt/vet/build 干净。
- 集成服务器（10.10.9.36，etcd 12379 / redis 16379 / nats 14222 / mongo 27018 常驻）：模板 38 包单测全绿；**e2e 十条全 PASS**（注册登录、五协议、匹配队列、战斗完整闭环、战斗重连、跨网关挤下线）。
- 已知环境差异：atlas 主树的 `cmd/protoc-gen-atlas-errors` 集成型测试依赖 `protoc` 二进制，无 protoc 的机器上会失败（CI 环境已装，非代码问题）。

## 遗留事项

- CI 的 `ATLAS_REF` 仍指向 main；模板编译基准为 atlas **feat/actor** 分支（含 Notify 帧下行、PushRaw、actor `Type` 命名等能力，领先 main），需决策切换 ci.yml 还是等 feat/actor 合回 main。
- 依赖升级（模板 20 模块 / atlas 91 模块）与本次重构同批入库；atlas 侧另修复了 etcd 系测试因 grpc 新版懒连接语义导致的探活卡死。
