# lt-driver — 远程压测驱动（实测版）

把负载**打在另一台机器上真正部署的五服务**上的压测驱动：业务链路走远端网关（TCP SDK 会话：注册 / 登录 / 入队 / 收成局通知），战斗帧经远端接入层（WS / UDP）直连，并在压测端按 5 s 窗口记录上行包速率时间线、按连接记录 SyncFrames 往返延迟样本。

## 为什么需要它（与进程内装配版的区别）

`scripts/loadtest` 是一体化压测：`main.go` 里调用 `startStack` 在**本进程内**拉起五服务（etcd/redis/nats + gateway/matcher/battle + 接入层），再让客户端连 `127.0.0.1`。

这种形态只能测「本机、与压测客户端抢同一批 CPU 的服务」，无法回答「远端真实部署形态在 N 核机器上崩塌不崩塌」：

- 进程内形态下压测客户端与服务共享 GOMAXPROCS / 内存 / 网络栈，测到的是混合体；
- 进程内形态无法在服务出问题时**从压测机另一侧**抓 pprof / 采 RSS（服务进程是压测进程自己的 goroutine）；
- 进程内形态无法跨机复现「五服务正常、但接入层或 battle 单点崩塌」的场景。

因此本目录只保留**地址全部来自旗标**的驱动：不装配任何服务，`-gateway` / `-edge-ws` / `-edge-udp`（或 `-via direct` + `-battle-ws` / `-battle-udp`）指向远端即可。

## 参数说明

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-players` | `50` | 并发玩家数（= 帧连接数），每个玩家一个账号 |
| `-per-battle` | `2` | 每局人数；模板匹配器固定 1v1，非 2 时仅记录 |
| `-transport` | `ws` | 帧传输面：`ws` \| `udp` |
| `-via` | `edge` | 驱动路径：`edge`（经接入层，验票 + 转发）\| `direct`（直连 battle 帧端口，排障对照） |
| `-pps` | `20` | 每连接每秒帧输入数（上行包速率） |
| `-probe` | `1s` | 每连接 SyncFrames 探针间隔（往返延迟样本来源） |
| `-probe-jitter` | `false` | 探针相位抖动（诊断「所有连接同时探针」造成的假象） |
| `-duration` | `20s` | 稳定负载时长（从建连/入局完成后开始计）。**到点自动停止发送并打印 SUMMARY**：单次帧调用带 2 s 预算（`DefaultFrameCallTimeout`），卡死的连接最多让收尾晚 `调用预算 + 1 s`（`pumpAll` 的等待上限），超过即强制进入汇总——不再出现「到点仍 0 pps、需人工 kill」 |
| `-steady-window` | `5s` | 稳态窗口长度（报表里「稳态末 N 秒」用） |
| `-rate-every` | `5s` | 上行包速率采样间隔（压测端 pps 时间线） |
| `-dial-concurrency` | `64` | 建连 / 注册的并发度（建流窗口的主要耗时来源） |
| `-hello-timeout` | `300ms` | 数据报面 hello 单次读超时：接入层限流/票类拒绝时不回错误，只能靠超时发现（旧值 `3s` 会把建流窗口放大到几十秒） |
| `-hello-attempts` | `3` | 数据报面 hello 最大尝试次数（含首次；每次重试都换新 socket，避免迟到的旧 flow-id 被误当答案） |
| `-hello-backoff` | `150ms` | 数据报面 hello 两次尝试之间的退避（读超时等普通失败用） |
| `-hello-unavailable-backoff` | `150ms` | 收到「暂不可用」回执后的重试间隔（夹到 100–200ms）。接入层对「解析失败/后端拨号失败」的 hello 会回 1 字节 `0x00`（框架 `edge.HelloRejectUnavailable`），驱动据此**不等超时**立即重试——一次瞬时失败因此从秒级降到百毫秒级 |
| `-connect-timeout` | `120s` | 建连与入局阶段的总超时 |
| `-total-timeout` | `10m` | 整轮压测的兜底超时（到点必退，避免挂死） |
| `-gateway` | `10.10.1.23:9001` | 远端网关业务面地址（host:port） |
| `-edge-ws` | `10.10.1.23:7100` | 远端接入层 WS 面地址 |
| `-edge-udp` | `10.10.1.23:7102` | 远端接入层 UDP 面地址 |
| `-battle-ws` / `-battle-udp` | 空 | `-via direct` 时的 battle 帧端口 |

输出：人读表格（连接 / 包速率 / 三段延迟分位 / 分段 p99）+ 末尾一行 `SUMMARY {…}` JSON（`rate_samples` 是压测端视角的 5 s 窗口 pps，崩塌判据「上行跌 >50%」用它判）。字段名进归档日志，勿随意改名。

## 构建与运行

```bash
# 本机交叉编译到 Linux 压测机（10.10.9.36 有 Go，也可直接在那编译）
GOOS=linux GOARCH=amd64 go build -o /tmp/ltdriver ./scripts/lt-driver
scp /tmp/ltdriver shimmer-bi@10.10.9.36:/tmp/ltdriver

# 128 流自检（建连 + 入局 + 稳定负载，确认驱动与远端全链路通）
/tmp/ltdriver -players 128 -duration 20s -pps 10 -probe 1s \
  -gateway 10.10.1.23:9001 -edge-ws 10.10.1.23:7100 -edge-udp 10.10.1.23:7102

# 正式压测：4096 流、60 s、每连接 20 pkt/s
/tmp/ltdriver -players 4096 -per-battle 2 -duration 60s -pps 20 -probe 1s -rate-every 5s \
  -gateway 10.10.1.23:9001 -edge-ws 10.10.1.23:7100 -edge-udp 10.10.1.23:7102

# 排障对照：绕过接入层直连 battle 帧端口
/tmp/ltdriver -players 128 -via direct -battle-ws 10.10.1.23:<battle-ws> -duration 20s
```

自检通过判据：建连成功 128、失败 0、`SUMMARY` 里 `frame_failures=0`、探针 p99 在毫秒级。压测期间被压端的 pprof（`10.10.1.23:6060–6064`）与 `/metrics` 采样由调用方另行采集，本驱动不抓栈、不发信号（`SIGQUIT` 会让服务优雅停机，**禁止**用它抓栈）。

## 与 `scripts/loadtest` 的关系

`client.go` / `edgeconn.go` / `stats.go` 是 `scripts/loadtest` 同名文件的**逐字副本**（`diff` 为空），本目录只换了 `main.go`（去掉 `startStack`，地址走旗标）。

为什么复制而不是 import：`scripts/loadtest` 是 `package main`，Go 不允许 import 另一个 `main` 包；把它改成库包 `loadtest` + 薄 `main` 需要重命名数十个标识符并同时改 e2e 脚本，风险与收益不成比例（本次任务明确「不要大改 `scripts/loadtest`」）。因此采用**最小复制**。

纪律（务必遵守）：

- 改 `scripts/loadtest` 里上述三个文件时，**必须同步本目录副本**，否则两边行为漂移；
- 本目录的 `main.go` 只做「读旗标 → 建玩家 → 加压 → 报表」，不放传输 / 统计逻辑；
- `scripts/check-dup/allowlist.txt` 里为这批同构副本列了专门的豁免段（协议机器人三文件与本目录逐字一致，属设计性重复）。
