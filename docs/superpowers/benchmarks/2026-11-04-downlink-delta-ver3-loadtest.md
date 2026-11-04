# ver=3 下行增量编码：真机 A/B 压测（2026-11-04）

| 这份文档讲什么 | 什么时候更新的 | 还有效吗 | 合并了哪些旧文档 |
|---|---|---|---|
| 4096 流 ws 面、10 人局、tick=100ms 下，`frame_broadcast_delta` 开/关的 A/B 实测 | 2026-11-04 | ✅ 有效（结论：**协商到 ver=3，但实际下发全量 ver=2，TX 未降**） | 无 |

## 一、口径与现场

- 被测：模板 `86162bc` + 框架 `52d06bed`，9.36 离线构建（`GOPROXY=off`），部署 1.23；双 battle 运行中 exe sha256 一致（`805d60f6…`）。
- 负载：`-players 4096 -per-battle 10 -transport ws -via edge -pps 20 -probe 1s -probe-jitter -duration 60s`，410 局、实际 9.99 人/局。
- 配置：`max_frames=20000`、`tick_interval_ms=100`、`frame_broadcast_protobuf=true`、`memory_mode` 关闭；A 组 `frame_broadcast_delta` 缺省（false），B 组 `true`（两 battle 均开）。
- **preflight 11/11 通过**（两组均 exit 0；含双 battle 同源、双注册、UDP 收缓冲 rb16777216、网关 health）。
- 驱动：`scripts/lt-driver` 增补 ver=3 支持（声明 `accept_payload_versions=[1,2,3]`、`lib/downlink` 解码合并、字节/版本计数），未提交。

## 二、A/B 对照

| 指标 | A（delta 关） | B（delta 开） | 变化 |
|---|---|---|---|
| 协商版本（回执） | `{2: 4096}` | `{3: 4096}` | ✅ 协商到 3 |
| 实收帧版本 | v2=2,525,248 / v3=0 | v2=2,479,555 / **v3=0** | ❌ 未走增量 |
| 下行载荷字节 | 1,273,517,992 B | 1,252,011,818 B | −1.7%（随帧数，非每帧） |
| 每帧字节（含 16B 帧头） | 520.3 B | 520.9 B | **+0.1%** |
| 帧数 / pps | 2,525,248 / 42,082 | 2,479,555 / 41,323 | −1.8%（每连接 10.3→10.1 帧/s） |
| 写 syscall 合计（六进程） | 21,393,328 | 21,285,415 | −0.5% |
| CPU 合计（六进程） | 9.54 核 | 10.07 核 | +5.6% |
| 其中 battle1/battle2 | 3.10 / 3.25 | 0.08 / 6.49 | 负载集中到单节点 |
| 探针 p50/p95/p99 | 0.62 / 11.02 / 27.47 ms | 0.34 / 28.59 / 47.90 ms | p95/p99 变差 |
| 帧失败 | 1 | 1,960 | ❌ 上升 |
| NIC TX（1.23 eth） | 2,251.7 MB / 75s | 2,240.0 MB / 72s | ≈0 |

## 三、结论

1. **TX 未降（0%），远低于 ≥70% 的验收线 ⇒ 本轮证伪。**
2. 包数 / 帧数 / 写 syscall **基本相等**（−1.8% / −0.5%，属轮次抖动与建流窗口差异）——「包数不变」这条断言成立。
3. 关键证据：B 组回执协商到 **ver=3**，但线上**每一帧都是 ver=2 全量**（v3=0、每帧 520.9 B、NIC TX 与 A 相同）。即「以为开了、实际没走增量」。
4. 隔离验证：`TestDeltaWireCarriesCurrentHash` / `TestDeltaPacketCountUnchanged` 通过 ⇒ 服务端编码/决策逻辑在单测形态正确，差异出在真机装配/运行期（逐连接 `lastSeq`/`payloadVer` 基线或 `linkOK`），本轮按约束**未改业务代码**，未继续定位。
5. B 组 CPU/时延/帧失败与 A 组不可直接比较：B 轮 4096 连接几乎全落在单台 battle（b1 0.08 核），属放置偏斜；delta 未生效时它等价于第二组 ver=2。

## 四、复现与产物

- 驱动：`scripts/lt-driver/{client.go,main.go}`（本轮本地改动，未提交）。
- 现场产物（9.36）：`~/delta-round/runs/delta-A4/`、`~/delta-round/runs/delta-B/`（`driver.log` 内含 `SUMMARY`、`cpu.txt`、`preflight.txt`、`snap-t0/t1.txt`）。
- 脚本：`~/delta-round/run.sh <A|B> <tag>`（部署→配置→重启→preflight→快照→60s→汇总）。
- 还原：两 battle 的 `frame_broadcast_delta` 已删除（=false），六进程已重启，`frame_broadcast_protobuf=true` 保持不变；**未提交任何代码**。
