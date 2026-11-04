# ver=3 下行增量编码：真机 A/B 压测（2026-11-04）

| 这份文档讲什么 | 什么时候更新的 | 还有效吗 | 合并了哪些旧文档 |
|---|---|---|---|
| 4096 流 ws 面、10 人局、tick=100ms 下，`frame_broadcast_delta` 开/关的 A/B 实测 | 2026-11-04（§五复测同日） | ✅ 有效（结论：**修复后 99.5% 帧走 ver=3 增量，每帧 −90.5%、下行载荷 −93%**；§二/§三的两轮失败实测作历史保留） | 无 |

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

## 五、复测：修复「重连抹掉协商版本」后（2026-11-04）

### 5.1 元数据

- **日期**：2026-11-04（UTC+8），A/A2/A3 与 B/B2/B3 各 3 轮，每轮 60s。
- **对比范围**：§二（模板 `86162bc` + 框架 `52d06bed`，delta 未生效）→ 本节
  **模板 `55d255f` + 框架 `91945e33`**（`55d255f` 修 `resetState` 抹掉 `payloadVer`）。
- **环境**：压测端 `shimmer-bi-MS-7D17`，Linux 7.0.0 x86_64，Intel i5-10400F（12 逻辑核），
  `go1.27.3`（`ltdriver` 构建信息 `go version -m` 同值）；服务端 `10.10.1.23`
  （`docker-compose-03`，16 核）；压测端 NTP offset +34.263ms、服务端 +1.313ms，
  每轮 `clock_offset_drift_ms` < 1.4ms（可用）。
- **命令**：
  `bin/ltdriver -players 4096 -per-battle 10 -transport ws -via edge -pps 20 -probe 1s -probe-jitter -duration 60s`
- **条件**：ws 面 4096 流经接入层（`-via edge`）、410/411 局、实际 9.97–9.99 人/局、
  `tick_interval_ms=100`、`max_frames=20000`、`frame_broadcast_protobuf=true`、
  `memory_mode` 关闭；A 组 `frame_broadcast_delta` 缺省（false），B 组两 battle 均 true；
  双 battle + 双 battle-frame 注册、edge `max_streams=8192`、UDP 收缓冲 `rb16777216`，
  preflight 两组均通过。
- **口径**：每帧字节 = `downlink_bytes_total / broadcasts_received`（含 16B 帧头）；
  下行载荷 = `downlink_payload_bytes_total`；battle 实写 = 两 battle 进程 `io` 的 `wchar` 之和；
  写 syscall = 六进程 `syscw` 之和；包数 = 压测端网卡 `tx` 包计数差；
  探针 p99 = `probe_ok_rtt_ms.p99`；帧失败 = `frame_failures`。

### 5.2 A/B 对照（三轮，A=delta 关 / B=delta 开）

| 指标 | delta 关（A/A2/A3） | delta 开（B/B2/B3） | 变化 |
|---|---|---|---|
| 每帧字节 | 520.7 / 520.5 / 520.4 B | 49.6 / 49.8 / 49.9 B | **−90.5%** |
| 下行载荷 | 1266.7 MB | 84.4 MB | −93% |
| battle 实写 | 1535 / 1535 / 1541 MB | 344 / 348 / 349 MB | −77% |
| 客户端 v3 帧 | 0 | 249 万（99.5%） | — |
| 包数 | 7.88 / 7.91 / 8.39 M | 7.85 / 7.99 / 8.30 M | ±1% |
| 写 syscall | 19.93 / 20.06 / 21.32 M | 19.93 / 20.28 / 21.35 M | ±1% |
| 帧失败 | 390 / 913 / 3540 | 1398 / 601 / 1 | 不升（B 整体低于 A 的波动上界） |
| 探针 p99 | 212.8 / 211.9 / 45.9 ms | 212.0 / 210.5 / 33.3 ms | 不劣化 |

补充：B 组三轮协商回执均为 `{3: 4096}`；实收帧 v2/v3 = 12,451/2,499,444、
13,823/2,520,742、14,380/2,493,051（v3 占比 99.4–99.5%）；`delta_drops` 173/1568/2096、
`keyframe_requests` 169/1551/2095，**`decode_failures` 全为 0**。A 组协商 `{2: 4096}`、
v3=0、drops/requests=0。

### 5.3 结论

1. **ver=3 真正生效**：99.5% 的下行帧走增量，客户端按 `base_seq` 合并后交付；失步走
   「丢弃 + 请求关键帧」恢复（请求数与丢弃数同量级，无解码失败）。
2. **带宽收益与设计预算同量级**：每帧 520→49.6 B（−90.5%），下行载荷 1266.7→84.4 MB（−93%），
   battle 进程实写 1535→344 MB（−77%，低于载荷降幅：关键帧、日志与其它写仍在）。
3. **包数 / 写 syscall 不变**（±1%）——验证了「delta 只省字节、不省包数」的设计前提。
4. **尾延迟与可靠性未劣化**：探针 p99 33.3–212.0ms 对 A 的 45.9–212.8ms；帧失败 B 组三轮
   整体更低（1–1398 对 390–3540）。
5. §三的「协商到 3 却全量下发」已由 `55d255f` 根治（根因：传输层重连 `resetState` 抹掉
   `payloadVer`），本节即修复后的验收数据。

### 5.4 复现与产物

- 现场产物（9.36）：`~/delta-round/runs/delta2-{A,A2,A3,B,B2,B3}/`
  （`summary.json`、`driver.log`、`cpu.txt`、`io0/io1.txt`、`tx0/tx1.txt`、`preflight.txt`、
  `snap-t0/t1.txt`、`config.txt`）。
- 驱动：`scripts/lt-driver/`（`client.go` 声明 `accept_payload_versions=[1,2,3]`、
  用 `lib/downlink` 解码合并并统计字节/版本；本轮已提交）。
- 编排：`~/delta-round/run2.sh`（部署→配置→重启→preflight→快照→60s→汇总）；A/B 各三轮。
- 还原：两 battle 的 `frame_broadcast_delta` 已删除（=false），六进程已重启，
  `frame_broadcast_protobuf=true` 保持。
