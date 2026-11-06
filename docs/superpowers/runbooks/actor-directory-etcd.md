# 运行手册：actor 目录与 etcd（6,000 人档"匹配崩溃"的处置）

> 适用：battle/game 的 actor 目录后端为 etcd（`actor.directory.backend: etcd`）的部署。
> 背景与完整证据链见框架仓 `docs/superpowers/benchmarks/接入层与热路径性能.md` §7.13。

## 1. 症状（长什么样）

- 压测/线上出现"匹配只成局一小部分人"：matcher 日志刷 `回池次数达上限，只发失败回执（不再重建票据）`；
- game 日志刷 `cluster: lease lost` + `cluster: lease reclaimed, activation restored`（成百上千条）；
- 同期 `renew cycle completed with issues renewed=N lost=M`，M 远大于 N。

## 2. 判据（三分钟定位）

| 看什么 | 正常 | 异常（本次事故） |
|---|---|---|
| `actor_renew_total{result="lost"}` | 0 | 一个周期 3,353 |
| `etcd_server_slow_apply_total` | 偶发 | 累计 **47 万**，且在涨 |
| `etcd_mvcc_db_total_size_in_bytes` / 活键数 | 成正比 | **1.7 GB 库只有 5,003 个活键** |
| etcd 日志 | 安静 | `apply request took too long ... 273ms` |

链条：etcd apply 变慢 → actor 目录续租（每键一次 `KeepAliveOnce`）跑不完 → 键过期 → 判 Lost →
冻结 + 重 Claim 风暴 → 入场（建战斗）失败 → matcher 回池 2 次后判失败 → 只成局一小部分人。

## 3. 处置

### 3.1 先做（非破坏性）

```bash
# 压缩历史 revision + 碎片整理（对在线成员安全）
docker exec deploy-etcd-1 etcdctl compact $(docker exec deploy-etcd-1 etcdctl endpoint status \
  --write-out=json | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["Status"]["header"]["revision"])')
docker exec deploy-etcd-1 etcdctl defrag --command-timeout=120s
```

### 3.2 若仍无效（测试环境）

**重建 etcd 数据**：actor 目录与注册键都是临时数据，服务重启会自动重建。

```bash
cd ~/atlas-loadtest/deploy && docker compose rm -sf etcd && docker compose up -d etcd
docker exec deploy-etcd-1 etcdctl endpoint health     # 期望：healthy，提案耗时 < 1ms
curl -s http://10.10.9.36:12379/metrics | grep -E '^etcd_server_slow_apply_total|^etcd_mvcc_db_total_size_in_bytes'
```

> 2026-11-06 实测：重建后库从 1.7 GB → 245 KB、`slow_apply_total` 归零，6,000 人档从
> "只成局 1,300–1,400 人" 变成 "6,000 全部成局、匹配 2,217 玩家/秒"。

### 3.3 隔离（可选）

etcd 与压测驱动同机时，把两者绑到不同核（本次仅做缓解，不是根因）：

```bash
docker update --cpuset-cpus "10-11" deploy-etcd-1
taskset -c 0-9 ./ltdriver ...        # 压测驱动避开 etcd 的核
```

## 4. 回滚

- 重建 etcd 不影响磁盘上的业务数据（本仓库测试环境的 actor 目录与注册信息均由服务在启动时重建）；
- 若重建后服务起不来：检查各服务 `registry.etcd.endpoints` 是否指向新实例、`namespaces` 是否一致。

## 5. 告警建议（待接入）

| 指标 | 阈值 |
|---|---|
| `actor_renew_total{result="lost"}` | 5 分钟内 > 0 即告警 |
| `etcd_server_slow_apply_total` 增速 | > 10/分钟 |
| `etcd_mvcc_db_total_size_in_bytes` ÷ 活键数 | > 100 KB/键（本次事故为 340 KB/键） |
