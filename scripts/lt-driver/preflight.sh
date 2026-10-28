#!/usr/bin/env bash
# preflight.sh — 开测前「一条命令打印 10 项」的检查脚本（在压测端 10.10.9.36 上运行）。
#
# 用法：
#   ./scripts/lt-driver/preflight.sh                 # 默认 SERVER=ubuntu@10.10.1.23
#   SERVER=ubuntu@10.10.1.23 DEPLOY=/home/ubuntu/cap-deploy ./preflight.sh
#
# 打印项（顺序固定）：
#   1 压测端是谁   2 服务端是谁   3 NTP Δ（含跨机直测）  4 双 battle 进程数
#   5 注册条数     6 rb16777216   7 配置生效值（含局人数） 8 二进制 sha256
#   9 端口/健康   10 判据汇总
# 退出码：全部通过 0，有失败 1（便于开测脚本 gate）。
set -uo pipefail

SERVER="${SERVER:-ubuntu@10.10.1.23}"
DEPLOY="${DEPLOY:-/home/ubuntu/cap-deploy}"
B2="${B2:-/home/ubuntu/cap-deploy-b2}"
ETCD_CONTAINER="${ETCD_CONTAINER:-deploy-etcd-1}"
GATEWAY="${GATEWAY:-10.10.1.23:9001}"
# 2 人/局（MaxSide=1）的 matcher 二进制摘要；与部署值不同即判「局人数不可确认」。
EXPECT_MATCHER_SHA="${EXPECT_MATCHER_SHA:-79b625f4546f00402c7249b28b92c8d556b411fe1c1cc02b4f8f2029636dd0d8}"

FAIL=0
R() { ssh -o BatchMode=yes -o ConnectTimeout=5 "$SERVER" "$@"; }
hr() { printf '\n== %s ==\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '  [FAIL] %s\n' "$*"; }
ok() { printf '  [ok] %s\n' "$*"; }

hr "1/10 压测端是谁"
printf '  hostname: %s\n  ip: %s\n  cpu: %s\n  time: %s\n' \
  "$(hostname)" "$(hostname -I 2>/dev/null | awk '{print $1}')" "$(nproc)" "$(date -Is)"

hr "2/10 服务端是谁"
R 'printf "  hostname: %s\n  ip: %s\n  cpu: %s\n  time(UTC): %s\n  uptime: %s\n" \
   "$(hostname)" "$(hostname -I | awk "{print \$1}")" "$(nproc)" "$(date -u -Is)" "$(uptime -p 2>/dev/null)"'

hr "3/10 NTP Δ（压测端 - 服务端）"
loc_off="$(timedatectl timesync-status 2>/dev/null | awk -F': ' '/^ *Offset:/{print $2}')"
rem_off="$(R "timedatectl timesync-status 2>/dev/null | awk -F': ' '/^ *Offset:/{print \$2}'")"
printf '  压测端 NTP 自评 Offset: %s\n  服务端 NTP 自评 Offset: %s\n' "${loc_off:-N/A}" "${rem_off:-N/A}"
# 跨机直测：先建 SSH 多路复用主连接（否则每次握手的耗时会被算进偏移）。
master="/tmp/preflight-clock-$$"
SSHM=(ssh -o BatchMode=yes -o ConnectTimeout=5 -o ControlMaster=auto -o ControlPath="$master" -o ControlPersist=60)
if "${SSHM[@]}" "$SERVER" true >/dev/null 2>&1; then
  printf '  跨机直测（取最小 RTT 样本）：\n'
  for _ in 1 2 3 4 5; do
    t0=$(date +%s%N); t1=$("${SSHM[@]}" "$SERVER" 'date +%s%N' 2>/dev/null); t3=$(date +%s%N)
    [ -n "$t1" ] && awk -v a="$t0" -v b="$t1" -v c="$t3" \
      'BEGIN{printf "    Δ=%+.3fms  rtt=%.3fms\n", -(b-(a+c)/2)/1e6, (c-a)/1e6}'
    sleep 0.05
  done
  ssh -O exit -o ControlPath="$master" "$SERVER" >/dev/null 2>&1
else
  bad "跨机 SSH 直测不可用（只保留两端 NTP 自评）"
fi
printf '  判据：轮首/轮尾各测一次，|漂移| ≤ 5ms 该轮才可用（驱动写进 SUMMARY 的 clock_offset_drift_ms）\n'

hr "4/10 双 battle 进程数（服务端）"
b_all="$(R "pgrep -fc 'bin/battle$'" 2>/dev/null)"
printf '  battle 进程合计: %s 个（两进程可分别来自 cap-deploy / cap-deploy-b2）\n' "${b_all:-0}"
R "pgrep -af 'bin/battle$' | sed 's/^/    /'"
[ "${b_all:-0}" = "2" ] && ok "双 battle 进程 2 个" || bad "battle 进程数不是 2"

hr "5/10 注册条数（etcd /atlas/services/cap）"
# 注册中心 etcd 在压测端本机（10.10.9.36:12379 → 容器 $ETCD_CONTAINER），不要经 SERVER 查。
reg="$(docker exec "$ETCD_CONTAINER" etcdctl get --prefix --keys-only /atlas/services/cap 2>/dev/null)"
n_reg="$(printf '%s\n' "$reg" | grep -c . || true)"
n_battle="$(printf '%s\n' "$reg" | grep -c 'battle-frame' || true)"
printf '  服务注册键总数: %s；battle-frame: %s\n' "${n_reg:-0}" "${n_battle:-0}"
printf '%s\n' "$reg" | sed 's/^/    /'
[ "${n_battle:-0}" = "2" ] && ok "battle-frame 注册 2 条（双 battle）" || bad "battle-frame 注册不是 2 条"

hr "6/10 内核缓冲 rb16777216（服务端）"
R 'printf "  rmem_max=%s wmem_max=%s\n" "$(sysctl -n net.core.rmem_max)" "$(sysctl -n net.core.wmem_max)"; \
   printf "  udp:7102 %s\n" "$(ss -ulm -n "sport = :7102" | grep -o "rb[0-9]*" | head -1)"'
rb="$(R "ss -ulm -n 'sport = :7102' | grep -o 'rb[0-9]*' | head -1" 2>/dev/null)"
[ "$rb" = "rb16777216" ] && ok "UDP 收缓冲 rb16777216" || bad "UDP 收缓冲 ${rb:-缺失}（应按 16MB）"

hr "7/10 配置生效值（含局人数）"
printf '  -- battle1 --\n'
R "grep -nE '^[[:space:]]*(max_frames|tick_interval_ms|frame_broadcast_protobuf|max_players|seat_window_ms):' $DEPLOY/services/battle/configs/config.yaml | sed 's/^/    /'"
R "grep -nE '^[[:space:]]*directory:' -A3 $DEPLOY/services/battle/configs/config.yaml | grep -E 'directory|backend' | sed 's/^/    /'"
printf '  -- battle2 --\n'
R "grep -nE '^[[:space:]]*(max_frames|tick_interval_ms|frame_broadcast_protobuf|max_players|seat_window_ms):' $B2/services/battle/configs/config.yaml | sed 's/^/    /'"
printf '  -- edge --\n'
R "grep -nE '^[[:space:]]*(max_streams|max_per_ip|new_conn_rate|new_conn_burst):' $DEPLOY/services/edge/configs/config.yaml | sed 's/^/    /'"
R "grep -nE '^[[:space:]]*directory:' -A3 $DEPLOY/services/edge/configs/config.yaml | grep -E 'directory|backend' | sed 's/^/    /'"
m_sha="$(R "sha256sum $DEPLOY/bin/matcher" 2>/dev/null | awk '{print $1}')"
if [ "$m_sha" = "$EXPECT_MATCHER_SHA" ]; then
  ok "局人数 = 2 人/局（matcher MaxSide=1，sha 与预期一致）"
else
  bad "局人数不可确认（matcher sha=${m_sha:0:12}… ≠ 2 人/局预期 ${EXPECT_MATCHER_SHA:0:12}…；默认可能是 5v5=10 人/局）"
fi

hr "8/10 二进制 sha256（服务端）"
R "cd $DEPLOY && sha256sum bin/gateway bin/game bin/matcher bin/battle bin/edge 2>/dev/null | sed 's/^/  /'"
R "sha256sum $B2/bin/battle 2>/dev/null | sed 's/^/  /'"

hr "9/10 端口与健康"
R "ss -ltn | grep -E ':(9001|7100|7101|7102|9200|9201|9280|9480)\b' | sed 's/^/  /'" || true
health="$(R "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9480/health" 2>/dev/null)"
[ "$health" = "200" ] && ok "网关 /health 200" || bad "网关 /health=${health:-连不上}"

hr "10/10 判据汇总"
if [ "$FAIL" -eq 0 ]; then
  ok "preflight 全部通过（可以开测）"
else
  bad "preflight 有 $FAIL 项失败（不要开测）"
fi
exit $([ "$FAIL" -eq 0 ] && echo 0 || echo 1)
