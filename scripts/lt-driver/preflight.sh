#!/usr/bin/env bash
# preflight.sh — 开测前「一条命令打印 11 项」的检查脚本（在压测端 10.10.9.36 上运行）。
#
# 用法：
#   ./scripts/lt-driver/preflight.sh                 # 默认 SERVER=ubuntu@10.10.1.23
#   SERVER=ubuntu@10.10.1.23 DEPLOY=/home/ubuntu/cap-deploy ./preflight.sh
#
# 打印项（顺序固定）：
#   1 压测端是谁   2 服务端是谁   3 NTP Δ（含跨机直测）  4 双 battle 进程数
#   5 注册条数（battle / battle-frame 各 2）  6 rb16777216  7 配置生效值（含局人数）
#   8 部署文件 sha256   9 运行中进程 sha256（/proc/<pid>/exe，双 battle 必须同源）
#   10 端口/健康   11 判据汇总
#
# 硬门槛（9/11）：双 battle 的 bin/battle sha256 必须一致。两台不同源 ⇒ 同 battle_id 的目录
#   属主互相覆盖 ⇒ 开跑后帧改投另一台 ⇒ 一局跑不完（本轮 250 局只跑 15 局、5,000 人数据作废
#   正是这个原因）。口径：读运行中进程的 /proc/<pid>/exe 再算 sha256，不读部署文件 ——
#   文件被换而进程仍是旧进程时，读文件会误判「同源」。
#
# 退出码：全部通过 0，有失败 1（便于开测脚本 gate）。
set -uo pipefail

SERVER="${SERVER:-ubuntu@10.10.1.23}"
DEPLOY="${DEPLOY:-/home/ubuntu/cap-deploy}"
B2="${B2:-/home/ubuntu/cap-deploy-b2}"
ETCD_CONTAINER="${ETCD_CONTAINER:-deploy-etcd-1}"
GATEWAY="${GATEWAY:-10.10.1.23:9001}"
# 2 人/局（MaxSide=1）的 matcher 二进制摘要；与部署值不同即判「局人数不可确认」。
EXPECT_MATCHER_SHA="${EXPECT_MATCHER_SHA:-79b625f4546f00402c7249b28b92c8d556b411fe1c1cc02b4f8f2029636dd0d8}"
# 其余服务的预期摘要集合（空格分隔，命中任一即通过）；留空只打印实际值供人工核对。
EXPECT_GATEWAY_SHA="${EXPECT_GATEWAY_SHA:-}"
EXPECT_GAME_SHA="${EXPECT_GAME_SHA:-}"
EXPECT_EDGE_SHA="${EXPECT_EDGE_SHA:-}"

FAIL=0
R() { ssh -o BatchMode=yes -o ConnectTimeout=5 "$SERVER" "$@"; }
hr() { printf '\n== %s ==\n' "$*"; }
bad() { FAIL=$((FAIL+1)); printf '  [FAIL] %s\n' "$*"; }
ok() { printf '  [ok] %s\n' "$*"; }

hr "1/11 压测端是谁"
printf '  hostname: %s\n  ip: %s\n  cpu: %s\n  time: %s\n' \
  "$(hostname)" "$(hostname -I 2>/dev/null | awk '{print $1}')" "$(nproc)" "$(date -Is)"

hr "2/11 服务端是谁"
R 'printf "  hostname: %s\n  ip: %s\n  cpu: %s\n  time(UTC): %s\n  uptime: %s\n" \
   "$(hostname)" "$(hostname -I | awk "{print \$1}")" "$(nproc)" "$(date -u -Is)" "$(uptime -p 2>/dev/null)"'

hr "3/11 NTP Δ（压测端 - 服务端）"
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

hr "4/11 双 battle 进程数（服务端）"
b_all="$(R "pgrep -fc 'bin/battle$'" 2>/dev/null)"
printf '  battle 进程合计: %s 个（两进程可分别来自 cap-deploy / cap-deploy-b2）\n' "${b_all:-0}"
R "pgrep -af 'bin/battle$' | sed 's/^/    /'"
[ "${b_all:-0}" = "2" ] && ok "双 battle 进程 2 个" || bad "battle 进程数不是 2"

hr "5/11 注册条数（etcd /atlas/services/cap）"
# 注册中心 etcd 在压测端本机（10.10.9.36:12379 → 容器 $ETCD_CONTAINER），不要经 SERVER 查。
reg="$(docker exec "$ETCD_CONTAINER" etcdctl get --prefix --keys-only /atlas/services/cap 2>/dev/null)"
n_reg="$(printf '%s\n' "$reg" | grep -c . || true)"
n_battle="$(printf '%s\n' "$reg" | grep -c '/battle/' || true)"
n_bframe="$(printf '%s\n' "$reg" | grep -c 'battle-frame' || true)"
printf '  服务注册键总数: %s；battle: %s；battle-frame: %s\n' "${n_reg:-0}" "${n_battle:-0}" "${n_bframe:-0}"
printf '%s\n' "$reg" | sed 's/^/    /'
[ "${n_battle:-0}" = "2" ] && ok "battle 注册 2 条（双 battle）" || bad "battle 注册不是 2 条"
[ "${n_bframe:-0}" = "2" ] && ok "battle-frame 注册 2 条（双 battle）" || bad "battle-frame 注册不是 2 条"

hr "6/11 内核缓冲 rb16777216（服务端）"
R 'printf "  rmem_max=%s wmem_max=%s\n" "$(sysctl -n net.core.rmem_max)" "$(sysctl -n net.core.wmem_max)"; \
   printf "  udp:7102 %s\n" "$(ss -ulm -n "sport = :7102" | grep -o "rb[0-9]*" | head -1)"'
rb="$(R "ss -ulm -n 'sport = :7102' | grep -o 'rb[0-9]*' | head -1" 2>/dev/null)"
[ "$rb" = "rb16777216" ] && ok "UDP 收缓冲 rb16777216" || bad "UDP 收缓冲 ${rb:-缺失}（应按 16MB）"

hr "7/11 配置生效值（含局人数）"
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
  ok "局人数 = 2 人/局（matcher MaxSide=1，部署文件 sha 与预期一致）"
else
  bad "局人数不可确认（部署文件 matcher sha=${m_sha:0:12}… ≠ 2 人/局预期 ${EXPECT_MATCHER_SHA:0:12}…；默认可能是 5v5=10 人/局）"
fi

hr "8/11 部署文件 sha256（服务端文件，仅作对照；门禁看 9/11 的进程口径）"
R "cd $DEPLOY && sha256sum bin/gateway bin/game bin/matcher bin/battle bin/edge 2>/dev/null | sed 's/^/  /'"
R "sha256sum $B2/bin/battle 2>/dev/null | sed 's/^/  /'"

hr "9/11 运行中进程 sha256（读 /proc/<pid>/exe；双 battle 必须同源）"
# 采集在服务端执行：对每个进程读 /proc/<pid>/exe（内核指向进程实际映像的 inode）再算 sha256。
# 不读 <部署目录>/bin/*：文件被换而进程未重启时二者会不同，只有 /proc/<pid>/exe 反映真实运行体。
psha() {
  R "for p in \$(pgrep -f 'bin/$1\$' 2>/dev/null); do printf '%s %s %s\n' \"\$p\" \"\$(sha256sum /proc/\$p/exe 2>/dev/null | cut -d' ' -f1)\" \"\$(readlink /proc/\$p/exe 2>/dev/null)\"; done"
}
in_set() { case " $2 " in *" $1 "*) return 0 ;; esac; return 1; }
# psha1 <服务名> <预期摘要集合>：单实例服务；有预期就比对，没设预期只打印实际值供人工核对。
psha1() {
  local name="$1" want="$2" rows n pid sha exe
  rows="$(psha "$name" 2>/dev/null)"
  n="$(printf '%s\n' "$rows" | grep -c . || true)"
  if [ "$n" != "1" ]; then
    bad "$name 运行中进程 ${n:-0} 个（应为 1 个，无法确认运行体）"
    printf '%s\n' "$rows" | sed 's/^/    /'
    return
  fi
  pid="$(printf '%s' "$rows" | awk '{print $1}')"
  sha="$(printf '%s' "$rows" | awk '{print $2}')"
  exe="$(printf '%s' "$rows" | awk '{print $3}')"
  printf '  %-8s pid=%-8s sha256=%s\n           exe=%s\n' "$name" "$pid" "${sha:-<读不到>}" "${exe:-N/A}"
  if [ -z "$sha" ]; then
    bad "$name 读不到 /proc/${pid}/exe 的 sha256（权限不足或进程已退出）"
  elif [ -z "$want" ]; then
    printf '  [人工核对] %s 未设预期集合（EXPECT_%s_SHA），请人工确认上面的 sha 是本轮该跑的版本\n' \
      "$name" "$(printf '%s' "$name" | tr '[:lower:]' '[:upper:]')"
  elif in_set "$sha" "$want"; then
    ok "$name 运行中 sha256 在预期集合内"
  else
    bad "$name 运行中 sha256=${sha:0:16}… 不在预期集合内（预期 ${want}）"
  fi
}
psha1 gateway "$EXPECT_GATEWAY_SHA"
psha1 game "$EXPECT_GAME_SHA"
psha1 matcher "$EXPECT_MATCHER_SHA"
psha1 edge "$EXPECT_EDGE_SHA"

# 硬门槛：双 battle 的运行中 exe sha256 必须完全一致（不同源 = 禁止开测）。
b_rows="$(psha battle 2>/dev/null)"
b_n="$(printf '%s\n' "$b_rows" | grep -c . || true)"
printf '  battle 进程 %s 个（pid / sha256 / exe）：\n' "${b_n:-0}"
printf '%s\n' "$b_rows" | while read -r pid sha exe; do
  printf '    pid=%-8s sha256=%s  exe=%s\n' "$pid" "${sha:-<读不到>}" "${exe:-N/A}"
done
if [ "${b_n:-0}" != "2" ]; then
  bad "battle 运行中进程 ${b_n:-0} 个（硬门槛要求 2 个才能比对同源）"
elif printf '%s\n' "$b_rows" | awk '{print $2}' | grep -qv '^[0-9a-f]\{64\}$'; then
  bad "双 battle 中有进程读不到 /proc/<pid>/exe 的 sha256（见上表 <读不到>）"
elif [ "$(printf '%s\n' "$b_rows" | awk '{print $2}' | sort -u | wc -l)" = "1" ]; then
  ok "双 battle 同源（两进程 exe sha256 一致：$(printf '%s' "$b_rows" | awk 'NR==1{print $2}')）"
else
  bad "双 battle 的 bin/battle sha256 不一致（不同源 ⇒ 同 battle_id 目录属主互相覆盖 ⇒ 禁止开测）："
  printf '%s\n' "$b_rows" | awk '{printf "         sha256=%s  (pid=%s, exe=%s)\n", $2, $1, $3}'
fi

hr "10/11 端口与健康"
R "ss -ltn | grep -E ':(9001|7100|7101|7102|9200|9201|9280|9480)\b' | sed 's/^/  /'" || true
# 健康端点可能只绑服务端 LAN 地址（不是 127.0.0.1）：先试回环，再试服务端第一个 IP。
health="$(R "for h in 127.0.0.1 \$(hostname -I | awk '{print \$1}'); do \
  c=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://\$h:9480/health); \
  if [ \"\$c\" = 200 ]; then echo \$c; break; fi; done" 2>/dev/null)"
[ "$health" = "200" ] && ok "网关 /health 200" || bad "网关 /health=${health:-连不上}"

hr "11/11 判据汇总"
if [ "$FAIL" -eq 0 ]; then
  ok "preflight 全部通过（可以开测）"
else
  bad "preflight 有 $FAIL 项失败（不要开测）"
fi
exit $([ "$FAIL" -eq 0 ] && echo 0 || echo 1)
