#!/bin/bash
# start-services.sh — 部署启动脚本：拉起 gateway/game/matcher/battle/edge（可再起第二份 battle）
#
# 用法：
#   ./scripts/start-services.sh                      # 不注入内存上限（对照口径）
#   BATTLE_GOMEMLIMIT=2GiB EDGE_GOMEMLIMIT=224MiB ./scripts/start-services.sh
#   SERVICES_DIR=/path/to/cap-deploy ./scripts/start-services.sh   # 指定部署目录（内含 bin/ 与 services/）
#
# 目录约定：$SERVICES_DIR/bin/<服务>（二进制）、$SERVICES_DIR/services/<服务>/configs/config.yaml（配置）、
#           $SERVICES_DIR2/bin/battle（可选第二份 battle，缺省 = 与 SERVICES_DIR 相同）。
#
# ── GC 调优：GOMEMLIMIT 的位置与初值口径 ─────────────────────────────────────────
# GOMEMLIMIT 是 Go 运行时的**绝对软上限**：堆越接近上限 GC 越积极，逼近上限自动收紧。
# 但**只设上限不会放宽 GC**——默认 GOGC=100 的触发点是「存活堆×2」，只要该点低于上限，
# GC 仍按 GOGC 节奏跑。4096 人局实测（同一批二进制、tick=100ms）：
#   不设上限 10.27 核 / 探针 p99 42.2ms / 广播 p99 35.1ms / RSS 加总 2.5GB
#   设 2GiB   10.42 核 / 探针 p99 47.2ms / 广播 p99 38.1ms / RSS 加总 2.6GB（零收益，符合上述机理）
# 故本仓库的定位是：上限用作**安全阀**（防 5000/1 万人规模把堆堆爆），CPU 收益要靠 GOGC 或业务侧，
# 不要指望单靠 GOMEMLIMIT 复现 GOGC=400 的 −9.4%。历史上 GOGC 类调优的收益与代价（供参考）：
#   CPU −9.4%、探针 p99 36.1→17.7ms、帧失败 95→1，代价 RSS +1.18GB。
#
# 初值口径 = **该规模实测稳态 RSS × 1.6**（留 60% 余量，既当安全阀又不至于一压就顶到上限）：
#   4096 人局：battle ≈ 1.26GB → 2GiB（1.63×）；edge ≈ 140MB → 224MiB（1.60×）
#   5000 人局：battle ≈ 1.95GB → 3GiB（1.58×）—— 实测上限低于 1.6×RSS 时（2GiB@5000）
#   会变成绑定约束：GC 更频繁、探针 p99 反而更差（87.7ms vs 79.9ms）。上限务必按**目标规模**取。
#
# 两个变量留空 = 不注入（对照口径），便于同机 A/B 复测。
set -u
ulimit -n 1048575 2>/dev/null || true

SERVICES_DIR="${SERVICES_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
SERVICES_DIR2="${SERVICES_DIR2:-$SERVICES_DIR}"
BATTLE_GOMEMLIMIT="${BATTLE_GOMEMLIMIT:-}"
EDGE_GOMEMLIMIT="${EDGE_GOMEMLIMIT:-}"
SECOND_BATTLE="${SECOND_BATTLE:-1}"

# 注入 GOMEMLIMIT：仅 battle/edge 两个内存大户，其余服务堆很小、设上限反而徒增 GC 压力。
start_one() {
  local dir="$1" name="$2" lim="$3"
  mkdir -p "$dir/logs"
  ( cd "$dir" || exit 1
    if [ -n "$lim" ]; then
      setsid nohup env GOMEMLIMIT="$lim" ./bin/"$name" > "/tmp/$(basename "$dir")-$name.out" 2>&1 < /dev/null &
    else
      setsid nohup ./bin/"$name" > "/tmp/$(basename "$dir")-$name.out" 2>&1 < /dev/null &
    fi
    echo "$name $! dir=$dir gomemlimit=${lim:-unset}"
  )
}

limit_of() {
  case "$1" in
    battle) printf '%s' "$BATTLE_GOMEMLIMIT" ;;
    edge) printf '%s' "$EDGE_GOMEMLIMIT" ;;
    *) printf '' ;;
  esac
}

# battle_id_of <目录> — 取 runtime 段里显式配置的 id（未配 = 空串 = 依赖缺省 <服务名>-<主机名>）。
battle_id_of() {
  sed -n '/^runtime:/,/^[a-z]/p' "$1/services/battle/configs/config.yaml" 2>/dev/null |
    sed -n 's/^[[:space:]]*id:[[:space:]]*\([A-Za-z0-9_-]\{1,\}\).*/\1/p' | head -1
}

for s in gateway game matcher battle edge; do
  start_one "$SERVICES_DIR" "$s" "$(limit_of "$s")"
done

if [ "$SECOND_BATTLE" = "1" ] && [ -x "$SERVICES_DIR2/bin/battle" ]; then
  # 双 battle 必须各自显式配**不同**的 runtime.id：缺省 id = <服务名>-<主机名>，同主机两份
  # battle 会注册同一个键 /atlas/services/<ns>/battle/<id>，后注册方的注册被归属校验拒绝
  # （实例键仍被前者租约占用）⇒ 该实例在 battle/ 下缺失 ⇒ matcher 候选集塌成 1、
  # 全部对局压到另一台。故启动前就拦住，别等压测偏斜了再查。
  # 同目录双起不在校验范围（两会话端口本就冲突，起不来是立即暴露的错）。
  if [ "$SERVICES_DIR2" != "$SERVICES_DIR" ]; then
    id1="$(battle_id_of "$SERVICES_DIR")"
    id2="$(battle_id_of "$SERVICES_DIR2")"
    if [ -z "$id1" ] || [ -z "$id2" ] || [ "$id1" = "$id2" ]; then
      echo "start-services: 双 battle 需在 runtime.id 显式配置互不相同的实例 ID" >&2
      echo "  $SERVICES_DIR → '${id1:-<缺省>}'；$SERVICES_DIR2 → '${id2:-<缺省>}'" >&2
      echo "  缺省 = <服务名>-<主机名>，同主机两台必然同名 → 注册冲突 → battle/ 只留一台" >&2
      exit 1
    fi
  fi
  start_one "$SERVICES_DIR2" battle "$BATTLE_GOMEMLIMIT"
fi
