"""把 loadtest 的 SUMMARY 行汇总成对比表（分段 p95/p99 + 中位数/极差）。

用法：python3 analyze.py <summary 文件> [标题]
"""
import json
import statistics as st
import sys

FIELDS = (("full", "全程 frame_rtt"), ("dial", "建流窗口"), ("steady", "稳态窗口"), ("dl", "下行广播"))


def load(path):
    """读取按 [tag] 前缀标注的 SUMMARY 行。"""
    rows = []
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line.startswith("["):
            continue
        tag = line[1:line.index("]")].strip()
        rows.append((tag, json.loads(line[line.index("{"):])))
    return rows


def line_of(tag, r):
    """把一条 SUMMARY 渲染为定宽行。"""
    f, d, s, dl = r["frame_rtt_ms"], r["dial_rtt_ms"], r["steady_rtt_ms"], r["broadcast_downlink_ms"]
    return (f"{tag:<13}{r['connections_established']:>5}{r['frame_failures']:>5} |"
            f"{f['p50_ms']:>8.2f}{f['p95_ms']:>7.2f}{f['p99_ms']:>7.2f} |"
            f"{d['p95_ms']:>8.2f}{d['p99_ms']:>7.2f}{d['count']:>6} |"
            f"{s['p95_ms']:>10.2f}{s['p99_ms']:>7.2f}{s['count']:>6} |"
            f"{dl['p99_ms']:>7.2f}{r['broadcast_pps']:>9.0f}")


def main(path, title):
    """打印逐轮表与按臂汇总。"""
    rows = load(path)
    print(f"### {title}（轮次 {len(rows)}）")
    print(f"{'tag':<13}{'conn':>5}{'fail':>5} |{'full p50':>8}{'p95':>7}{'p99':>7} |"
          f"{'dial p95':>8}{'p99':>7}{'n':>6} |{'steady p95':>10}{'p99':>7}{'n':>6} |"
          f"{'dl p99':>7}{'bcast/s':>9}")
    agg = {}
    for tag, r in rows:
        print(line_of(tag, r))
        a = agg.setdefault(r["via"], {k: [] for k, _ in FIELDS})
        a["full"].append(r["frame_rtt_ms"]["p99_ms"])
        a["dial"].append(r["dial_rtt_ms"]["p99_ms"])
        a["steady"].append(r["steady_rtt_ms"]["p99_ms"])
        a["dl"].append(r["broadcast_downlink_ms"]["p99_ms"])
    print("--- 汇总（p99 中位数 / 极差）---")
    for via, a in agg.items():
        for k, label in FIELDS:
            xs = a[k]
            print(f"{via:<7}{label:<14} 中位 {st.median(xs):7.2f}  极差 [{min(xs):7.2f}, {max(xs):7.2f}]"
                  f"  散布 {max(xs) - min(xs):6.2f}  n={len(xs)}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else sys.argv[1])
