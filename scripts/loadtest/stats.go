// stats.go 提供压测延迟样本的采集与分位数统计（只依赖标准库，便于单测与复用）。
//
// 样本同时保留**打点时刻**：端到端 p99 会被「建流窗口」（512 条流同时准入/入局）的
// 慢样本污染，故报告里把同一批探针按时间窗切成「建流窗口 vs 稳态窗口」两段分别给分位数。
package main

import (
	"sort"
	"sync"
	"time"
)

// sample 是一次延迟打点：时长（毫秒）+ 打点时刻（按窗口切片用）。
type sample struct {
	at time.Time
	ms float64
}

// samples 是并发安全的延迟样本集（内部以毫秒存储，保留打点时刻）。
type samples struct {
	mu sync.Mutex
	xs []sample
}

// add 记录一次延迟样本（非正值忽略：未打点或时钟回拨都不该污染分位数）。
func (s *samples) add(d time.Duration) {
	if d <= 0 {
		return
	}
	ms := float64(d.Microseconds()) / 1000
	s.mu.Lock()
	s.xs = append(s.xs, sample{at: time.Now(), ms: ms})
	s.mu.Unlock()
}

// merge 把另一组样本并入本组（汇总各连接的样本用）。
func (s *samples) merge(other *samples) {
	if other == nil {
		return
	}
	other.mu.Lock()
	xs := append([]sample(nil), other.xs...)
	other.mu.Unlock()
	s.mu.Lock()
	s.xs = append(s.xs, xs...)
	s.mu.Unlock()
}

// sorted 返回升序排序后的全部样本副本。
func (s *samples) sorted() []float64 { return s.window(time.Time{}, time.Time{}) }

// window 返回打点时刻落在 [from, to] 内的样本（升序）；零值 from/to 表示该侧不限。
func (s *samples) window(from, to time.Time) []float64 {
	s.mu.Lock()
	out := make([]float64, 0, len(s.xs))
	for _, x := range s.xs {
		if !from.IsZero() && x.at.Before(from) {
			continue
		}
		if !to.IsZero() && x.at.After(to) {
			continue
		}
		out = append(out, x.ms)
	}
	s.mu.Unlock()
	sort.Float64s(out)
	return out
}

// latency 是一组延迟的分位数摘要（毫秒）。
type latency struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

// summarize 计算分位数摘要（空样本返回零值摘要）。
func summarize(sorted []float64) latency {
	if len(sorted) == 0 {
		return latency{}
	}
	return latency{
		Count: len(sorted),
		P50:   percentile(sorted, 0.50),
		P95:   percentile(sorted, 0.95),
		P99:   percentile(sorted, 0.99),
		Max:   sorted[len(sorted)-1],
	}
}

// percentile 取分位数（输入须已升序；空输入返回 0）。
func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)-1))
	return sorted[idx]
}
