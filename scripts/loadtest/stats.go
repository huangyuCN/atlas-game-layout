// stats.go 提供压测延迟样本的采集与分位数统计（只依赖标准库，便于单测与复用）。
package main

import (
	"sort"
	"sync"
	"time"
)

// samples 是并发安全的延迟样本集（内部以毫秒存储）。
type samples struct {
	mu sync.Mutex
	xs []float64
}

// add 记录一次延迟样本（非正值忽略：未打点或时钟回拨都不该污染分位数）。
func (s *samples) add(d time.Duration) {
	if d <= 0 {
		return
	}
	ms := float64(d.Microseconds()) / 1000
	s.mu.Lock()
	s.xs = append(s.xs, ms)
	s.mu.Unlock()
}

// merge 把另一组样本并入本组（汇总各连接的样本用）。
func (s *samples) merge(other *samples) {
	if other == nil {
		return
	}
	other.mu.Lock()
	xs := append([]float64(nil), other.xs...)
	other.mu.Unlock()
	s.mu.Lock()
	s.xs = append(s.xs, xs...)
	s.mu.Unlock()
}

// sorted 返回升序排序后的样本副本。
func (s *samples) sorted() []float64 {
	s.mu.Lock()
	out := append([]float64(nil), s.xs...)
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
