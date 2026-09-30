// stats_test.go 覆盖延迟样本的时间窗切片（分段 p99 的正确性前提：窗口边界不得串味）。
package main

import (
	"testing"
	"time"
)

// TestSamplesWindowSplitsByTimestamp 覆盖按打点时刻切片：窗口外样本不进分位数，零值边界表示不限。
func TestSamplesWindowSplitsByTimestamp(t *testing.T) {
	t0 := time.Now()
	s := &samples{xs: []sample{
		{at: t0, ms: 1},
		{at: t0.Add(2 * time.Second), ms: 2},
		{at: t0.Add(4 * time.Second), ms: 3},
	}}
	cases := []struct {
		name string
		from time.Time
		to   time.Time
		want int
	}{
		{"全区间", time.Time{}, time.Time{}, 3},
		{"仅后半段", t0.Add(3 * time.Second), time.Time{}, 1},
		{"含两端边界", t0, t0.Add(2 * time.Second), 2},
		{"空窗口", t0.Add(10 * time.Second), time.Time{}, 0},
	}
	for _, c := range cases {
		if got := len(s.window(c.from, c.to)); got != c.want {
			t.Errorf("%s: 样本数应为 %d，实际 %d", c.name, c.want, got)
		}
	}
}

// TestSummarizeEmptyAndSorted 覆盖空样本摘要为零值、分位数取值有序。
func TestSummarizeEmptyAndSorted(t *testing.T) {
	if got := summarize(nil); got.Count != 0 || got.P99 != 0 {
		t.Fatalf("空样本摘要应为零值，实际 %+v", got)
	}
	got := summarize([]float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	// 分位数口径：idx = int(q*(n-1))（10 个样本时 p99 → 下标 8）。
	if got.P50 != 5 || got.P99 != 9 || got.Max != 10 {
		t.Fatalf("分位数取值不符：%+v", got)
	}
}
