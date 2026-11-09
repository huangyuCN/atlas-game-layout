package actor

import (
	"testing"
	"time"
)

// TestInputRetentionFrames 钉住帧输入日志的保留口径：覆盖「玩家还能重连的时长」×2，
// 且有下限，避免未配 offline_timeout 的测试场景无限增长。
//
// 背景（2026-11-09）：日志整局保留时 10 人局 × 30 Hz 每帧约 1.1KB、410 局并发约 9GB，
// battle RSS 涨到 8.4GB 把 GC 拖死，30 Hz 长跑 100–200 秒整体崩塌。
func TestInputRetentionFrames(t *testing.T) {
	const ms = int64(time.Millisecond)
	cases := []struct {
		name    string
		offline time.Duration
		tick    int64
		want    uint64
	}{
		{"30Hz/20s 掉线窗口 → ×2 余量", 20 * time.Second, 33 * ms, 1212},
		{"10Hz/20s 掉线窗口 → 触及下限 600", 20 * time.Second, 100 * ms, 600},
		{"未配掉线窗口 → 取下限", 0, 33 * ms, 600},
		{"帧间隔非法 → 不裁剪", 20 * time.Second, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := uint64(inputRetentionFrames(c.offline, c.tick)); got != c.want {
				t.Fatalf("保留帧数 = %d，期望 %d", got, c.want)
			}
		})
	}
}
