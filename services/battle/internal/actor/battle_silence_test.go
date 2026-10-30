package actor

import (
	"testing"
	"time"
)

// TestSilenceToleranceFollowsWindow 验证「客户端静默判负容忍时间」就是配置的掉线窗口
// （时间压缩口径：窗口 1.5s 落在配置合法区间 [1s, 300s] 内）：
// 静默 1s **不判负**（窗口内，容忍时间随配置放大——生产把它配成 20s 即「静默 10s 不判负」），
// 静默超过窗口**仍判负**（容忍不是无限，挂机占座另有上限 MaxOfflineTimeout）。
func TestSilenceToleranceFollowsWindow(t *testing.T) {
	cfg := offlineCfg(2)
	cfg.OfflineTimeout = 1500 * time.Millisecond
	env := newOfflineEnvWith(t, cfg, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b")

	notifyOffline(t, env, "p-b")
	assertNoOut(t, env, time.Second) // 静默 1s：窗口（1.5s）内，不判负
	waitOut(t, env, "p-a", "p-b")    // 超时后仍判负（p-a 收到 p-b 出局广播）
	if inRoster(t, env, "p-b") {
		t.Fatal("超时后 p-b 应被移出参战名单")
	}
}
