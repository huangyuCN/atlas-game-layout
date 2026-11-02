package app

import (
	"strings"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
)

// newGapTestConfig 构造只关心三处超时的最小合法配置（票密钥 + 一面接入层地址）。
func newGapTestConfig(gap, offline, datagramIdle string) *conf.Bootstrap {
	return &conf.Bootstrap{Battle: &conf.BattleConf{
		TicketKey:           testKeyBase64(),
		EdgeEndpoints:       testEdgeEndpoints(),
		MaxNetworkGap:       gap,
		OfflineTimeout:      offline,
		DatagramIdleTimeout: datagramIdle,
	}}
}

// TestMaxNetworkGapDefaultUnchanged 验证 P1 回归口径：不配 max_network_gap 时，
// 服务端两项超时**逐字节保持现有行为**（offline 缺省 15s、datagram idle 0 = 沿用 offline/3），
// 且显式配置的老写法照旧生效（零破坏）。
func TestMaxNetworkGapDefaultUnchanged(t *testing.T) {
	t.Run("全部不配", func(t *testing.T) {
		got, err := newActorConfig(newGapTestConfig("", "", ""))
		if err != nil {
			t.Fatalf("newActorConfig: %v", err)
		}
		if got.MaxNetworkGap != 0 {
			t.Fatalf("MaxNetworkGap = %s, 期望 0（不启用）", got.MaxNetworkGap)
		}
		if got.OfflineTimeout != actor.DefaultOfflineTimeout {
			t.Fatalf("OfflineTimeout = %s, 期望缺省 %s", got.OfflineTimeout, actor.DefaultOfflineTimeout)
		}
		if got.DatagramIdleTimeout != 0 {
			t.Fatalf("DatagramIdleTimeout = %s, 期望 0（沿用 offline/3）", got.DatagramIdleTimeout)
		}
	})

	t.Run("显式 0s 同样不启用", func(t *testing.T) {
		got, err := newActorConfig(newGapTestConfig("0s", "", ""))
		if err != nil {
			t.Fatalf("newActorConfig: %v", err)
		}
		if got.MaxNetworkGap != 0 || got.OfflineTimeout != actor.DefaultOfflineTimeout ||
			got.DatagramIdleTimeout != 0 {
			t.Fatalf("生效值 = gap %s / offline %s / idle %s, 期望 0 / %s / 0",
				got.MaxNetworkGap, got.OfflineTimeout, got.DatagramIdleTimeout, actor.DefaultOfflineTimeout)
		}
	})

	t.Run("老的双项写法不受影响", func(t *testing.T) {
		got, err := newActorConfig(newGapTestConfig("", "20s", "30s"))
		if err != nil {
			t.Fatalf("newActorConfig: %v", err)
		}
		if got.OfflineTimeout != 20*time.Second || got.DatagramIdleTimeout != 30*time.Second {
			t.Fatalf("生效值 = offline %s / idle %s, 期望 20s / 30s",
				got.OfflineTimeout, got.DatagramIdleTimeout)
		}
		if got.MaxNetworkGap != 0 {
			t.Fatalf("MaxNetworkGap = %s, 期望 0（未启用）", got.MaxNetworkGap)
		}
	})
}

// TestMaxNetworkGapDerivesTimeouts 验证一个值自动推导两处（旧实现只有两项、不推导 ⇒ 真红）：
// datagram_idle_timeout = max_network_gap；offline_timeout = min(max_network_gap × 1.5, 300s)
// （判负晚于拆会话，客户端才来得及在窗口内回座；gap×1.5 超过 offline 单项上限（300s）时
// 按 300s 夹取，故 gap=300s 的判负窗口是 300s 而不是旧的 450s）。
func TestMaxNetworkGapDerivesTimeouts(t *testing.T) {
	cases := []struct {
		raw     string
		gap     time.Duration
		idle    time.Duration
		offline time.Duration
	}{
		{raw: "30s", gap: 30 * time.Second, idle: 30 * time.Second, offline: 45 * time.Second},
		{raw: "20s", gap: 20 * time.Second, idle: 20 * time.Second, offline: 30 * time.Second},
		{raw: "1s", gap: actor.MinNetworkGap, idle: time.Second, offline: 1500 * time.Millisecond},
		{raw: "200s", gap: 200 * time.Second, idle: 200 * time.Second, offline: 300 * time.Second},
		{raw: "300s", gap: actor.MaxNetworkGap, idle: 300 * time.Second, offline: 300 * time.Second},
	}
	for _, c := range cases {
		got, err := newActorConfig(newGapTestConfig(c.raw, "", ""))
		if err != nil {
			t.Fatalf("%s：newActorConfig: %v", c.raw, err)
		}
		if got.MaxNetworkGap != c.gap {
			t.Fatalf("%s：MaxNetworkGap = %s, 期望 %s", c.raw, got.MaxNetworkGap, c.gap)
		}
		if got.DatagramIdleTimeout != c.idle {
			t.Fatalf("%s：DatagramIdleTimeout = %s, 期望 %s", c.raw, got.DatagramIdleTimeout, c.idle)
		}
		if got.OfflineTimeout != c.offline {
			t.Fatalf("%s：OfflineTimeout = %s, 期望 %s（min(gap × 1.5, 300s)）", c.raw, got.OfflineTimeout, c.offline)
		}
	}
}

// TestMaxNetworkGapConflicts 验证二义配置启动失败：max_network_gap 与
// offline_timeout / datagram_idle_timeout 任一同时出现即拒绝，提示二选一。
func TestMaxNetworkGapConflicts(t *testing.T) {
	cases := []struct {
		name    string
		offline string
		idle    string
	}{
		{name: "与 offline_timeout 冲突", offline: "20s"},
		{name: "与 datagram_idle_timeout 冲突", idle: "30s"},
		{name: "与两项都冲突", offline: "20s", idle: "30s"},
	}
	for _, c := range cases {
		_, err := newActorConfig(newGapTestConfig("30s", c.offline, c.idle))
		if err == nil {
			t.Fatalf("%s：竟然装配成功，期望启动失败", c.name)
		}
		msg := err.Error()
		for _, want := range []string{"max_network_gap", "offline_timeout", "datagram_idle_timeout"} {
			if !strings.Contains(msg, want) {
				t.Fatalf("%s：错误信息未点名字段 %q: %v", c.name, want, err)
			}
		}
	}
}

// TestMaxNetworkGapRange 验证范围 [1s, 300s]：越界/负值/非法一律启动失败，
// 显式 0s 是「不启用」（合法，见 TestMaxNetworkGapDefaultUnchanged）。
func TestMaxNetworkGapRange(t *testing.T) {
	for _, raw := range []string{"500ms", "301s", "-1s", "30"} {
		_, err := newActorConfig(newGapTestConfig(raw, "", ""))
		if err == nil {
			t.Fatalf("max_network_gap=%q 竟然装配成功，期望启动失败（范围 [%s, %s]）",
				raw, actor.MinNetworkGap, actor.MaxNetworkGap)
		}
		if !strings.Contains(err.Error(), "max_network_gap") {
			t.Fatalf("max_network_gap=%q：错误信息未点名字段: %v", raw, err)
		}
	}
}
