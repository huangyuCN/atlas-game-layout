package conf

import (
	"testing"
	"time"
)

// TestRelayTimeoutDefault 验证未配置 relay.timeout 时缺省 5s（懒激活冷启动预算）。
func TestRelayTimeoutDefault(t *testing.T) {
	got, err := (&Bootstrap{}).RelayTimeout()
	if err != nil {
		t.Fatalf("缺省配置不应报错: %v", err)
	}
	if DefaultRelayTimeout != 5*time.Second {
		t.Fatalf("DefaultRelayTimeout = %v, want 5s", DefaultRelayTimeout)
	}
	if got != DefaultRelayTimeout {
		t.Fatalf("缺省预算 = %v, want %v", got, DefaultRelayTimeout)
	}
}

// TestRelayTimeoutConfigurable 验证显式配置覆盖缺省（正时长按 ParseDuration 解析）。
func TestRelayTimeoutConfigurable(t *testing.T) {
	b := &Bootstrap{Relay: &Relay{Timeout: "2500ms"}}
	got, err := b.RelayTimeout()
	if err != nil {
		t.Fatalf("显式正值不应报错: %v", err)
	}
	if got != 2500*time.Millisecond {
		t.Fatalf("预算 = %v, want 2500ms", got)
	}
}

// TestRelayTimeoutRejectsNonPositive 验证显式 ≤0 与非法时长在启动期报错（不静默回落缺省）。
func TestRelayTimeoutRejectsNonPositive(t *testing.T) {
	for _, raw := range []string{"0s", "0", "-1s", "abc"} {
		t.Run(raw, func(t *testing.T) {
			b := &Bootstrap{Relay: &Relay{Timeout: raw}}
			if _, err := b.RelayTimeout(); err == nil {
				t.Fatalf("relay.timeout=%q 应启动失败", raw)
			}
		})
	}
}
