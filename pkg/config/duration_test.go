package config

import (
	"testing"
	"time"
)

// TestParseDuration 验证时长配置值解析：空 = 未配置（返回 0），合法值解析，非法与非正值报错。
func TestParseDuration(t *testing.T) {
	if d, err := ParseDuration(""); err != nil || d != 0 {
		t.Fatalf("空值应表示未配置（0 且无错误），实际 d=%v err=%v", d, err)
	}
	d, err := ParseDuration("30s")
	if err != nil || d != 30*time.Second {
		t.Fatalf("30s 解析结果 = %v, err=%v", d, err)
	}
	if d, err := ParseDuration("1m30s"); err != nil || d != 90*time.Second {
		t.Fatalf("1m30s 解析结果 = %v, err=%v", d, err)
	}
	// 无单位、乱码、非正值都必须在启动期报错，而不是静默降级。
	for _, bad := range []string{"30", "abc", "-5s", "0s"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Fatalf("%q 期望报错，实际为 nil", bad)
		}
	}
}
