package version

import "testing"

// TestCompare 验证版本比较的语义：段补齐、预发布低于正式版、构建元数据忽略、
// 非法输入报错（调用方按失败即不满足处理）。
func TestCompare(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1.0.0", "1.0.0", 0},
		{"1.2", "1.2.0", 0}, // 段数不足按 0 补齐
		{"1.2.3", "1.2.4", -1},
		{"1.10.0", "1.9.9", 1}, // 数字比较而非字符串比较
		{"2.0.0", "1.99.99", 1},
		{"1.2.0-rc.1", "1.2.0", -1}, // 预发布低于正式版
		{"1.2.0", "1.2.0-rc.1", 1},
		{"1.2.0+build.5", "1.2.0", 0}, // 构建元数据忽略
		{" 1.4.0 ", "1.4.0", 0},       // 两侧空白容忍
	}
	for _, c := range cases {
		got, err := Compare(c.a, c.b)
		if err != nil {
			t.Errorf("Compare(%q, %q) 报错: %v", c.a, c.b, err)
			continue
		}
		if got != c.want {
			t.Errorf("Compare(%q, %q) = %d, 期望 %d", c.a, c.b, got, c.want)
		}
	}
}

// TestCompareInvalid 验证非法版本串一律报错（调用方不得静默放行）。
func TestCompareInvalid(t *testing.T) {
	for _, v := range []string{"", "  ", "abc", "1.x.0", "1.2.3.4", "-1.0.0"} {
		if _, err := Compare(v, "1.0.0"); err == nil {
			t.Errorf("Compare(%q, 1.0.0) 应报错", v)
		}
		if _, err := Compare("1.0.0", v); err == nil {
			t.Errorf("Compare(1.0.0, %q) 应报错", v)
		}
	}
}
