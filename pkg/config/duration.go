package config

import (
	"fmt"
	"time"
)

// ParseDuration 解析配置中的时长字符串（time.ParseDuration 格式，如 "30s"、"1m"）：
// 空字符串表示「未配置」，返回 0 交给调用方使用底层默认值；
// 无单位、非法值与非正值都在启动期报错——静默降级会让「配了不生效」难以发现。
func ParseDuration(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("config: 时长 %q 解析失败（示例 30s/1m）: %w", value, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("config: 时长 %q 必须为正数", value)
	}
	return d, nil
}
