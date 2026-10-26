package conf

import (
	"fmt"
	"time"
)

// DefaultRelayTimeout 是 relay.timeout 的缺省值（懒激活冷启动预算）。
const DefaultRelayTimeout = 5 * time.Second

// RelayTimeout 返回推送/控制回调访问 redis 的单次预算：
// 未配置（空）= 缺省 5s；显式值必须能被 time.ParseDuration 解析且为正，
// ≤0 或非法即启动失败（不静默回落缺省）。
func (b *Bootstrap) RelayTimeout() (time.Duration, error) {
	raw := b.GetRelay().GetTimeout()
	if raw == "" {
		return DefaultRelayTimeout, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("relay.timeout 非法（应为 time.ParseDuration 字符串，如 5s）: %w", err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("relay.timeout 必须为正（当前 %s）", raw)
	}
	return d, nil
}
