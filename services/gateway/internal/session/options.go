package session

import "time"

// DefaultTTL 是未配置会话租期时的默认租期。
// 取值规则：生效租期应 ≥ 客户端会话心跳周期的 3 倍（允许连丢两次心跳仍不掉线）——
// 客户端心跳由 SDK 决定（`sdkclient.WithSessionHeartbeatInterval`，默认 30s；
// 本仓集成脚本显式设为 10s），故用 SDK 默认心跳的客户端需要把 ttl 配到 ≥90s。
const DefaultTTL = 30 * time.Second

// minSweepInterval 是过期清扫周期的下限（time.NewTicker 对非正值 panic，极小租期配置下自保）。
const minSweepInterval = time.Millisecond

// Options 是会话管理器的时间参数（零值即默认：租期 DefaultTTL、清扫周期取生效租期的一半）。
type Options struct {
	// TTL 是会话路由租期（redis 路由 TTL 与心跳续租周期）；<=0 用 DefaultTTL。
	TTL time.Duration
	// SweepInterval 是过期会话清扫周期；<=0 按生效 TTL 的一半推导。
	SweepInterval time.Duration
}

// resolve 返回生效的（租期, 清扫周期）：零值与非正值一律回落到默认，清扫周期不低于 1ms。
func (o Options) resolve() (time.Duration, time.Duration) {
	ttl := o.TTL
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	sweep := o.SweepInterval
	if sweep <= 0 {
		sweep = ttl / 2
	}
	if sweep < minSweepInterval {
		sweep = minSweepInterval
	}
	return ttl, sweep
}
