package actor

import (
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
)

// 缺省租约：TTL=60s / RenewInterval=20s（框架 DefaultLease 的 10s/3s 是开发期取值）。
//
// 为什么改缺省：归属是 etcd 里带 TTL 的键，续租每个周期对本节点全部 owned 做
// LookupCAS + CAS。TTL=10s 时，一次十几秒的后端抖动（实测 commit 0.88→24.12ms、
// fsync 0.66→14.12ms，叠加后抖动窗口十几秒）就会让键在续租落地前过期 → 续租读到
// ErrNotFound → 判 Lost；1024 流压测里表现为 90 秒后大批对局同时被拆。60s/20s 与框架
// 自带压测节点一致，etcd 写量也随之降到约 1/6.7（每局每秒提交数 ≈ 2 / renew 周期）。
//
// 代价（必须知情）：故障接管等待 = lease_ttl + QuiesceGrace，从 15s 拉长到 65s——
// 进程崩溃/被 kill 后，他节点要等满这个窗口才能接管。强单活业务若不接受，把
// actor.lease_ttl 下调即可（renew 建议保持 TTL/3），但别低于实测抖动幅度。
const (
	// DefaultLeaseTTL 是 actor 目录归属键的缺省租约时长（60s）。
	DefaultLeaseTTL = 60 * time.Second
	// DefaultLeaseRenewInterval 是缺省续租周期（20s，约为 TTL 的 1/3）。
	DefaultLeaseRenewInterval = 20 * time.Second
)

// LeaseDefaults 返回缺省租约参数（TTL=60s / RenewInterval=20s），供文档与调用方对齐口径。
func LeaseDefaults() (ttl, renew time.Duration) {
	return DefaultLeaseTTL, DefaultLeaseRenewInterval
}

// LeaseFromConf 从公共 actor 配置段读取两项时长（time.ParseDuration 字符串）：
// 空字段 = 未配置（返回 0，交给缺省值兜底）；无单位/非法串/非正值在装配期报错，
// 不静默降级——「配了不生效」比启动失败更贵。
func LeaseFromConf(a *configspb.Actor) (ttl, renew time.Duration, err error) {
	if a == nil {
		return 0, 0, nil
	}
	if ttl, err = config.ParseDuration(a.GetLeaseTtl()); err != nil {
		return 0, 0, fmt.Errorf("actor: actor.lease_ttl 非法（time.ParseDuration 字符串，如 60s）: %w", err)
	}
	if renew, err = config.ParseDuration(a.GetLeaseRenewInterval()); err != nil {
		return 0, 0, fmt.Errorf("actor: actor.lease_renew_interval 非法（time.ParseDuration 字符串，如 20s）: %w", err)
	}
	return ttl, renew, nil
}

// leaseConfig 由 Options 组装并校验租约：TTL/RenewInterval 为 0 时取缺省 60s/20s，
// 发送方缓存与 QuiesceGrace 沿用框架默认（只有这两项是业务要调的旋钮）。
//
// 非法组合（Renew 非正或 Renew ≥ TTL）在此直接报错，由 NewRuntime 上抛为启动失败——
// 续租周期一旦逼近 TTL，抖动还没过去键就先过期，表现为整批 actor 被判 Lost。
func (o Options) leaseConfig() (cluster.LeaseConfig, error) {
	ttl, renew := o.LeaseTTL, o.LeaseRenewInterval
	if ttl == 0 {
		ttl = DefaultLeaseTTL
	}
	if renew == 0 {
		renew = DefaultLeaseRenewInterval
	}
	lease := cluster.DefaultLease()
	lease.TTL, lease.RenewInterval = ttl, renew
	if err := lease.Validate(); err != nil {
		return cluster.LeaseConfig{}, fmt.Errorf("actor: 租约配置非法: %w", err)
	}
	return lease, nil
}
