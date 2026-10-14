package actor

import (
	"strings"
	"testing"
	"time"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
)

// TestLeaseDefaults 锁定缺省租约：60s/20s（框架 DefaultLease 的 10s/3s 在生产实测里
// 扛不住后端抖动——etcd commit 0.88→24.12ms、fsync 0.66→14.12ms 就能让 10s 的键
// 在续租落地前过期，整批 actor 判 Lost）。
func TestLeaseDefaults(t *testing.T) {
	ttl, renew := LeaseDefaults()
	if ttl != 60*time.Second || renew != 20*time.Second {
		t.Fatalf("缺省租约 = %s/%s，期望 60s/20s", ttl, renew)
	}
}

// TestLeaseConfigDefaults 锁定未配置时按缺省 60s/20s 组装，
// 其余字段（发送方缓存 / QuiesceGrace）沿用框架默认。
func TestLeaseConfigDefaults(t *testing.T) {
	lease, err := Options{}.leaseConfig()
	if err != nil {
		t.Fatalf("缺省配置不应报错: %v", err)
	}
	if lease.TTL != 60*time.Second || lease.RenewInterval != 20*time.Second {
		t.Fatalf("缺省租约 = %s/%s，期望 60s/20s", lease.TTL, lease.RenewInterval)
	}
	def := cluster.DefaultLease()
	if lease.SenderCacheTTL != def.SenderCacheTTL || lease.QuiesceGrace != def.QuiesceGrace {
		t.Fatalf("缓存/grace 应沿用框架默认: got cache=%s grace=%s want cache=%s grace=%s",
			lease.SenderCacheTTL, lease.QuiesceGrace, def.SenderCacheTTL, def.QuiesceGrace)
	}
	if err := lease.Validate(); err != nil {
		t.Fatalf("缺省租约应自洽: %v", err)
	}
}

// TestLeaseConfigExplicitOverride 锁定显式覆盖生效（配置入口真的接上了）。
func TestLeaseConfigExplicitOverride(t *testing.T) {
	lease, err := Options{LeaseTTL: 90 * time.Second, LeaseRenewInterval: 30 * time.Second}.leaseConfig()
	if err != nil {
		t.Fatalf("合法覆盖不应报错: %v", err)
	}
	if lease.TTL != 90*time.Second || lease.RenewInterval != 30*time.Second {
		t.Fatalf("显式覆盖 = %s/%s，期望 90s/30s", lease.TTL, lease.RenewInterval)
	}
}

// TestLeaseConfigRejectsInvalid 锁定非法组合一律报错（Renew 必须为正且严格小于 TTL）。
func TestLeaseConfigRejectsInvalid(t *testing.T) {
	cases := []struct {
		name  string
		ttl   time.Duration
		renew time.Duration
	}{
		{"Renew 等于 TTL", 60 * time.Second, 60 * time.Second},
		{"Renew 大于 TTL", 60 * time.Second, 90 * time.Second},
		{"TTL 为负", -time.Second, time.Second},
		{"Renew 为负", time.Minute, -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := (Options{LeaseTTL: tc.ttl, LeaseRenewInterval: tc.renew}).leaseConfig(); err == nil {
				t.Fatal("非法租约应报错")
			}
		})
	}
}

// TestNewRuntimeFailsFastOnInvalidLease 锁定「非法即启动失败」：
// 校验发生在建连接之前，故无需 etcd/NATS 即可断言错误来自租约配置而非连不上后端。
func TestNewRuntimeFailsFastOnInvalidLease(t *testing.T) {
	_, err := NewRuntime(Options{
		NodeID: "n1", Namespace: "test",
		EtcdEndpoints: []string{"127.0.0.1:1"}, NatsURL: "nats://127.0.0.1:1",
		LeaseTTL: 10 * time.Second, LeaseRenewInterval: 10 * time.Second,
	})
	if err == nil {
		t.Fatal("非法租约应让 NewRuntime 启动失败")
	}
	if !strings.Contains(err.Error(), "租约") {
		t.Fatalf("错误应指向租约配置，实际: %v", err)
	}
}

// TestLeaseFromConf 锁定 conf → Options 的映射：字段是 time.ParseDuration 字符串，
// 空 = 未配置（返回 0 交给缺省值）；非法串与非正值在启动期报错。
func TestLeaseFromConf(t *testing.T) {
	if ttl, renew, err := LeaseFromConf(nil); err != nil || ttl != 0 || renew != 0 {
		t.Fatalf("nil 配置应返回零值且不报错，实际 %s/%s err=%v", ttl, renew, err)
	}
	if ttl, renew, err := LeaseFromConf(&configspb.Actor{}); err != nil || ttl != 0 || renew != 0 {
		t.Fatalf("空字段应返回零值且不报错，实际 %s/%s err=%v", ttl, renew, err)
	}
	ttl, renew, err := LeaseFromConf(&configspb.Actor{LeaseTtl: "45s", LeaseRenewInterval: "15s"})
	if err != nil || ttl != 45*time.Second || renew != 15*time.Second {
		t.Fatalf("映射结果 = %s/%s err=%v，期望 45s/15s", ttl, renew, err)
	}
	for _, bad := range []*configspb.Actor{
		{LeaseTtl: "60"},          // 无单位
		{LeaseTtl: "0s"},          // 非正
		{LeaseRenewInterval: "x"}, // 非法串
	} {
		if _, _, err := LeaseFromConf(bad); err == nil {
			t.Fatalf("非法时长应报错: %+v", bad)
		}
	}
}
