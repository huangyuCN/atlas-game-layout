package server

import (
	"testing"
	"time"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
)

// TestDatagramIdleIndependentConfig 验证数据报面空闲读超时可由 battle.datagram_idle_timeout
// 独立指定（battle 侧静默检测不再恒等于 offline_timeout/3）：
//   - 缺省（0）⇒ 仍是 offline/3（守回归，现行为零变化）；
//   - 显式 30s ⇒ 生效值就是 30s，**允许 ≥ offline_timeout**（旧实现恒 offline/3 且要求
//     idle < offline，30s/20s 这一组会被判失败——无感切网需要 idle ≥ 最长断流时间）。
func TestDatagramIdleIndependentConfig(t *testing.T) {
	cases := []struct {
		name    string
		policy  FramePolicy
		want    time.Duration
		wantErr bool
	}{
		{
			name:   "缺省仍是 offline/3（20s 窗口 → 6.67s）",
			policy: FramePolicy{OfflineTimeout: 20 * time.Second},
			want:   20 * time.Second / 3,
		},
		{
			name:   "显式 30s 配在 20s 掉线窗口上（无感切网口径）",
			policy: FramePolicy{OfflineTimeout: 20 * time.Second, DatagramIdleTimeout: 30 * time.Second},
			want:   30 * time.Second,
		},
		{
			name:   "显式 30s 配在 15s 缺省窗口上同样成立",
			policy: FramePolicy{OfflineTimeout: 15 * time.Second, DatagramIdleTimeout: 30 * time.Second},
			want:   30 * time.Second,
		},
		{
			name:   "关闭掉线判定 + 显式 30s 原样保留",
			policy: FramePolicy{OfflineTimeout: 0, DatagramIdleTimeout: 30 * time.Second},
			want:   30 * time.Second,
		},
	}
	for _, c := range cases {
		got, err := datagramIdle(c.policy, 0)
		if c.wantErr {
			if err == nil {
				t.Fatalf("%s：竟然推导成功（idle=%s）", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s：datagramIdle: %v", c.name, err)
		}
		if got != c.want {
			t.Fatalf("%s：idle = %s, 期望 %s", c.name, got, c.want)
		}
	}
}

// TestNewFrameServersAcceptsIdleLongerThanOffline 验证装配期不再把「idle ≥ offline」判为错误：
// 显式 30s 静默检测 + 20s 掉线窗口必须能构造出 KCP/UDP 帧面（旧实现此处失败）。
func TestNewFrameServersAcceptsIdleLongerThanOffline(t *testing.T) {
	ops, err := NewFrameOps(nil, testRecorder(), testTicketKey())
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	policy := FramePolicy{OfflineTimeout: 20 * time.Second, DatagramIdleTimeout: 30 * time.Second}
	kcpCfg := &conf.Bootstrap{Server: &configspb.Server{
		Kcp: &configspb.Server_KCP{Addr: "127.0.0.1:0"},
	}}
	if _, err := NewFrameServers(kcpCfg, ops, policy, testRecorder()); err != nil {
		t.Fatalf("idle(30s) ≥ offline(20s) 竟然装配失败: %v", err)
	}
}
