package app

import (
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
)

// TestDatagramIdleTimeoutConfig 验证数据报面静默检测独立配置（battle.datagram_idle_timeout）：
// 缺省（空串/0s）⇒ 0 = 沿用 offline_timeout/3（不改现行为）；显式值必须落在 [1s, 600s]，
// 非法、负值、越界一律启动失败；该值与 offline_timeout **解耦**（可以更大，无感切网就靠它）。
func TestDatagramIdleTimeoutConfig(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{name: "缺省 = 0 沿用 offline/3", raw: "", want: 0},
		{name: "显式 0s 同样沿用 offline/3", raw: "0s", want: 0},
		{name: "显式 30s（无感切网口径）", raw: "30s", want: 30 * time.Second},
		{name: "下限 1s", raw: "1s", want: actor.MinDatagramIdleTimeout},
		{name: "上限 600s", raw: "600s", want: actor.MaxDatagramIdleTimeout},
		{name: "低于下限即失败", raw: "500ms", wantErr: true},
		{name: "超上限即失败", raw: "601s", wantErr: true},
		{name: "负值即失败", raw: "-1s", wantErr: true},
		{name: "非法即失败", raw: "30", wantErr: true},
	}
	for _, c := range cases {
		cfg := &conf.Bootstrap{Battle: &conf.BattleConf{
			TicketKey:           testKeyBase64(),
			EdgeEndpoints:       testEdgeEndpoints(),
			DatagramIdleTimeout: c.raw,
		}}
		got, err := newActorConfig(cfg)
		if c.wantErr {
			if err == nil {
				t.Fatalf("%s：datagram_idle_timeout=%q 竟然装配成功（%s）", c.name, c.raw, got.DatagramIdleTimeout)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s：newActorConfig: %v", c.name, err)
		}
		if got.DatagramIdleTimeout != c.want {
			t.Fatalf("%s：DatagramIdleTimeout = %s, 期望 %s", c.name, got.DatagramIdleTimeout, c.want)
		}
	}
}

// TestDatagramIdleTimeoutDecoupledFromOffline 验证新配置与掉线窗口**解耦**：30s 静默检测
// 配在 20s 掉线窗口之上仍能装配（旧实现把 idle 恒绑定为 offline/3 并要求 < offline，
// 这条口径下的 30s 会被判失败——无感切网也就无从谈起）。
func TestDatagramIdleTimeoutDecoupledFromOffline(t *testing.T) {
	cfg := &conf.Bootstrap{Battle: &conf.BattleConf{
		TicketKey:           testKeyBase64(),
		EdgeEndpoints:       testEdgeEndpoints(),
		OfflineTimeout:      "20s",
		DatagramIdleTimeout: "30s",
	}}
	got, err := newActorConfig(cfg)
	if err != nil {
		t.Fatalf("newActorConfig: %v", err)
	}
	if got.OfflineTimeout != 20*time.Second || got.DatagramIdleTimeout != 30*time.Second {
		t.Fatalf("生效值 = offline %s / idle %s, 期望 20s / 30s", got.OfflineTimeout, got.DatagramIdleTimeout)
	}
}
