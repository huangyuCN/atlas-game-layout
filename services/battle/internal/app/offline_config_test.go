package app

import (
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
)

// TestOfflineTimeoutConfig 验证掉线窗口装配（规格 §9.2）：缺省 15s（不写死在使用点）、
// 可显式覆盖、显式 0 关闭判定（测试/特例）、非法与非正值即启动失败。
func TestOfflineTimeoutConfig(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{name: "缺省 15s", raw: "", want: actor.DefaultOfflineTimeout},
		{name: "显式 15s", raw: "15s", want: 15 * time.Second},
		// 无感切网口径：静默容忍必须能调到 ≥15–20s（20s 是「切网 + 重连」的实测上限）。
		{name: "显式 20s（无感切网口径）", raw: "20s", want: 20 * time.Second},
		{name: "下限 1s", raw: "1s", want: actor.MinOfflineTimeout},
		{name: "上限 300s", raw: "300s", want: actor.MaxOfflineTimeout},
		{name: "显式 0 关闭判定", raw: "0s", want: 0},
		{name: "负值即失败", raw: "-1s", wantErr: true},
		{name: "非法即失败", raw: "15", wantErr: true},
		// 范围校验：低于下限/超上限一律启动失败（亚秒窗口只允许在测试里直接设 Config，
		// 配置面拒绝——小于 1s 的窗口必然先被帧面空闲读超时误判，属于笔误）。
		{name: "低于下限即失败", raw: "500ms", wantErr: true},
		{name: "超上限即失败", raw: "301s", wantErr: true},
	}
	for _, c := range cases {
		cfg := &conf.Bootstrap{Battle: &conf.BattleConf{
			TicketKey:      testKeyBase64(),
			EdgeEndpoints:  testEdgeEndpoints(),
			OfflineTimeout: c.raw,
		}}
		got, err := newActorConfig(cfg)
		if c.wantErr {
			if err == nil {
				t.Fatalf("%s：offline_timeout=%q 竟然装配成功（%s）", c.name, c.raw, got.OfflineTimeout)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s：newActorConfig: %v", c.name, err)
		}
		if got.OfflineTimeout != c.want {
			t.Fatalf("%s：OfflineTimeout = %s, 期望 %s", c.name, got.OfflineTimeout, c.want)
		}
	}
}
