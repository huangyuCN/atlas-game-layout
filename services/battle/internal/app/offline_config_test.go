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
		{name: "显式毫秒级窗口", raw: "800ms", want: 800 * time.Millisecond},
		{name: "显式 0 关闭判定", raw: "0s", want: 0},
		{name: "负值即失败", raw: "-1s", wantErr: true},
		{name: "非法即失败", raw: "15", wantErr: true},
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
