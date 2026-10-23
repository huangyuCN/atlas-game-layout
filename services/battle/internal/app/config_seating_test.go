package app

import (
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
)

// seatTestConf 构造一份「其余字段都合法」的 battle 配置（只测入座窗口/容量两项）。
func seatTestConf() *conf.Bootstrap {
	return &conf.Bootstrap{Battle: &conf.BattleConf{
		TicketKey:     testKeyBase64(),
		EdgeEndpoints: testEdgeEndpoints(),
	}}
}

// msPtr 取毫秒字段的指针（proto optional 字段的「已配置」语义）。
func msPtr(v int64) *int64 { return &v }

// playersPtr 取容量字段的指针。
func playersPtr(v int32) *int32 { return &v }

// TestNewActorConfigSeatWindow 验证入座窗口的三态语义：
// **未配置**取缺省 3s；**显式 0** 关闭窗口（成局即开打，旧行为）；显式正数原样生效；
// 负值与超上限一律装配失败（负数没有语义，超上限已不是「等入座」）。
func TestNewActorConfigSeatWindow(t *testing.T) {
	cases := []struct {
		name    string
		ms      *int64
		want    time.Duration
		wantErr bool
	}{
		{name: "未配置取缺省", ms: nil, want: actor.DefaultSeatWindow},
		{name: "显式 0 关闭", ms: msPtr(0), want: 0},
		{name: "显式 2s", ms: msPtr(2000), want: 2 * time.Second},
		{name: "负值失败", ms: msPtr(-1), wantErr: true},
		{name: "超上限失败", ms: msPtr(int64(actor.MaxSeatWindow/time.Millisecond) + 1), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := seatTestConf()
			cfg.Battle.SeatWindowMs = tc.ms
			got, err := newActorConfig(cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("期望装配失败，实际成功（SeatWindow=%v）", got.SeatWindow)
				}
				return
			}
			if err != nil {
				t.Fatalf("newActorConfig: %v", err)
			}
			if got.SeatWindow != tc.want {
				t.Fatalf("SeatWindow = %v, 期望 %v", got.SeatWindow, tc.want)
			}
		})
	}
}

// TestNewActorConfigMaxPlayers 验证每局会话容量：未配置取缺省 10（与 matcher 的 5v5 对齐），
// 显式值原样生效，0/负值/超上限一律装配失败（0 不是「不限」）。
func TestNewActorConfigMaxPlayers(t *testing.T) {
	cases := []struct {
		name    string
		np      *int32
		want    int
		wantErr bool
	}{
		{name: "未配置取缺省", np: nil, want: actor.DefaultMaxPlayers},
		{name: "显式 10 人局", np: playersPtr(10), want: 10},
		{name: "显式上限", np: playersPtr(actor.MaxPlayersLimit), want: actor.MaxPlayersLimit},
		{name: "0 失败", np: playersPtr(0), wantErr: true},
		{name: "负值失败", np: playersPtr(-1), wantErr: true},
		{name: "超上限失败", np: playersPtr(actor.MaxPlayersLimit + 1), wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := seatTestConf()
			cfg.Battle.MaxPlayers = tc.np
			got, err := newActorConfig(cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("期望装配失败，实际成功（MaxPlayers=%d）", got.MaxPlayers)
				}
				return
			}
			if err != nil {
				t.Fatalf("newActorConfig: %v", err)
			}
			if got.MaxPlayers != tc.want {
				t.Fatalf("MaxPlayers = %d, 期望 %d", got.MaxPlayers, tc.want)
			}
		})
	}
}
