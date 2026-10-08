package app

import (
	"strings"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
)

// int64Ptr 返回 int64 的指针（proto optional 字段用指针表达「未配置」）。
func int64Ptr(v int64) *int64 { return &v }

// TestFrameLimitDefaults 验证局时长两件（max_frames / tick_interval_ms）**不配置即保持现值**：
// 缺省 60 帧 × 100ms ≈ 6s，装配结果与硬编码常量逐字相等（本批次「行为零变化」的判据）。
func TestFrameLimitDefaults(t *testing.T) {
	got, err := newActorConfig(&conf.Bootstrap{Battle: &conf.BattleConf{
		TicketKey:     testKeyBase64(),
		EdgeEndpoints: testEdgeEndpoints(),
	}})
	if err != nil {
		t.Fatalf("newActorConfig: %v", err)
	}
	if got.MaxFrames != actor.DefaultMaxFrames {
		t.Errorf("缺省 MaxFrames = %d, 期望 %d", got.MaxFrames, actor.DefaultMaxFrames)
	}
	if got.TickInterval != actor.DefaultTickInterval {
		t.Errorf("缺省 TickInterval = %d, 期望 %d", got.TickInterval, actor.DefaultTickInterval)
	}
	// 单局寿命 = 帧数上限 × 帧间隔：缺省必须仍是约 6s（回归保护，防止缺省被顺手改掉）。
	if life := time.Duration(int64(got.MaxFrames) * got.TickInterval); life != 6*time.Second {
		t.Errorf("缺省单局寿命 = %s, 期望 6s", life)
	}
}

// TestFrameLimitOverride 验证显式配置生效：帧间隔按毫秒换算成纳秒、帧数上限逐字透传，
// 且与掉线窗口等既有字段互不干扰；上下限边界值（1ms/1000ms、1/20000 帧）合法。
func TestFrameLimitOverride(t *testing.T) {
	cases := []struct {
		name         string
		intervalMs   *int64
		maxFrames    *int64
		wantInterval int64
		wantFrames   uint64
	}{
		{name: "长局验收口径 300 帧", intervalMs: int64Ptr(100), maxFrames: int64Ptr(300),
			wantInterval: int64(100 * time.Millisecond), wantFrames: 300},
		{name: "只覆盖帧间隔", intervalMs: int64Ptr(50),
			wantInterval: int64(50 * time.Millisecond), wantFrames: actor.DefaultMaxFrames},
		{name: "只覆盖帧数上限", maxFrames: int64Ptr(1200),
			wantInterval: actor.DefaultTickInterval, wantFrames: 1200},
		{name: "帧间隔下限 1ms", intervalMs: int64Ptr(actor.MinTickIntervalMillis),
			wantInterval: int64(time.Millisecond), wantFrames: actor.DefaultMaxFrames},
		{name: "帧间隔上限 1000ms", intervalMs: int64Ptr(actor.MaxTickIntervalMillis),
			wantInterval: int64(time.Second), wantFrames: actor.DefaultMaxFrames},
		{name: "帧数下限 1", maxFrames: int64Ptr(1),
			wantInterval: actor.DefaultTickInterval, wantFrames: 1},
		{name: "帧数上限 20000", maxFrames: int64Ptr(actor.MaxFramesLimit),
			wantInterval: actor.DefaultTickInterval, wantFrames: uint64(actor.MaxFramesLimit)},
	}
	for _, c := range cases {
		cfg := &conf.Bootstrap{Battle: &conf.BattleConf{
			TicketKey:      testKeyBase64(),
			EdgeEndpoints:  testEdgeEndpoints(),
			TickIntervalMs: c.intervalMs,
			MaxFrames:      c.maxFrames,
		}}
		got, err := newActorConfig(cfg)
		if err != nil {
			t.Fatalf("%s：newActorConfig: %v", c.name, err)
		}
		if got.TickInterval != c.wantInterval {
			t.Errorf("%s：TickInterval = %d, 期望 %d", c.name, got.TickInterval, c.wantInterval)
		}
		if got.MaxFrames != c.wantFrames {
			t.Errorf("%s：MaxFrames = %d, 期望 %d", c.name, got.MaxFrames, c.wantFrames)
		}
		// 覆盖局时长不得顺手改掉掉线窗口（两件事独立配置）。
		if got.OfflineTimeout != actor.DefaultOfflineTimeout {
			t.Errorf("%s：OfflineTimeout = %s, 期望缺省 %s", c.name, got.OfflineTimeout, actor.DefaultOfflineTimeout)
		}
	}
}

// TestFrameLimitInvalid 验证非法值即启动失败并给出明确原因：显式 0、负值、超上限
// （缺省与显式 0 必须区分——proto3 的 optional 就是为此存在的）。
func TestFrameLimitInvalid(t *testing.T) {
	cases := []struct {
		name       string
		intervalMs *int64
		maxFrames  *int64
		wantField  string
	}{
		{name: "帧间隔显式 0", intervalMs: int64Ptr(0), wantField: "tick_interval_ms"},
		{name: "帧间隔负值", intervalMs: int64Ptr(-1), wantField: "tick_interval_ms"},
		{name: "帧间隔超上限", intervalMs: int64Ptr(actor.MaxTickIntervalMillis + 1), wantField: "tick_interval_ms"},
		{name: "帧数上限显式 0", maxFrames: int64Ptr(0), wantField: "max_frames"},
		{name: "帧数上限负值", maxFrames: int64Ptr(-60), wantField: "max_frames"},
		{name: "帧数上限超上限", maxFrames: int64Ptr(actor.MaxFramesLimit + 1), wantField: "max_frames"},
	}
	for _, c := range cases {
		cfg := &conf.Bootstrap{Battle: &conf.BattleConf{
			TicketKey:      testKeyBase64(),
			EdgeEndpoints:  testEdgeEndpoints(),
			TickIntervalMs: c.intervalMs,
			MaxFrames:      c.maxFrames,
		}}
		got, err := newActorConfig(cfg)
		if err == nil {
			t.Fatalf("%s：竟然装配成功（MaxFrames=%d TickInterval=%d），非法配置必须启动失败",
				c.name, got.MaxFrames, got.TickInterval)
		}
		if !strings.Contains(err.Error(), c.wantField) {
			t.Errorf("%s：错误信息未点名字段 %s: %v", c.name, c.wantField, err)
		}
		// 失败原因必须落在 battle 配置域，便于运维一眼定位到配置项。
		if !strings.Contains(err.Error(), "battle.") {
			t.Errorf("%s：错误信息未带 battle. 前缀: %v", c.name, err)
		}
	}
}
