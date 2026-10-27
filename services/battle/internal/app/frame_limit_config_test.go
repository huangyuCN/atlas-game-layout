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

// TestFrameLimitDefaults 验证局时长两件（max_frames / tick_interval_ms）**不配置**时的缺省值：
// 6000 帧 × 100ms = 10 分钟，落在真实对局时长 5–15 分钟的中位。
//
// 期望值写死字面量 6000 / 10min，而**不是**引用 actor.DefaultMaxFrames：本用例的职责就是钉住
// 「缺省是多少」这条规格；引用常量会让「常量被改回 60、用例跟着绿」的回归穿过去（缺省 60 即
// 6 秒一局，压测端看到的全是「对局已结束」，几轮数据全部作废）。
func TestFrameLimitDefaults(t *testing.T) {
	got, err := newActorConfig(&conf.Bootstrap{Battle: &conf.BattleConf{
		TicketKey:     testKeyBase64(),
		EdgeEndpoints: testEdgeEndpoints(),
	}})
	if err != nil {
		t.Fatalf("newActorConfig: %v", err)
	}
	if got.MaxFrames != 6000 {
		t.Errorf("缺省 MaxFrames = %d, 期望 6000（10fps 下 10 分钟）", got.MaxFrames)
	}
	if got.TickInterval != int64(100*time.Millisecond) {
		t.Errorf("缺省 TickInterval = %d, 期望 %d", got.TickInterval, int64(100*time.Millisecond))
	}
	// 单局寿命 = 帧数上限 × 帧间隔：缺省必须覆盖真实对局时长（5–15 分钟 ⇒ 3000–9000 帧），
	// 6 秒量级意味着「一局还没怎么打就结算」——压测/联调会把「对局异常结束」误认成业务失败。
	life := time.Duration(int64(got.MaxFrames) * got.TickInterval)
	if life != 10*time.Minute {
		t.Errorf("缺省单局寿命 = %s, 期望 10min（6000 帧 × 100ms，落在 5–15 分钟中位）", life)
	}
}

// TestFrameLimitOverride 验证显式配置生效：帧间隔按毫秒换算成纳秒、帧数上限逐字透传，
// 且与掉线窗口等既有字段互不干扰；上下限边界值（1ms/1000ms、1/20000 帧）合法。
// 20000 写成字面量而非 actor.MaxFramesLimit：上限是规格值（见 conf.proto），用例钉住它，
// 避免「常量被顺手改大、边界用例跟着改」的静默放宽。
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
		{name: "帧数上限 20000", maxFrames: int64Ptr(20000),
			wantInterval: actor.DefaultTickInterval, wantFrames: 20000},
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
// （缺省与显式 0 必须区分——proto3 的 optional 就是为此存在的）。超上限用例写死
// 20001 同理：20000 是规格上限，不该跟着常量漂移。
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
		{name: "帧数上限超上限", maxFrames: int64Ptr(20001), wantField: "max_frames"},
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
