package app

import (
	"testing"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas/transport/frame"
)

// TestNewActorConfigFrameBroadcastProtobuf 验证帧广播编码开关的配置装配：
// 字段缺失 = false（protojson/ver=1，行为零变化），显式 true 才切 protobuf。
func TestNewActorConfigFrameBroadcastProtobuf(t *testing.T) {
	base := func() *conf.Bootstrap {
		return &conf.Bootstrap{Battle: &conf.BattleConf{
			TicketKey:     testKeyBase64(),
			EdgeEndpoints: testEdgeEndpoints(),
		}}
	}

	def, err := newActorConfig(base())
	if err != nil {
		t.Fatalf("newActorConfig(缺省): %v", err)
	}
	if def.FrameBroadcastProtobuf {
		t.Fatal("frame_broadcast_protobuf 不配置应为 false（缺省 protojson/ver=1）")
	}

	cfg := base()
	cfg.Battle.FrameBroadcastProtobuf = true
	on, err := newActorConfig(cfg)
	if err != nil {
		t.Fatalf("newActorConfig(开启): %v", err)
	}
	if !on.FrameBroadcastProtobuf {
		t.Fatal("frame_broadcast_protobuf=true 应装配为开启")
	}
}

// TestNewStreamRegistryEncodingSwitch 验证开关落到注册表：
// 关闭时帧广播走 protojson（ver=1，缺省零变化），开启时走 protobuf（ver=2）。
func TestNewStreamRegistryEncodingSwitch(t *testing.T) {
	off, err := newStreamRegistry(actor.DefaultConfig())
	if err != nil {
		t.Fatalf("newStreamRegistry(关闭): %v", err)
	}
	if got := off.PushVersion(); got != frame.Version {
		t.Fatalf("关闭时 push ver = %d, 期望 %d（protojson）", got, frame.Version)
	}

	cfg := actor.DefaultConfig()
	cfg.FrameBroadcastProtobuf = true
	on, err := newStreamRegistry(cfg)
	if err != nil {
		t.Fatalf("newStreamRegistry(开启): %v", err)
	}
	if got := on.PushVersion(); got != frame.Version2 {
		t.Fatalf("开启时 push ver = %d, 期望 %d（protobuf wire）", got, frame.Version2)
	}
}

// TestNewActorConfigFrameBroadcastDelta 验证增量开关的配置装配：
// 不配置 = false（现行为零变化），显式 true 才开启。
func TestNewActorConfigFrameBroadcastDelta(t *testing.T) {
	cfg := &conf.Bootstrap{Battle: &conf.BattleConf{
		TicketKey:     testKeyBase64(),
		EdgeEndpoints: testEdgeEndpoints(),
	}}
	def, err := newActorConfig(cfg)
	if err != nil {
		t.Fatalf("newActorConfig(缺省): %v", err)
	}
	if def.FrameBroadcastDelta {
		t.Fatal("frame_broadcast_delta 不配置应为 false（现行为零变化）")
	}
	cfg.Battle.FrameBroadcastDelta = true
	on, err := newActorConfig(cfg)
	if err != nil {
		t.Fatalf("newActorConfig(开启): %v", err)
	}
	if !on.FrameBroadcastDelta {
		t.Fatal("frame_broadcast_delta=true 应装配为开启")
	}
}

// TestNewStreamRegistryDeltaAssembly 验证增量开关的**装配期校验**：
// 开启增量但没开 protobuf（关键帧即 ver=2 全量帧）必须启动失败，不能运行期降级；
// 两者都开则注册表按 3 个版本下发（pushVer 仍是 2 —— 全量/关键帧永远是 ver=2）。
func TestNewStreamRegistryDeltaAssembly(t *testing.T) {
	cfg := actor.DefaultConfig()
	cfg.FrameBroadcastDelta = true
	if _, err := newStreamRegistry(cfg); err == nil {
		t.Fatal("frame_broadcast_delta=true 且 frame_broadcast_protobuf=false 必须装配失败")
	}
	cfg.FrameBroadcastProtobuf = true
	reg, err := newStreamRegistry(cfg)
	if err != nil {
		t.Fatalf("两者都开应成功: %v", err)
	}
	if got := reg.PushVersion(); got != frame.Version2 {
		t.Fatalf("全量/关键帧 ver = %d, 期望 %d（关键帧复用 ver=2）", got, frame.Version2)
	}
}
