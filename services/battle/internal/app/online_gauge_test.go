package app

import (
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/metricstest"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	"github.com/huangyuCN/atlas/transport"
)

// TestRegisterOnlineGauge 验证 battle_online_players 是**拉取式**指标：值实时取自直连注册表
// （真相源），不依赖生命周期事件成对增减——急停/重启等不回调 OnStop 的路径也不会让计数漂移。
func TestRegisterOnlineGauge(t *testing.T) {
	rec := metricstest.New()
	reg := stream.NewRegistry()
	registerOnlineGauge(rec, reg)
	fn, ok := rec.Observable(actor.MetricOnlinePlayers)
	if !ok {
		t.Fatalf("未登记拉取式指标 %s", actor.MetricOnlinePlayers)
	}
	if got := fn(); got != 0 {
		t.Fatalf("无直连时 = %v, 期望 0", got)
	}
	reg.Register("p-a", stream.Conn{Kind: transport.KindWebSocket, ConnID: 1, BattleID: "b-1"})
	reg.Register("p-b", stream.Conn{Kind: transport.KindWebSocket, ConnID: 2, BattleID: "b-1"})
	if got := fn(); got != 2 {
		t.Fatalf("两名玩家在册时 = %v, 期望 2", got)
	}
	reg.Unregister("p-b", stream.Conn{Kind: transport.KindWebSocket, ConnID: 2, BattleID: "b-1"})
	if got := fn(); got != 1 {
		t.Fatalf("一名玩家在册时 = %v, 期望 1", got)
	}
	// 指标名与告警规则/面板引用的字面量必须同源（防三处漂移）。
	if actor.MetricOnlinePlayers != "battle_online_players" {
		t.Fatalf("指标名 = %q, 期望 battle_online_players", actor.MetricOnlinePlayers)
	}
}
