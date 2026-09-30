package e2e

import (
	"context"
	"testing"
	"time"

	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
)

// TestE2EBattleOfflineTimeout 验证掉线超时判负（口径直连，规格 §9.2/§9.3/§9.8）：
// 参战者直连帧面断开且窗口内未回座 → 出局广播 + 掉线者判负 + 仅剩一人判胜结算 + 结束广播 + 关闭本局直连。
// 数据报面（KCP）没有关闭握手，掉线由帧面空闲读超时（offline_timeout/3）发现，故窗口取秒级。
func TestE2EBattleOfflineTimeout(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	cfg := battleassemble.DefaultBattleConfig()
	cfg.TrackLen = 1000                  // 长赛道：不让对局自然分出胜负
	cfg.MaxFrames = 1 << 40              // 帧数上限放到不可达，胜负只由掉线判定决定
	cfg.OfflineTimeout = 3 * time.Second // 掉线窗口（数据报面空闲读超时 = 1s）
	newGame(t)
	gw := newGateway(t, "a")
	battle := newBattle(t, &cfg)
	obs := newBattleObserver(t)
	a := newBattleClient(t, ctx, gw, battle, frameKCP)
	b := newBattleClient(t, ctx, gw, battle, frameKCP)

	// 直接开局并对两名客户端出票（不依赖 matcher：本用例验证的是掉线链路）。
	battleID := "b-off-" + itNS
	seedBattle(t, ctx, battle, battleID, a.playerID, b.playerID)
	a.setTicket(issueTicket(t, ctx, battle, battleID, a.playerID))
	b.setTicket(issueTicket(t, ctx, battle, battleID, b.playerID))
	a.joinBattle(t, ctx, battleID)
	b.joinBattle(t, ctx, battleID)
	a.sendFrames(t, ctx, battleID, 1, 3, 1)
	b.sendFrames(t, ctx, battleID, 1, 3, 0)
	a.waitFrames(t, 1)

	// a 断开直连且不重连：窗口内未回座 → 判负。
	if err := a.frame.close(); err != nil {
		t.Fatalf("关闭 a 的直连: %v", err)
	}

	b.waitOut(t, a.playerID) // 出局广播（b 收到 a 出局）
	winner := b.waitEnd(t)   // 只剩 b 一人 → b 判胜并走既有结算路径
	if winner != b.playerID {
		t.Fatalf("掉线判负后胜者 = %q, 期望 %q", winner, b.playerID)
	}
	select {
	case ev := <-obs.settled:
		if ev.GetBattleId() != battleID || settledWinnerOf(ev) != b.playerID || len(ev.GetPlayers()) != 2 {
			t.Fatalf("结算事件不符: %+v", ev)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("未收到结算事件")
	}
	assertWalkoverSaved(t, ctx, battleID, b.playerID, a.playerID)
}
