package actor

import "testing"

// TestBattleActorDirectPush 验证战斗域通知只走**直连推送**（阶段 3 批次 5 删掉 NATS 路）：
// 一局的帧广播与战斗结束通知经直连推送端口下发给每个参战玩家，且结算后关闭本局直连
// （规格 §9.8：结算后回收，禁止悬挂连接）。
func TestBattleActorDirectPush(t *testing.T) {
	env := newBattleEnv(t)
	seedAndJoin(t, env, newTestContext())
	sendFrameInputs(t, env, newTestContext())
	waitSettled(t, env)

	frames, ends, closed := env.pusher.snapshots()
	for _, p := range []string{"p-a", "p-b"} {
		if len(frames[p]) == 0 {
			t.Fatalf("玩家 %s 未收到直连帧广播", p)
		}
		if len(ends[p]) != 1 || ends[p][0] != "p-a" {
			t.Fatalf("玩家 %s 直连结束通知不符: %v", p, ends[p])
		}
	}
	if len(closed) != 1 || closed[0] != "b-test01" {
		t.Fatalf("结算后未关闭本局直连: %v", closed)
	}
}
