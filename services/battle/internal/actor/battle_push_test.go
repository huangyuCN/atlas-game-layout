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

// TestBattleActorFrameBroadcastIsOneCallPerFrame 验证帧广播不再「逐目标各调一次」：
// 每帧只发起**一次**广播调用（目标数在实现内部展开），故广播次数与每个玩家收到的帧数
// 一一对应、目标数合计恒等于「目标数 × 帧数」（一次编码、多人复用）。
func TestBattleActorFrameBroadcastIsOneCallPerFrame(t *testing.T) {
	env := newBattleEnv(t)
	seedAndJoin(t, env, newTestContext())
	sendFrameInputs(t, env, newTestContext())
	waitSettled(t, env)

	frames, _, _ := env.pusher.snapshots()
	calls, targets := env.pusher.broadcastStats()
	if calls == 0 {
		t.Fatal("对局期间没有发生帧广播")
	}
	for _, p := range []string{"p-a", "p-b"} {
		if len(frames[p]) != calls {
			t.Fatalf("广播调用 %d 次，玩家 %s 收到 %d 帧：期望每帧一次广播", calls, p, len(frames[p]))
		}
	}
	if want := 2 * calls; targets != want {
		t.Fatalf("广播目标数合计 = %d, 期望 %d（2 人 × %d 次广播）", targets, want, calls)
	}
}
