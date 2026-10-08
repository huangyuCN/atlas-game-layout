package actor

import (
	"context"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// TestBattleActorMaxFramesConfigurable 验证「局时长可配」真正接到帧引擎：
// 同一套 actor 装配，只把 MaxFrames 从缺省 60 提到 120，则在第 60 帧**不结算**、
// 第 120 帧才按位置领先者结算。这是「配置项只落在结构体里、没接到 lockstep 会话」的反证。
//
// 赛道取 1000（远大于帧数上限），保证胜负只由帧数上限决定；p-b 每帧前进 1 格领先。
func TestBattleActorMaxFramesConfigurable(t *testing.T) {
	cfg := testBattleConfig()
	cfg.TrackLen = 1000
	cfg.MaxFrames = 120
	env := newBattleEnvWith(t, cfg)
	ctx := context.Background()
	seedAndJoin(t, env, ctx)

	// 跑满缺省的 60 帧：旧口径下此时早已结算，新口径下必须还活着。
	sendFrameRange(t, env, ctx, 1, DefaultMaxFrames)
	waitFrameBroadcast(t, env, DefaultMaxFrames)
	assertNotSettled(t, env, DefaultMaxFrames)

	// 继续跑到 120 帧：帧数上限到点，位置领先者（p-b）判胜。
	sendFrameRange(t, env, ctx, DefaultMaxFrames+1, 120)
	waitSettled(t, env)
	env.publisher.mu.Lock()
	ev := env.publisher.ev
	env.publisher.mu.Unlock()
	if winner := settledWinner(ev); winner != "p-b" {
		t.Fatalf("帧数上限结算胜者 = %q, want p-b（事件 %+v）", winner, ev)
	}
}

// sendFrameRange 发送 [from, to] 闭区间的帧输入：p-a 原地不动（步长 0），p-b 每帧前进 1。
func sendFrameRange(t *testing.T, env *battleEnv, ctx context.Context, from, to uint64) {
	t.Helper()
	for i := from; i <= to; i++ {
		for p, step := range map[string]byte{"p-a": 0, "p-b": 1} {
			sender, err := types.NewPID(playerType, p)
			if err != nil {
				t.Fatalf("sender PID %s: %v", p, err)
			}
			if err := env.rt.Tell(ctx, env.pid, &battlev1.FrameInputReq{
				BattleId: "b-test01",
				Input:    &locksteppb.LockstepInput{FrameId: i, PlayerId: p, Payload: []byte{step}},
			}, core.WithSender(sender)); err != nil {
				t.Fatalf("帧输入 %s@%d: %v", p, i, err)
			}
		}
	}
}

// waitFrameBroadcast 等到直连帧广播推进到 frame（会话按 ticker 逐帧消费输入，故必须等）。
func waitFrameBroadcast(t *testing.T, env *battleEnv, frame uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if maxBroadcastFrame(env) >= frame {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("未在时限内广播到第 %d 帧（当前 %d）", frame, maxBroadcastFrame(env))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// maxBroadcastFrame 返回已广播的最大帧号（0 = 尚无广播）。
func maxBroadcastFrame(env *battleEnv) uint64 {
	frames, _, _ := env.pusher.snapshots()
	var max uint64
	for _, list := range frames {
		for _, f := range list {
			if id := f.GetFrameId(); id > max {
				max = id
			}
		}
	}
	return max
}

// assertNotSettled 断言第 frame 帧跑完后仍未结算：结算事件为空、无结束通知、无关闭对局。
func assertNotSettled(t *testing.T, env *battleEnv, frame uint64) {
	t.Helper()
	env.publisher.mu.Lock()
	ev := env.publisher.ev
	env.publisher.mu.Unlock()
	_, ends, closed := env.pusher.snapshots()
	if ev != nil || len(ends) != 0 || len(closed) != 0 {
		t.Fatalf("第 %d 帧就结算了（事件 %+v；结束通知 %v；关闭 %v）——帧数上限未生效",
			frame, ev, ends, closed)
	}
}
