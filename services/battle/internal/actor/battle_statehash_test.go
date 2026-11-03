package actor

import (
	"context"
	"testing"
)

// TestBattleFrameStateHashPerFrame 验证**每帧**广播都带帧级 state_hash。
//
// 旧实现只在快照帧填 SnapshotMeta.StateHash，帧级 LockstepFrame.StateHash 恒为空——
// 「帧级哈希」对客户端等于不存在（本用例在旧实现上真红：首帧 length=0）。
// 同时钉住两条口径互不影响：快照帧的 SnapshotMeta.StateHash 依旧非空。
func TestBattleFrameStateHashPerFrame(t *testing.T) {
	env := newBattleEnv(t)
	ctx := context.Background()
	seedAndJoin(t, env, ctx)
	sendFrameInputs(t, env, ctx)
	waitSettled(t, env)

	frames, _, _ := env.pusher.snapshots()
	got := frames["p-a"]
	if len(got) == 0 {
		t.Fatal("没有收到任何帧广播")
	}
	distinct := make(map[string]struct{}, len(got))
	snapFrames := 0
	for _, f := range got {
		h := f.GetStateHash()
		if len(h) != 8 {
			t.Fatalf("第 %d 帧 state_hash 长度 = %d（%x），期望每帧恒为 8 字节",
				f.GetFrameId(), len(h), h)
		}
		distinct[string(h)] = struct{}{}
		if f.GetSnapshot() != nil {
			snapFrames++
			if len(f.GetSnapshot().GetStateHash()) == 0 {
				t.Fatalf("第 %d 帧的快照 StateHash 为空（快照口径被影响）", f.GetFrameId())
			}
		}
	}
	// 帧级哈希必须随状态变化（位置每帧都在推进）：全部帧只有一种取值说明算的不是当帧状态。
	if len(distinct) < 2 {
		t.Fatalf("%d 帧广播的 state_hash 只有 %d 种取值（应随每帧状态变化）", len(got), len(distinct))
	}
	if snapFrames == 0 {
		t.Fatal("没有观察到快照帧（用例前提不成立：SnapshotEvery 应小于对局总帧数）")
	}
}
