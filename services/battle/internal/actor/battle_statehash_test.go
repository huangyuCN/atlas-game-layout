package actor

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/huangyuCN/atlas/lockstep"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/simulator"
)

// TestBattleFrameStateHashPerFrame 验证**每帧**广播都带帧级 state_hash。
//
// 旧实现只在快照帧填 SnapshotMeta.StateHash，帧级 LockstepFrame.StateHash 恒为空——
// 「帧级哈希」对客户端等于不存在（本用例在旧实现上真红：首帧 length=0）。
// 快照帧的 SnapshotMeta.StateHash 必须与同一帧的帧级 hash **逐字节相等**：
// 同一状态两个值会让客户端跨帧/跨快照比对误报失步。
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
			snapHash := f.GetSnapshot().GetStateHash()
			if len(snapHash) == 0 {
				t.Fatalf("第 %d 帧的快照 StateHash 为空（快照口径被影响）", f.GetFrameId())
			}
			// 同一状态两个 hash 必须逐字节相等：不等 ⇒ 客户端跨帧/跨快照比对误报失步。
			if !bytes.Equal(snapHash, h) {
				t.Fatalf("第 %d 帧：帧级 state_hash %x ≠ 快照 state_hash %x（同一状态两个值）",
					f.GetFrameId(), h, snapHash)
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

// TestStateHashBytesSameCaliber ④ 帧级与快照的线上字节口径一致：8 字节大端、逐字节相等。
// 用真实模拟器复现会话的取值方式（帧级取 Hash()、快照取 Snapshot().Hash）：
// 旧实现快照走 json.Marshal + fnv，本用例在「字节相等」断言上真红。
func TestStateHashBytesSameCaliber(t *testing.T) {
	sim := simulator.New(100, 1000)
	if err := sim.Step(context.Background(), 1, []lockstep.Input{
		{Frame: 1, Player: "a", Payload: []byte{3}},
		{Frame: 1, Player: "b", Payload: []byte{2}},
	}); err != nil {
		t.Fatalf("Step: %v", err)
	}
	snap, err := sim.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	result := lockstep.FrameResult{Frame: 1, Hash: sim.Hash(), HasHash: true, Snapshot: &snap}
	frameBytes := frameStateHash(result)
	snapBytes := snapshotMeta(result.Snapshot).GetStateHash()
	if !bytes.Equal(frameBytes, snapBytes) {
		t.Fatalf("帧级 state_hash %x ≠ 快照 state_hash %x（同一状态两个字节串）", frameBytes, snapBytes)
	}
	if !bytes.Equal(snapBytes, hashBytes(sim.Hash())) {
		t.Fatalf("快照字节 %x ≠ 8 字节大端编码 %x", snapBytes, hashBytes(sim.Hash()))
	}
	if got := binary.BigEndian.Uint64(snapBytes); got != sim.Hash() {
		t.Fatalf("按大端解回 %#x，期望 %#x（字节序不一致）", got, sim.Hash())
	}
	if snapBytes[0] != byte(sim.Hash()>>56) {
		t.Fatalf("首字节 %#x 不是最高字节（不是大端）", snapBytes[0])
	}
	// 编码细节守卫：0/1/高位/全字节覆盖，按大端往返。
	for _, h := range []uint64{0, 1, 0x0102030405060708, 0xdeadbeef, 1<<63 + 42} {
		raw := hashBytes(h)
		if got := binary.BigEndian.Uint64(raw); got != h || raw[0] != byte(h>>56) {
			t.Fatalf("hash %#x：字节 %x 按大端解回 %#x", h, raw, got)
		}
	}
	// 未算哈希时帧级留空（不得用 8 个零字节冒充真实哈希）。
	if got := frameStateHash(lockstep.FrameResult{Hash: 42, HasHash: false}); got != nil {
		t.Fatalf("HasHash=false 时帧级 state_hash = %x，期望空", got)
	}
}
