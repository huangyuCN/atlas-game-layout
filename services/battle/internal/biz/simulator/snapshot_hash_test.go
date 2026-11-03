package simulator

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/huangyuCN/atlas/lockstep"
)

// hashBytesOf 把状态哈希编码为 8 字节**大端**（与 actor 层 battle_frame.go 的 hashBytes 同口径）：
// 帧级与快照级在线上都是这 8 个字节，逐字节相等才算「同一口径」。
func hashBytesOf(h uint64) [8]byte {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], h)
	return raw
}

// snapshotOf 取一次快照并断言无错误（测试内复用）。
func snapshotOf(t *testing.T, b *Battle) lockstep.Snapshot {
	t.Helper()
	snap, err := b.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return snap
}

// stepAt 按给定帧号推进一帧（构造用例状态用）。
func stepAt(t *testing.T, b *Battle, frame lockstep.FrameID, ins ...lockstep.Input) {
	t.Helper()
	if err := b.Step(context.Background(), frame, ins); err != nil {
		t.Fatalf("Step(%d): %v", frame, err)
	}
}

// battleTwoPlayers 两人一帧、未分胜负。
func battleTwoPlayers(t *testing.T) *Battle {
	t.Helper()
	b := New(10, 100)
	stepAt(t, b, 1, mkInput("a", 1, 1), mkInput("b", 1, 2))
	return b
}

// battleThreePlayers 三人两帧（覆盖玩家名字典序与多帧推进）。
func battleThreePlayers(t *testing.T) *Battle {
	t.Helper()
	b := New(10, 100)
	stepAt(t, b, 1, mkInput("zoe", 1, 3), mkInput("amy", 1, 1), mkInput("bob", 1, 2))
	stepAt(t, b, 2, mkInput("amy", 2, 1))
	return b
}

// battleSettled 单帧直接冲线（已分胜负、得分非零）。
func battleSettled(t *testing.T) *Battle {
	t.Helper()
	b := New(10, 100)
	stepAt(t, b, 1, mkInput("a", 1, 10))
	return b
}

// TestSnapshotHashEqualsFrameHash ① 同一状态：快照 hash == 帧级 hash（逐字节相等）。
//
// 旧实现快照走 json.Marshal + fnv64a、帧级走定向编码，同一状态必然不等（本用例真红）；
// 二者是客户端跨帧/跨快照比对的同一份凭据，不等即误报失步。
func TestSnapshotHashEqualsFrameHash(t *testing.T) {
	cases := []struct {
		name string
		sim  *Battle
	}{
		{"空状态", New(10, 100)},
		{"两人未分胜负", battleTwoPlayers(t)},
		{"三人多帧", battleThreePlayers(t)},
		{"已分胜负", battleSettled(t)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snap := snapshotOf(t, c.sim)
			if got, want := snap.Hash, c.sim.Hash(); got != want {
				t.Fatalf("快照 hash = %d，帧级 hash = %d（同一状态必须同值）", got, want)
			}
			if got, want := hashBytesOf(snap.Hash), hashBytesOf(c.sim.Hash()); got != want {
				t.Fatalf("线上字节不等：快照 %x，帧级 %x（客户端跨帧/跨快照比对必然误报失步）", got, want)
			}
		})
	}
}

// TestSnapshotAndFrameHashTrackStateChanges ② 改一个字段（位置/得分/胜者/增删玩家）：
// 快照与帧级同步变化且仍逐字节相等（属性测试）。
func TestSnapshotAndFrameHashTrackStateChanges(t *testing.T) {
	base := snapshotOf(t, simOfState(baseState()))
	seen := map[uint64]string{base.Hash: "基准"}
	for _, m := range stateMutations() {
		st := baseState()
		m.mutate(st)
		sim := simOfState(st)
		snap := snapshotOf(t, sim)
		if snap.Hash == base.Hash {
			t.Fatalf("%s 变化后快照 hash 不变（快照口径漏了该字段）", m.name)
		}
		if prev, ok := seen[snap.Hash]; ok {
			t.Fatalf("%s 与 %s 撞哈希（不同状态同值）", m.name, prev)
		}
		seen[snap.Hash] = m.name
		if snap.Hash != sim.Hash() {
			t.Fatalf("%s：快照 hash %d ≠ 帧级 hash %d", m.name, snap.Hash, sim.Hash())
		}
		if got, want := hashBytesOf(snap.Hash), hashBytesOf(sim.Hash()); got != want {
			t.Fatalf("%s：线上字节不等 %x vs %x", m.name, got, want)
		}
	}
}

// TestSnapshotHashStableAcrossRestore ③ 恢复前后同值：快照 hash、恢复后重算的哈希一致。
func TestSnapshotHashStableAcrossRestore(t *testing.T) {
	sim := battleThreePlayers(t)
	before := snapshotOf(t, sim)
	restored := New(10, 100)
	if err := restored.Restore(context.Background(), before); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	after := snapshotOf(t, restored)
	if after.Hash != before.Hash {
		t.Fatalf("恢复后快照 hash = %d，恢复前 = %d（键序/序列化影响了哈希）", after.Hash, before.Hash)
	}
	if restored.Hash() != before.Hash {
		t.Fatalf("恢复后帧级 hash = %d，恢复前快照 = %d", restored.Hash(), before.Hash)
	}
	if got, want := hashBytesOf(after.Hash), hashBytesOf(before.Hash); got != want {
		t.Fatalf("恢复前后线上字节不等：%x vs %x", got, want)
	}
}
