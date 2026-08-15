package simulator

import (
	"context"
	"testing"

	"github.com/huangyuCN/atlas/lockstep"
)

// mkInput 构造单玩家移动输入。
func mkInput(player string, frame lockstep.FrameID, move byte) lockstep.Input {
	return lockstep.Input{Frame: frame, Player: lockstep.PlayerID(player), Payload: []byte{move}}
}

// run 以给定帧序推进模拟器。
func run(t *testing.T, b *Battle, frames []struct {
	frame lockstep.FrameID
	ins   []lockstep.Input
}) {
	t.Helper()
	for _, f := range frames {
		if err := b.Step(context.Background(), f.frame, f.ins); err != nil {
			t.Fatalf("Step(%d): %v", f.frame, err)
		}
	}
}

// TestSimulatorWinner 验证先到终点者胜。
func TestSimulatorWinner(t *testing.T) {
	b := New(3, 100)
	run(t, b, []struct {
		frame lockstep.FrameID
		ins   []lockstep.Input
	}{
		{1, []lockstep.Input{mkInput("a", 1, 2), mkInput("b", 1, 1)}},
		{2, []lockstep.Input{mkInput("a", 2, 1), mkInput("b", 2, 1)}},
	})
	if b.Winner() != "a" {
		t.Fatalf("Winner = %q, want a", b.Winner())
	}
	if b.Score("a") != 1 || b.Score("b") != 0 {
		t.Fatalf("分数不符: a=%d b=%d", b.Score("a"), b.Score("b"))
	}
}

// TestSimulatorMaxFrames 验证帧数耗尽按位置领先者胜。
func TestSimulatorMaxFrames(t *testing.T) {
	b := New(10, 5)
	run(t, b, []struct {
		frame lockstep.FrameID
		ins   []lockstep.Input
	}{
		{1, []lockstep.Input{mkInput("a", 1, 2), mkInput("b", 1, 1)}},
		{2, []lockstep.Input{mkInput("a", 2, 1), mkInput("b", 2, 1)}},
		{3, []lockstep.Input{mkInput("a", 3, 1), mkInput("b", 3, 1)}},
		{4, []lockstep.Input{mkInput("a", 4, 1), mkInput("b", 4, 1)}},
		{5, []lockstep.Input{mkInput("a", 5, 1), mkInput("b", 5, 1)}},
	})
	if b.Winner() != "a" {
		t.Fatalf("Winner = %q, want a", b.Winner())
	}
}

// TestSimulatorDeterministic 验证同输入序列 → 同状态哈希（确定性契约）。
func TestSimulatorDeterministic(t *testing.T) {
	runOne := func() uint64 {
		b := New(10, 100)
		run(t, b, []struct {
			frame lockstep.FrameID
			ins   []lockstep.Input
		}{
			{1, []lockstep.Input{mkInput("a", 1, 1), mkInput("b", 1, 2)}},
			{2, []lockstep.Input{mkInput("a", 2, 1), mkInput("b", 2, 1)}},
			{3, []lockstep.Input{mkInput("a", 3, 1)}},
		})
		snap, err := b.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		return snap.Hash
	}
	first := runOne()
	for i := 0; i < 10; i++ {
		if got := runOne(); got != first {
			t.Fatalf("第 %d 次哈希 %d != 首次 %d（非确定性）", i, got, first)
		}
	}
}

// TestSimulatorRestore 验证快照恢复往返一致。
func TestSimulatorRestore(t *testing.T) {
	b := New(5, 100)
	run(t, b, []struct {
		frame lockstep.FrameID
		ins   []lockstep.Input
	}{
		{1, []lockstep.Input{mkInput("a", 1, 2), mkInput("b", 1, 1)}},
		{2, []lockstep.Input{mkInput("a", 2, 1)}},
	})
	snap, err := b.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored := New(5, 100)
	if err := restored.Restore(context.Background(), snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restored.Winner() != b.Winner() || restored.Score("a") != b.Score("a") {
		t.Fatalf("恢复状态不符: %+v vs %+v", restored, b)
	}
}

// TestSimulatorInputOrderIndependent 验证同帧输入顺序无关（map 定位更新）。
func TestSimulatorInputOrderIndependent(t *testing.T) {
	runOne := func(order []lockstep.Input) uint64 {
		b := New(10, 100)
		run(t, b, []struct {
			frame lockstep.FrameID
			ins   []lockstep.Input
		}{{1, order}})
		snap, _ := b.Snapshot(context.Background())
		return snap.Hash
	}
	a, bIn := mkInput("a", 1, 1), mkInput("b", 1, 2)
	if runOne([]lockstep.Input{a, bIn}) != runOne([]lockstep.Input{bIn, a}) {
		t.Fatal("同帧输入顺序影响结果（破坏确定性）")
	}
}
