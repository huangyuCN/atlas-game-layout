package simulator

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"reflect"
	"sort"
	"testing"

	"github.com/huangyuCN/atlas/lockstep"
)

// refHash 是 hash.go 编码口径的**独立参考实现**（bytes.Buffer + hash/fnv 逐字节拼装）：
// 与被测实现写法完全不同，任何一侧编码漂移都会让两者不等。
func refHash(pos, score map[string]int32, winner string) uint64 {
	keys := make([]string, 0, len(pos)+len(score))
	seen := make(map[string]bool, len(pos)+len(score))
	for p := range pos {
		keys = append(keys, p)
		seen[p] = true
	}
	for p := range score {
		if !seen[p] {
			keys = append(keys, p)
		}
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	put := func(v uint64) {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], v)
		buf.Write(b[:])
	}
	putStr := func(s string) {
		put(uint64(len(s)))
		buf.WriteString(s)
	}
	put(uint64(len(keys)))
	for _, p := range keys {
		putStr(p)
		put(uint64(uint32(pos[p])))
	}
	put(uint64(len(keys)))
	for _, p := range keys {
		putStr(p)
		put(uint64(uint32(score[p])))
	}
	putStr(winner)
	h := fnv.New64a()
	_, _ = h.Write(buf.Bytes())
	return h.Sum64()
}

// stateOf 取模拟器当前状态的引用（与 Snapshot 组装 State 的口径一致）。
func stateOf(b *Battle) *State {
	return &State{Positions: b.pos, Scores: b.score, Winner: b.winner}
}

// TestHashMatchesReferenceEncoding 是等价性测试：定向编码的 Hash() 必须与
// 独立参考实现（同一口径的字节流 + hash/fnv）逐值相等。
func TestHashMatchesReferenceEncoding(t *testing.T) {
	cases := []struct {
		name string
		sim  *Battle
	}{
		{"空状态", New(10, 100)},
		{"两人未分胜负", newBattleWith(t, []struct {
			frame lockstep.FrameID
			ins   []lockstep.Input
		}{{1, []lockstep.Input{mkInput("a", 1, 1), mkInput("b", 1, 2)}}})},
		{"三人多帧", newBattleWith(t, []struct {
			frame lockstep.FrameID
			ins   []lockstep.Input
		}{
			{1, []lockstep.Input{mkInput("zoe", 1, 3), mkInput("amy", 1, 1), mkInput("bob", 1, 2)}},
			{2, []lockstep.Input{mkInput("amy", 2, 1)}},
		})},
		{"已分胜负", newBattleWith(t, []struct {
			frame lockstep.FrameID
			ins   []lockstep.Input
		}{{1, []lockstep.Input{mkInput("a", 1, 10)}}})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := stateOf(c.sim)
			want := refHash(st.Positions, st.Scores, st.Winner)
			if got := c.sim.Hash(); got != want {
				t.Fatalf("Hash() = %d，参考实现 = %d（定向编码与口径不一致）", got, want)
			}
		})
	}
}

// newBattleWith 构造并按帧序推进一个模拟器（测试内复用）。
func newBattleWith(t *testing.T, frames []struct {
	frame lockstep.FrameID
	ins   []lockstep.Input
}) *Battle {
	t.Helper()
	b := New(10, 100)
	run(t, b, frames)
	return b
}

// TestHashCoversAllStateFields 是「防漏字段」属性测试：State 的每个字段单独变化，
// 哈希都必须跟着变（编码漏掉某字段 ⇒ 该字段怎么改哈希都不变 ⇒ 红）。
func TestHashCoversAllStateFields(t *testing.T) {
	base := func() *State {
		return &State{Positions: map[string]int32{"p1": 1, "p2": 2}, Scores: map[string]int32{"p1": 0, "p2": 0}}
	}
	simOf := func(st *State) *Battle {
		return &Battle{pos: st.Positions, score: st.Scores, winner: st.Winner}
	}
	mutations := []struct {
		name   string
		mutate func(*State)
	}{
		{"位置数值", func(s *State) { s.Positions["p1"]++ }},
		{"新增玩家位置", func(s *State) { s.Positions["p3"] = 1 }},
		{"删玩家位置", func(s *State) { delete(s.Positions, "p2") }},
		{"得分数值", func(s *State) { s.Scores["p1"]++ }},
		{"新增玩家得分", func(s *State) { s.Scores["p3"] = 1 }},
		{"胜者", func(s *State) { s.Winner = "p1" }},
	}
	baseHash := simOf(base()).Hash()
	for _, m := range mutations {
		st := base()
		m.mutate(st)
		got := simOf(st).Hash()
		if got == baseHash {
			t.Fatalf("%s 变化后哈希不变（编码漏了该字段）", m.name)
		}
		if want := refHash(st.Positions, st.Scores, st.Winner); got != want {
			t.Fatalf("%s：Hash() = %d，参考实现 = %d", m.name, got, want)
		}
	}
	// 字段数守卫：State 增字段时必须同步补编码与本用例（否则新字段会静默不进哈希）。
	if n := reflect.TypeOf(State{}).NumField(); n != 3 {
		t.Fatalf("State 现有 %d 个字段，本用例只覆盖 3 个（位置/得分/胜者）：请补编码与用例", n)
	}
}

// TestHashStableAcrossSnapshotRestore 验证恢复前后同状态同哈希：
// 快照恢复会重建 map（键序变化），定向编码必须与 map 迭代顺序无关。
func TestHashStableAcrossSnapshotRestore(t *testing.T) {
	b := newBattleWith(t, []struct {
		frame lockstep.FrameID
		ins   []lockstep.Input
	}{{1, []lockstep.Input{mkInput("zoe", 1, 3), mkInput("amy", 1, 1), mkInput("bob", 1, 2)}}})
	snap, err := b.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored := New(10, 100)
	if err := restored.Restore(context.Background(), snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got, want := restored.Hash(), b.Hash(); got != want {
		t.Fatalf("恢复后 Hash = %d，恢复前 = %d（键序影响了哈希）", got, want)
	}
}

// TestSnapshotHashStaysJSON 钉住快照口径不受影响：Snapshot().Hash 仍是
// json.Marshal(State) + fnv64a（帧级 Hash 是另一条口径，两者不混用）。
func TestSnapshotHashStaysJSON(t *testing.T) {
	b := newBattleWith(t, []struct {
		frame lockstep.FrameID
		ins   []lockstep.Input
	}{{1, []lockstep.Input{mkInput("a", 1, 2), mkInput("b", 1, 1)}}})
	snap, err := b.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	raw, err := json.Marshal(&State{Positions: b.pos, Scores: b.score, Winner: b.winner})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	h := fnv.New64a()
	_, _ = h.Write(raw)
	if snap.Hash != h.Sum64() {
		t.Fatalf("快照哈希口径变了：got %d，json+fnv = %d", snap.Hash, h.Sum64())
	}
}

// BenchmarkHash 帧级哈希性能：目标 0 分配、亚微秒级（旧 json.Marshal 路径约 2.2µs/23 分配）。
func BenchmarkHash(b *testing.B) {
	sim := New(100, 1000)
	_ = sim.Step(context.Background(), 1, []lockstep.Input{
		mkInput("a", 1, 3), mkInput("b", 1, 2),
	})
	var sink uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = sim.Hash()
	}
	_ = sink
}

// BenchmarkSnapshotHash 是旧口径对照：同一状态走 json.Marshal + fnv64a（快照路径）。
func BenchmarkSnapshotHash(b *testing.B) {
	sim := New(100, 1000)
	_ = sim.Step(context.Background(), 1, []lockstep.Input{
		mkInput("a", 1, 3), mkInput("b", 1, 2),
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sim.Snapshot(context.Background()); err != nil {
			b.Fatalf("Snapshot: %v", err)
		}
	}
}
