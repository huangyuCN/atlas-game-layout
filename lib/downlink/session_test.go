// 客户端解码状态机的验收用例：跳变必请求关键帧（去重）、base_seq 不符即丢弃不缓冲、
// 快照缺失即清空。这些是设计 §4.3 的**硬语义**，任何"尽力而为"的容错都会让失步静默花屏。

package downlink

import (
	"bytes"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fullPayload 构造一帧全量广播的 protobuf 载荷（ver=2）。
func fullPayload(t *testing.T, seq uint64, snapshot *locksteppb.SnapshotMeta) []byte {
	t.Helper()
	bc := &battlev1.FrameBroadcast{BattleId: "b-1", Frame: &locksteppb.LockstepFrame{
		FrameId:    seq,
		ServerTime: timestamppb.New(epochNanos(int64(seq) * 1000)),
		Snapshot:   snapshot,
		Inputs: []*locksteppb.LockstepInput{
			{FrameId: seq, PlayerId: "p-a", Payload: []byte{1}},
			{FrameId: seq, PlayerId: "p-b", Payload: []byte{2}},
		},
	}}
	b, err := proto.Marshal(bc)
	if err != nil {
		t.Fatalf("marshal full: %v", err)
	}
	return b
}

// deltaPayload 构造一帧增量载荷（ver=3）。
func deltaPayload(t *testing.T, base, seq uint64, changed []battlev1.FrameDeltaField,
	d *battlev1.FrameBroadcastDelta) []byte {
	t.Helper()
	msg := d
	if msg == nil {
		msg = &battlev1.FrameBroadcastDelta{}
	}
	msg.BaseSeq, msg.Seq, msg.Changed = base, seq, changed
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal delta: %v", err)
	}
	return b
}

// newTestDecoder 构造带请求计数的解码器。
func newTestDecoder() (*Decoder, *int) {
	n := 0
	return NewDecoder(func(uint64) error { n++; return nil }), &n
}

// TestSeqJumpRequestsKeyframeOnce ②：丢一帧（seq 跳变）⇒ 必须请求关键帧，且同一 pending
// 窗口内**只请求一次**（去重），失步的增量绝不交付上层。
func TestSeqJumpRequestsKeyframeOnce(t *testing.T) {
	dec, requests := newTestDecoder()
	if _, ok, err := dec.Apply(fullPayload(t, 1, nil), 2); err != nil || !ok {
		t.Fatalf("关键帧应交付: ok=%v err=%v", ok, err)
	}
	// 第 2 帧丢了，直接来了第 3 帧（base_seq=2）：
	if _, ok, err := dec.Apply(deltaPayload(t, 2, 3, nil, nil), VersionDelta); err != nil || ok {
		t.Fatalf("跳变的增量必须丢弃（不猜基线）: ok=%v err=%v", ok, err)
	}
	if *requests != 1 {
		t.Fatalf("跳变后请求次数 = %d, 期望 1", *requests)
	}
	// 关键帧在途期间又来一条失步增量：不得重复请求。
	if _, ok, _ := dec.Apply(deltaPayload(t, 4, 5, nil, nil), VersionDelta); ok {
		t.Fatal("失步的增量必须继续丢弃")
	}
	if *requests != 1 {
		t.Fatalf("pending 期间重复请求（%d 次）：客户端必须去重", *requests)
	}
	// 关键帧到达：基线重置，pending 清空，之后连续增量正常交付。
	if _, ok, err := dec.Apply(fullPayload(t, 3, nil), 2); err != nil || !ok {
		t.Fatalf("关键帧应交付: ok=%v err=%v", ok, err)
	}
	if dec.Pending() || dec.LastSeq() != 3 {
		t.Fatalf("关键帧后 pending=%v lastSeq=%d, 期望 false/3", dec.Pending(), dec.LastSeq())
	}
	got, ok, err := dec.Apply(deltaPayload(t, 3, 4, nil, nil), VersionDelta)
	if err != nil || !ok {
		t.Fatalf("连续增量应交付: ok=%v err=%v", ok, err)
	}
	if got.GetFrame().GetFrameId() != 4 {
		t.Fatalf("交付帧号 = %d, 期望 4", got.GetFrame().GetFrameId())
	}
}

// TestBaseSeqMismatchDropsWithoutBuffering ③：base_seq 与自己的基线不符 ⇒ 丢弃、**不缓冲**、
// 不按 seq 猜测基线；随后到达的合法增量按真实基线合并，坏增量里的内容一条都不许混进去。
func TestBaseSeqMismatchDropsWithoutBuffering(t *testing.T) {
	dec, requests := newTestDecoder()
	dec.Apply(fullPayload(t, 1, nil), 2)
	bad := &battlev1.FrameBroadcastDelta{
		SetInputs: []*locksteppb.LockstepInput{{FrameId: 2, PlayerId: "p-ghost", Payload: []byte{9}}},
	}
	if _, ok, err := dec.Apply(deltaPayload(t, 5, 2, []battlev1.FrameDeltaField{2}, bad), VersionDelta); err != nil || ok {
		t.Fatalf("base_seq 不符必须丢弃: ok=%v err=%v", ok, err)
	}
	if *requests != 1 || dec.Drops() != 1 {
		t.Fatalf("请求=%d 丢弃=%d, 期望 1/1", *requests, dec.Drops())
	}
	if dec.LastSeq() != 1 {
		t.Fatalf("基线被坏增量改动了: lastSeq=%d, 期望 1", dec.LastSeq())
	}
	// 合法增量（base_seq=1, seq=2）到达：只包含 p-c 的新输入，坏增量的 p-ghost 不得出现。
	good := &battlev1.FrameBroadcastDelta{
		SetInputs: []*locksteppb.LockstepInput{{FrameId: 0, PlayerId: "p-c", Payload: []byte{7}}},
	}
	got, ok, err := dec.Apply(deltaPayload(t, 1, 2, []battlev1.FrameDeltaField{2}, good), VersionDelta)
	if err != nil || !ok {
		t.Fatalf("合法增量应交付: ok=%v err=%v", ok, err)
	}
	for _, in := range got.GetFrame().GetInputs() {
		if in.GetPlayerId() == "p-ghost" {
			t.Fatal("坏增量的输入混进了结果：发生了缓冲/猜测")
		}
	}
	if len(got.GetFrame().GetInputs()) != 3 {
		t.Fatalf("inputs 条数 = %d, 期望 3（基线 2 条 + 新增 p-c）", len(got.GetFrame().GetInputs()))
	}
}

// TestSnapshotMissingClears 验证「snapshot 缺失 = 本帧无快照 ⇒ 清空」，
// 未出现在 changed 中则沿用基线。
func TestSnapshotMissingClears(t *testing.T) {
	snap := &locksteppb.SnapshotMeta{FrameId: 1, StateHash: []byte{1}, SizeBytes: 8}
	dec, _ := newTestDecoder()
	dec.Apply(fullPayload(t, 1, snap), 2)

	keep, _, _ := dec.Apply(deltaPayload(t, 1, 2, nil, nil), VersionDelta)
	if !proto.Equal(keep.GetFrame().GetSnapshot(), snap) {
		t.Fatalf("未声明 SNAPSHOT 时应沿用基线快照: %v", keep.GetFrame().GetSnapshot())
	}

	clear, _, _ := dec.Apply(deltaPayload(t, 2, 3, []battlev1.FrameDeltaField{3}, nil), VersionDelta)
	if clear.GetFrame().GetSnapshot() != nil {
		t.Fatalf("声明 SNAPSHOT 但缺失 ⇒ 必须清空, got %v", clear.GetFrame().GetSnapshot())
	}
}

// TestMergeOverridesStateHash 验证 Go 解码侧的覆盖语义：增量帧的 state_hash **覆盖**基线
// （与全量帧同名同类型 bytes），空增量（changed 为空）同样覆盖——锁步靠当帧 hash 判失步。
func TestMergeOverridesStateHash(t *testing.T) {
	base := &battlev1.FrameBroadcast{BattleId: "b-1", Frame: &locksteppb.LockstepFrame{
		FrameId:   7,
		StateHash: []byte{0x01},
	}}
	want := []byte{0x02, 0x09}
	for _, changed := range [][]battlev1.FrameDeltaField{nil, {battlev1.FrameDeltaField_FRAME_DELTA_FIELD_INPUTS}} {
		d := &battlev1.FrameBroadcastDelta{BaseSeq: 7, Seq: 8, StateHash: want, Changed: changed}
		got := Merge(base, d)
		if !bytes.Equal(got.GetFrame().GetStateHash(), want) {
			t.Fatalf("changed=%v 时 state_hash = %x, 期望当帧 %x（不得沿用基线）",
				changed, got.GetFrame().GetStateHash(), want)
		}
		if !bytes.Equal(base.GetFrame().GetStateHash(), []byte{0x01}) {
			t.Fatal("Merge 改写了基线（基线必须只读）")
		}
	}
}
