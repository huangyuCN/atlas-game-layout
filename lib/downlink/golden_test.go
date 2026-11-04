// 跨语言 golden vectors 生成器（默认跳过，只读不跑）。
//
// 跑法（在模板仓根）：
//
//	UPDATE_DOWNLINK_GOLDEN=1 go test ./lib/downlink -run TestGenerateGoldenVectors -count=1
//
// 产物 lib/downlink/testdata/delta-golden.json 会被复制到 TS / C# SDK 的测试夹具；
// 三端用**同一批字节**做「增量合并 == 全量编码」的对拍。生成器只消费 Merge（不新造
// 合并逻辑），每一步都先用 proto.Equal 证明 Merge 结果 == 该 tick 的全量帧，再落盘。
package downlink

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// goldenStep 是一条增量与其合并结果（全部为 wire 字节的十六进制）。
type goldenStep struct {
	Seq     uint64 `json:"seq"`
	BaseSeq uint64 `json:"baseSeq"`
	Delta   string `json:"delta"`
	Merged  string `json:"merged"`
	Note    string `json:"note"`
}

// goldenFile 是跨语言夹具的完整形态。
type goldenFile struct {
	Source string       `json:"source"`
	Base   string       `json:"base"`
	Steps  []goldenStep `json:"steps"`
}

// goldenInput 构造一条输入（frame_id 会由合并器改写，这里故意给 0）。
func goldenInput(player string, payload []byte) *locksteppb.LockstepInput {
	return &locksteppb.LockstepInput{PlayerId: player, Payload: payload}
}

// TestGenerateGoldenVectors 生成跨语言向量（未设 UPDATE_DOWNLINK_GOLDEN 即跳过）。
func TestGenerateGoldenVectors(t *testing.T) {
	if os.Getenv("UPDATE_DOWNLINK_GOLDEN") != "1" {
		t.Skip("未设置 UPDATE_DOWNLINK_GOLDEN=1，跳过 golden 生成")
	}
	baseTime := int64(1_700_000_000_000_000_000)
	cur := &battlev1.FrameBroadcast{BattleId: "b-1", Frame: &locksteppb.LockstepFrame{
		FrameId:    1,
		ServerTime: timestamppb.New(time.Unix(0, baseTime)),
		StateHash:  []byte{0xA1},
		Inputs: []*locksteppb.LockstepInput{
			{FrameId: 1, PlayerId: "p-a", Payload: []byte{1}},
			{FrameId: 1, PlayerId: "p-b", Payload: []byte{2}},
		},
	}}
	baseBytes := mustMarshal(t, cur)

	type stepDef struct {
		note    string
		delta   *battlev1.FrameBroadcastDelta
		next    *battlev1.FrameBroadcast // 该 tick 的「独立全量编码」，用于对拍
		changed []battlev1.FrameDeltaField
	}
	hash := func(b byte) []byte { return []byte{b, 0x00} }
	steps := []stepDef{
		{ // 只有帧号推进（changed 空），hash 必须覆盖
			note:  "空增量：只有帧号推进，state_hash 覆盖基线",
			delta: &battlev1.FrameBroadcastDelta{Seq: 2, StateHash: hash(0xA2)},
			next:  fullOf(cur, 2, baseTime, hash(0xA2), nil, nil, cur.GetFrame().GetInputs()),
		},
		{ // server_time 绝对值替换
			note:    "只改 server_time（绝对值，不累加）",
			delta:   &battlev1.FrameBroadcastDelta{Seq: 3, ServerTimeUnixNano: baseTime + 1e9, StateHash: hash(0xA3)},
			changed: []battlev1.FrameDeltaField{battlev1.FrameDeltaField_FRAME_DELTA_FIELD_SERVER_TIME},
			next:    fullOf(cur, 3, baseTime+1e9, hash(0xA3), nil, nil, cur.GetFrame().GetInputs()),
		},
		{ // 稀疏替换 + 新增
			note: "inputs：p-a 整条替换、新增 p-c，未提及的 p-b 保持",
			delta: &battlev1.FrameBroadcastDelta{
				Seq:       4,
				StateHash: hash(0xA4),
				SetInputs: []*locksteppb.LockstepInput{goldenInput("p-a", []byte{9}), goldenInput("p-c", []byte{7})},
			},
			changed: []battlev1.FrameDeltaField{battlev1.FrameDeltaField_FRAME_DELTA_FIELD_INPUTS},
			next: fullOf(cur, 4, baseTime+1e9, hash(0xA4), nil, nil, []*locksteppb.LockstepInput{
				{FrameId: 4, PlayerId: "p-a", Payload: []byte{9}},
				{FrameId: 4, PlayerId: "p-b", Payload: []byte{2}},
				{FrameId: 4, PlayerId: "p-c", Payload: []byte{7}},
			}),
		},
		{ // 删除 + 新增（带 predicted/client_send_time）
			note: "inputs：先删 p-b 再追加 p-d（predicted/客户端时间）",
			delta: &battlev1.FrameBroadcastDelta{
				Seq:              5,
				StateHash:        hash(0xA5),
				RemovedPlayerIds: []string{"p-b"},
				SetInputs: []*locksteppb.LockstepInput{
					{PlayerId: "p-d", Payload: []byte{8}, Predicted: true, ClientSendTimeUnixNano: 12345},
				},
			},
			changed: []battlev1.FrameDeltaField{battlev1.FrameDeltaField_FRAME_DELTA_FIELD_INPUTS},
			next: fullOf(cur, 5, baseTime+1e9, hash(0xA5), nil, nil, []*locksteppb.LockstepInput{
				{FrameId: 5, PlayerId: "p-a", Payload: []byte{9}},
				{FrameId: 5, PlayerId: "p-c", Payload: []byte{7}},
				{FrameId: 5, PlayerId: "p-d", Payload: []byte{8}, Predicted: true, ClientSendTimeUnixNano: 12345},
			}),
		},
		{ // 快照出现
			note:    "snapshot：changed 含 SNAPSHOT 且携带值 ⇒ 替换",
			delta:   &battlev1.FrameBroadcastDelta{Seq: 6, StateHash: hash(0xA6), Snapshot: &locksteppb.SnapshotMeta{FrameId: 6, StateHash: []byte{6}, SizeBytes: 64}},
			changed: []battlev1.FrameDeltaField{battlev1.FrameDeltaField_FRAME_DELTA_FIELD_SNAPSHOT},
			next:    fullOf(cur, 6, baseTime+1e9, hash(0xA6), &locksteppb.SnapshotMeta{FrameId: 6, StateHash: []byte{6}, SizeBytes: 64}, nil, curInputs(5)),
		},
		{ // 快照清空
			note:    "snapshot：changed 含 SNAPSHOT 但缺失 ⇒ 清空",
			delta:   &battlev1.FrameBroadcastDelta{Seq: 7, StateHash: hash(0xA7)},
			changed: []battlev1.FrameDeltaField{battlev1.FrameDeltaField_FRAME_DELTA_FIELD_SNAPSHOT},
			next:    fullOf(cur, 7, baseTime+1e9, hash(0xA7), nil, nil, curInputs(6)),
		},
		{ // 三字段同帧 + 空 hash
			note: "三字段同帧（server_time+inputs+snapshot），state_hash 为空 ⇒ 不沿用基线",
			delta: &battlev1.FrameBroadcastDelta{
				Seq:                8,
				ServerTimeUnixNano: baseTime + 2e9,
				SetInputs:          []*locksteppb.LockstepInput{goldenInput("p-e", nil)},
				Snapshot:           &locksteppb.SnapshotMeta{FrameId: 8, SizeBytes: 128},
			},
			changed: []battlev1.FrameDeltaField{
				battlev1.FrameDeltaField_FRAME_DELTA_FIELD_SERVER_TIME,
				battlev1.FrameDeltaField_FRAME_DELTA_FIELD_INPUTS,
				battlev1.FrameDeltaField_FRAME_DELTA_FIELD_SNAPSHOT,
			},
			next: fullOf(cur, 8, baseTime+2e9, nil, &locksteppb.SnapshotMeta{FrameId: 8, SizeBytes: 128}, nil, append(curInputs(7), &locksteppb.LockstepInput{FrameId: 8, PlayerId: "p-e"})),
		},
	}

	out := goldenFile{
		Source: "atlas-game-layout/lib/downlink @ 55d255f (go1.26.5); generator: TestGenerateGoldenVectors",
		Base:   hex.EncodeToString(baseBytes),
	}
	prevBytes, prev := baseBytes, cur
	for i, st := range steps {
		st.delta.BaseSeq, st.delta.Changed = prev.GetFrame().GetFrameId(), st.changed
		deltaBytes := mustMarshal(t, st.delta)
		merged := Merge(prev, st.delta)
		if !proto.Equal(merged, st.next) {
			t.Fatalf("第 %d 步（%s）：合并结果 != 全量帧\n got=%v\nwant=%v", i+1, st.note, merged, st.next)
		}
		mergedBytes := mustMarshal(t, merged)
		if got := mustMarshal(t, st.next); string(got) != string(mergedBytes) {
			t.Fatalf("第 %d 步：merged 与全量帧字节不等", i+1)
		}
		out.Steps = append(out.Steps, goldenStep{
			Seq: st.delta.GetSeq(), BaseSeq: st.delta.GetBaseSeq(),
			Delta: hex.EncodeToString(deltaBytes), Merged: hex.EncodeToString(mergedBytes), Note: st.note,
		})
		prevBytes, prev = mergedBytes, merged
	}
	if len(prevBytes) == 0 || prev == nil {
		t.Fatal("生成结果为空")
	}
	path := filepath.Join("testdata", "delta-golden.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("建目录: %v", err)
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatalf("编码 JSON: %v", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		t.Fatalf("写文件: %v", err)
	}
	t.Logf("已生成 %s（%d 步）", path, len(out.Steps))
}

// fullOf 构造某 tick 的独立全量帧：inputs 一律把 frame_id 改写为该 tick。
func fullOf(base *battlev1.FrameBroadcast, seq uint64, nanos int64, hash []byte,
	snap *locksteppb.SnapshotMeta, _ []string, inputs []*locksteppb.LockstepInput) *battlev1.FrameBroadcast {
	cp := make([]*locksteppb.LockstepInput, 0, len(inputs))
	for _, in := range inputs {
		clone := proto.Clone(in).(*locksteppb.LockstepInput)
		clone.FrameId = seq
		cp = append(cp, clone)
	}
	return &battlev1.FrameBroadcast{BattleId: base.GetBattleId(), Frame: &locksteppb.LockstepFrame{
		FrameId:    seq,
		ServerTime: timestamppb.New(time.Unix(0, nanos)),
		StateHash:  hash,
		Inputs:     cp,
		Snapshot:   snap,
	}}
}

// curInputs 取当前合并基线的 inputs（只读；下一步会克隆）。
func curInputs(seq uint64) []*locksteppb.LockstepInput {
	base := &battlev1.FrameBroadcast{Frame: &locksteppb.LockstepFrame{
		FrameId: seq,
		Inputs: []*locksteppb.LockstepInput{
			{FrameId: seq, PlayerId: "p-a", Payload: []byte{9}},
			{FrameId: seq, PlayerId: "p-c", Payload: []byte{7}},
		},
	}}
	if seq >= 5 {
		base.Frame.Inputs = append(base.Frame.Inputs, &locksteppb.LockstepInput{FrameId: seq, PlayerId: "p-d", Payload: []byte{8}, Predicted: true, ClientSendTimeUnixNano: 12345})
	}
	return base.Frame.Inputs
}

// mustMarshal 序列化 protobuf，失败即终止（生成器不吞错）。
func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
