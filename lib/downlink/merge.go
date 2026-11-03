// Package downlink 是帧广播下行（ver=1/2/3）的**客户端解码与合并**实现（Go 侧 SDK 的
// ver=3 分支落点）：
//
//   - Merge 把增量载荷合并进基线，纯函数、无 IO、可属性测试；
//   - Decoder 是每战斗会话一份的解码状态机：只信 base_seq、跳变即丢弃并请求关键帧、
//     快照缺失即清空——**不猜、不缓冲、不乱序应用**（设计 §4.3）。
//
// 为什么不在 atlas-sdk-go：该 SDK 明确不持有游戏域消息（`direct/gamedomain_guard_test.go`
// 从依赖上禁止引游戏协议包），而合并必须看到 FrameBroadcast/FrameBroadcastDelta 的字段；
// 本包只依赖本仓 api 与 protobuf 运行时，是游戏侧「协议 + 会话」的 Go 落点。
package downlink

import (
	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// VersionDelta 是增量载荷的帧头 ver（ver=3），与 FramePayloadVersion.DELTA 一一对应。
const VersionDelta uint8 = 3

// Merge 把增量 d 合并到基线 base，返回新的完整广播（**不改入参**：基线只读，§9 R4）。
//
// 合并规则（设计 §3.2 表，逐条）：
//   - frame_id = d.seq（隐含，不单独传）；
//   - battle_id 增量永不携带 → 沿用基线（由关键帧/会话绑定提供）；
//   - server_time：changed 含 SERVER_TIME 时用绝对纳秒替换，否则沿用基线；
//   - inputs：先按 removed_player_ids 删除，再按 player_id 用 set_inputs 整条覆盖/追加，
//     未提及的保持基线，最后把全部 frame_id 改写为 seq（与全量帧语义一致）；
//   - snapshot：changed 含 SNAPSHOT 时用本条替换（**本条缺失 = 本帧无快照**，即清空），
//     否则沿用基线。
func Merge(base *battlev1.FrameBroadcast, d *battlev1.FrameBroadcastDelta) *battlev1.FrameBroadcast {
	seq := d.GetSeq()
	out := &battlev1.FrameBroadcast{BattleId: base.GetBattleId(), Frame: &locksteppb.LockstepFrame{
		FrameId:  seq,
		Inputs:   mergeInputs(base.GetFrame().GetInputs(), d),
		Snapshot: mergeSnapshot(base.GetFrame().GetSnapshot(), d),
	}}
	cur := base.GetFrame()
	out.Frame.StateHash = cur.GetStateHash() // 增量不携带 state_hash：沿用基线（见包注释与设计 §10 存疑项）
	out.Frame.ServerTime = cur.GetServerTime()
	if hasField(d, battlev1.FrameDeltaField_FRAME_DELTA_FIELD_SERVER_TIME) {
		out.Frame.ServerTime = timestamppb.New(epochNanos(d.GetServerTimeUnixNano()))
	}
	return out
}

// mergeInputs 做按 player_id 的稀疏替换，并把全部 frame_id 改写成本帧帧号。
func mergeInputs(base []*locksteppb.LockstepInput, d *battlev1.FrameBroadcastDelta) []*locksteppb.LockstepInput {
	removed := make(map[string]bool, len(d.GetRemovedPlayerIds()))
	for _, id := range d.GetRemovedPlayerIds() {
		removed[id] = true
	}
	set := make(map[string]*locksteppb.LockstepInput, len(d.GetSetInputs()))
	for _, in := range d.GetSetInputs() {
		set[in.GetPlayerId()] = in
	}
	out := make([]*locksteppb.LockstepInput, 0, len(base)+len(set))
	for _, in := range base {
		id := in.GetPlayerId()
		if removed[id] {
			continue
		}
		if repl, ok := set[id]; ok {
			out = append(out, rewriteFrame(repl, d.GetSeq()))
			delete(set, id)
			continue
		}
		out = append(out, rewriteFrame(in, d.GetSeq()))
	}
	for _, in := range d.GetSetInputs() {
		if _, ok := set[in.GetPlayerId()]; ok {
			out = append(out, rewriteFrame(in, d.GetSeq()))
		}
	}
	return out
}

// rewriteFrame 复制一条输入并把 frame_id 改写为本帧帧号（绝不改写入参）。
func rewriteFrame(in *locksteppb.LockstepInput, seq uint64) *locksteppb.LockstepInput {
	cp := proto.Clone(in).(*locksteppb.LockstepInput)
	cp.FrameId = seq
	return cp
}

// mergeSnapshot 按 SNAPSHOT 是否出现在 changed 决定「替换（含清空）」还是「沿用基线」。
func mergeSnapshot(base *locksteppb.SnapshotMeta, d *battlev1.FrameBroadcastDelta) *locksteppb.SnapshotMeta {
	if !hasField(d, battlev1.FrameDeltaField_FRAME_DELTA_FIELD_SNAPSHOT) {
		return base
	}
	return d.GetSnapshot() // nil = 本帧无快照（清空）
}

// hasField 判定 changed 里是否声明了某字段（重复项容忍）。
func hasField(d *battlev1.FrameBroadcastDelta, f battlev1.FrameDeltaField) bool {
	for _, got := range d.GetChanged() {
		if got == f {
			return true
		}
	}
	return false
}
