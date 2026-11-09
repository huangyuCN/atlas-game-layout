// 补帧回执封顶的单元用例：截断点取「能装下的最大连续前缀」、装不下即明确拒绝、
// 追平时给出「已追平」的续传点。集成口径（真会话 + 真编码）见 battle_syncclip_test.go。

package actor

import (
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/framelimit"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	atlasjson "github.com/huangyuCN/atlas/encoding/json"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	atlasframe "github.com/huangyuCN/atlas/transport/frame"
	"google.golang.org/protobuf/proto"
)

// TestClipSyncReplyKeepsMaximalPrefix 验证截断点是「恰好装得下的最大连续前缀」：
// 以装下 k 帧的预算裁剪，必须保留 k 帧（不多不少），续传点 = 第 k 帧 + 1，且再多一帧就超预算。
func TestClipSyncReplyKeepsMaximalPrefix(t *testing.T) {
	base := fakeSyncReply(200)
	const kept = 57
	budget := encodedPayloadSize(t, syncCandidate(base, kept))
	got, err := clipSyncReply(base, budget, atlasframe.Version)
	if err != nil {
		t.Fatalf("预算恰好装下 %d 帧却报错: %v", kept, err)
	}
	if n := len(got.GetMissed()); n != kept {
		t.Fatalf("保留帧组 = %d，期望 %d（应取最大前缀）", n, kept)
	}
	if got.GetNextFrame() != kept+1 || !got.GetHasMore() {
		t.Fatalf("续传点不符: next_frame=%d has_more=%v（期望 %d/true）",
			got.GetNextFrame(), got.GetHasMore(), kept+1)
	}
	if n := encodedPayloadSize(t, got); n > budget {
		t.Fatalf("裁剪后仍超预算: %d > %d", n, budget)
	}
	if encodedPayloadSize(t, syncCandidate(base, kept+1)) <= budget {
		t.Fatalf("再装一帧也没超预算（%d 字节），说明截断点不是最大前缀", budget)
	}
	if got.GetCurrentFrame() != base.GetCurrentFrame() || got.GetSnapshot() != base.GetSnapshot() {
		t.Fatal("裁剪不得改动当前帧与快照（客户端要据此判断整体进度）")
	}
}

// TestClipSyncReplyRejectsUnfittableFrame 验证「连一个帧组都装不下」时明确拒绝：
// 绝不下发必然被整包丢弃的超包回执，reason 与超包口径一致（客户端可判定）。
func TestClipSyncReplyRejectsUnfittableFrame(t *testing.T) {
	got, err := clipSyncReply(fakeSyncReply(3), 16, atlasframe.Version)
	if got != nil {
		t.Fatal("装不下时不得返回回执（超包必然被丢弃）")
	}
	if se := atlaserrors.FromError(err); se.Reason != atlaserrors.ReasonDownlinkFailed || se.Code != 500 {
		t.Fatalf("reason/code = %q/%d，期望 %q/500", se.Reason, se.Code, atlaserrors.ReasonDownlinkFailed)
	}
}

// TestClipSyncReplyCaughtUp 验证没有缺失帧时直接给出「已追平」的续传点
// （next_frame = current_frame + 1、has_more = false）：客户端据此收手，不靠猜。
func TestClipSyncReplyCaughtUp(t *testing.T) {
	got, err := clipSyncReply(&battlev1.SyncFramesReply{CurrentFrame: 7}, framelimit.ReplyBudget, atlasframe.Version)
	if err != nil {
		t.Fatalf("无缺失帧不该报错: %v", err)
	}
	if len(got.GetMissed()) != 0 || got.GetNextFrame() != 8 || got.GetHasMore() {
		t.Fatalf("追平口径不符: missed=%d next_frame=%d has_more=%v（期望 0/8/false）",
			len(got.GetMissed()), got.GetNextFrame(), got.GetHasMore())
	}
}

// fakeSyncReply 构造一份「缺失 frames 帧」的补帧回执：每帧两名玩家、各 1 字节载荷
// （与现场双人对局的回执形态一致，只是帧数可控）。
func fakeSyncReply(frames int) *battlev1.SyncFramesReply {
	reply := &battlev1.SyncFramesReply{CurrentFrame: uint64(frames)}
	for f := 1; f <= frames; f++ {
		group := &locksteppb.FrameInputs{FrameId: uint64(f)}
		for _, p := range []string{"p-a", "p-b"} {
			group.Inputs = append(group.Inputs, &locksteppb.LockstepInput{
				FrameId: uint64(f), PlayerId: p, Payload: []byte{0},
			})
		}
		reply.Missed = append(reply.Missed, group)
	}
	return reply
}

// encodedPayloadSize 返回回执载荷在两种编码下的较大字节数（不含 5 字节回执信封）：
// 与 clipSyncReply 的判定口径同源，避免用例自己另算一套。
func encodedPayloadSize(t *testing.T, reply *battlev1.SyncFramesReply) int {
	t.Helper()
	jsonBytes, err := atlasjson.MarshalOptions.Marshal(reply)
	if err != nil {
		t.Fatalf("protojson 编码失败: %v", err)
	}
	return max(len(jsonBytes), proto.Size(reply))
}

// TestSyncFitsBudgetSkipsJSONForProtobuf 钉住热路径：protobuf 客户端（ver=2）**一次 JSON 编码都不做**。
//
// 为什么关键：这段判定跑在单线程的 actor cell 里，原实现无条件把回执整包 protojson 编码一遍，
// 又被二分查找反复调用；30 Hz 下 4,096 个探针/秒把它放大成 battle 55% 的 CPU（profile 实测），
// 最终拖垮 cell 导致整段崩塌。而实测 4,990/5,000 个客户端协商的都是 ver=2。
func TestSyncFitsBudgetSkipsJSONForProtobuf(t *testing.T) {
	reply := fakeSyncReply(200)
	budget := proto.Size(reply) // 只够二进制：JSON 一定超
	jsonBytes, err := atlasjson.MarshalOptions.Marshal(reply)
	if err != nil {
		t.Fatalf("protojson 编码失败: %v", err)
	}
	if len(jsonBytes) <= budget {
		t.Fatalf("用例前提不成立：JSON(%d) 应大于预算(%d)", len(jsonBytes), budget)
	}

	orig := marshalJSONFn
	calls := 0
	marshalJSONFn = func(m proto.Message) ([]byte, error) {
		calls++
		return orig(m)
	}
	t.Cleanup(func() { marshalJSONFn = orig })

	if !syncFitsBudget(reply, budget, atlasframe.Version2) {
		t.Fatal("protobuf 客户端：二进制装得下就应通过")
	}
	if calls != 0 {
		t.Fatalf("protobuf 客户端不该做 JSON 编码，实际调用 %d 次", calls)
	}
}

// TestSyncFitsBudgetChecksJSONForProtojson 钉住正确性：ver=1 与「版本未知」都必须按 JSON 判定
// （未知时取最保守口径，与旧行为一致——宁可少发帧，也不下发必然被整包丢弃的超包回执）。
func TestSyncFitsBudgetChecksJSONForProtojson(t *testing.T) {
	reply := fakeSyncReply(200)
	budget := proto.Size(reply) // 二进制装得下、JSON 装不下
	if syncFitsBudget(reply, budget, atlasframe.Version) {
		t.Fatal("protojson 客户端：JSON 超预算必须判不合格")
	}
	if syncFitsBudget(reply, budget, 0) {
		t.Fatal("版本未知时必须取最保守口径（JSON 也要检查）")
	}
}

// TestClipSyncReplyKeepsMoreForProtobuf 钉住收益：同样的预算下，protobuf 客户端能一次拿到
// 全部帧（JSON 口径会裁掉大半）——这正是按版本分流要拿到的效果。
func TestClipSyncReplyKeepsMoreForProtobuf(t *testing.T) {
	base := fakeSyncReply(200)
	budget := proto.Size(syncCandidate(base, 200)) // 二进制全量刚好装下（含续传字段）

	jsonGot, err := clipSyncReply(base, budget, atlasframe.Version)
	if err != nil {
		t.Fatalf("protojson 裁剪报错: %v", err)
	}
	binGot, err := clipSyncReply(base, budget, atlasframe.Version2)
	if err != nil {
		t.Fatalf("protobuf 裁剪报错: %v", err)
	}
	if n := len(binGot.GetMissed()); n != 200 {
		t.Fatalf("protobuf 客户端应一次拿到全部 200 帧，实际 %d", n)
	}
	if n := len(jsonGot.GetMissed()); n >= 200 {
		t.Fatalf("protojson 口径应因 JSON 更大而裁剪，实际未裁（%d）", n)
	}
	if binGot.GetHasMore() {
		t.Fatal("protobuf 客户端拿全了帧，has_more 应为 false")
	}
}
