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
	"google.golang.org/protobuf/proto"
)

// TestClipSyncReplyKeepsMaximalPrefix 验证截断点是「恰好装得下的最大连续前缀」：
// 以装下 k 帧的预算裁剪，必须保留 k 帧（不多不少），续传点 = 第 k 帧 + 1，且再多一帧就超预算。
func TestClipSyncReplyKeepsMaximalPrefix(t *testing.T) {
	base := fakeSyncReply(200)
	const kept = 57
	budget := encodedPayloadSize(t, syncCandidate(base, kept))
	got, err := clipSyncReply(base, budget)
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
	got, err := clipSyncReply(fakeSyncReply(3), 16)
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
	got, err := clipSyncReply(&battlev1.SyncFramesReply{CurrentFrame: 7}, framelimit.ReplyBudget)
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
