// 补帧回执的「单包封顶 + 续传点」：回执按**编码后字节预算**截断，被截掉的帧留在会话里，
// 由客户端按 next_frame 再要一次。超包回执在接入层会被整包丢弃（客户端连回执都收不到，
// 更 ack 不了），故任何一条 SyncFrames 回执都必须装得进一个帧。

package actor

import (
	"fmt"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	atlasjson "github.com/huangyuCN/atlas/encoding/json"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"google.golang.org/protobuf/proto"
)

// clipSyncReply 按编码后字节预算裁剪补帧回执：
//   - 保留能装进 budget 的最大**连续帧前缀**（missed 必须连续，客户端据此顺序回放）；
//   - 截断时 next_frame = 最后送达帧 + 1、has_more = true（客户端 ack 后续传）；
//   - 未截断时 next_frame = current_frame + 1、has_more = false（已追平，客户端收手）。
//
// 连一个帧组都装不下时返回可判定拒绝：宁可明确失败，也不下发必然被丢弃的超包回执。
func clipSyncReply(reply *battlev1.SyncFramesReply, budget int) (*battlev1.SyncFramesReply, error) {
	if len(reply.GetMissed()) == 0 {
		return withContinuation(reply, reply.GetCurrentFrame()+1, false), nil
	}
	kept := fitSyncGroups(reply, budget)
	if kept == 0 {
		return nil, syncOversizeErr(reply, budget)
	}
	out := syncCandidate(reply, kept)
	if !syncFitsBudget(out, budget) { // 防御：候选与最终产物同一组装口径，正常不会失败
		return nil, syncOversizeErr(reply, budget)
	}
	return out, nil
}

// withContinuation 写定续传点并返回同一条回执（就地写：回执是本次请求新建的）。
func withContinuation(reply *battlev1.SyncFramesReply, next uint64, more bool) *battlev1.SyncFramesReply {
	reply.NextFrame = next
	reply.HasMore = more
	return reply
}

// syncCandidate 返回「只带前 k 个帧组」的回执候选（续传字段按最终下发口径写定）：
// 二分测量与最终下发共用它，测量口径才等于真正落包的字节数。
func syncCandidate(base *battlev1.SyncFramesReply, k int) *battlev1.SyncFramesReply {
	groups := base.GetMissed()
	if k >= len(groups) {
		return withContinuation(base, base.GetCurrentFrame()+1, false)
	}
	return withContinuation(&battlev1.SyncFramesReply{
		CurrentFrame: base.GetCurrentFrame(),
		Snapshot:     base.GetSnapshot(),
		Missed:       groups[:k],
	}, frameOfGroup(groups[k-1])+1, true)
}

// fitSyncGroups 二分出能装进预算的最大帧组数（0 = 连一个帧组都装不下）。
// 帧组按帧号连续排列，回执大小随帧组数单调增（repeated 字段只增不减），故可二分；
// 末尾再线性收敛一次，兜住「续传点位数变化」这类一两个字节的微扰。
func fitSyncGroups(base *battlev1.SyncFramesReply, budget int) int {
	lo, hi := 0, len(base.GetMissed())
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if syncFitsBudget(syncCandidate(base, mid), budget) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	for lo > 0 && !syncFitsBudget(syncCandidate(base, lo), budget) {
		lo--
	}
	return lo
}

// syncFitsBudget 判断回执在**两种载荷编码**下都装得进预算：ver=1 走 protojson
// （EmitUnpopulated，回执最大的形态）、ver=2 走 protobuf 二进制。两者都算一遍，
// 「无论客户端用哪种编码」都不会超包——不靠「JSON 一定比二进制大」这类经验假设。
func syncFitsBudget(reply *battlev1.SyncFramesReply, budget int) bool {
	jsonBytes, err := atlasjson.MarshalOptions.Marshal(reply)
	if err != nil {
		return false
	}
	return len(jsonBytes) <= budget && proto.Size(reply) <= budget
}

// frameOfGroup 返回帧组的帧号：优先帧组自带的 frame_id，缺失时回落到组内首个输入的
// 帧号（旧回执只填后者，客户端就是靠它定位组的）。
func frameOfGroup(group *locksteppb.FrameInputs) uint64 {
	if id := group.GetFrameId(); id != 0 {
		return id
	}
	if inputs := group.GetInputs(); len(inputs) > 0 {
		return inputs[0].GetFrameId()
	}
	return 0
}

// syncOversizeErr 返回「单个帧组就超过单包预算」的可判定拒绝（reason 与超包口径一致）。
func syncOversizeErr(reply *battlev1.SyncFramesReply, budget int) error {
	return atlaserrors.New(500, atlaserrors.ReasonDownlinkFailed,
		fmt.Sprintf("补帧回执装不进单包预算 %d 字节（当前帧 %d）：单个帧组的输入载荷就超限",
			budget, reply.GetCurrentFrame()))
}
