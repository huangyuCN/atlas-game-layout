// 补帧回执「单包封顶 + 续传点」的用例：客户端落后数百帧时，回执必须仍装得进一个帧
// （超包即被接入层丢弃 → 客户端拿不到回执 → 永远 ack 不了 → 下行自锁），
// 被截掉的帧由客户端按续传点再要一次，逐批取完。

package actor

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlasjson "github.com/huangyuCN/atlas/encoding/json"
	"github.com/huangyuCN/atlas/transport/frame"
	"google.golang.org/protobuf/proto"
)

// testMaxBodySize 是落到网线上的单帧 body 上限：接入层回程单包 16 KiB 减去帧头
// （与 services/battle/internal/server.MaxFrameBodySize 同一口径，测试侧独立钉住，
// 免得「被测常量自己也改了」把用例带绿）。
const testMaxBodySize = 16*1024 - frame.HeaderSize

// TestSyncFramesReplyFitsOnePacket 验证补帧回执**永不超出一个帧**（本轮的核心断言）：
// 客户端落后 500 帧时，旧实现把缺失帧全塞进一条回执（protojson 数十 KB），
// 接入层按整个数据报判超限后静默丢弃 ⇒ 客户端一帧也拿不到、ack 水位永不动 ⇒ 下行自锁。
// 新实现按**编码后字节预算**截断，故两种载荷编码（ver=1 protojson / ver=2 protobuf）都装得下。
func TestSyncFramesReplyFitsOnePacket(t *testing.T) {
	env, player := newBehindEnv(t, 500)
	reply := syncReply(t, env, player, 0)
	if got := len(reply.GetMissed()); got == 0 {
		t.Fatal("落后 500 帧却没返回任何缺失帧（用例场景没构造出来）")
	}
	for _, ver := range []uint8{frame.Version, frame.Version2} {
		if n := wireBodySize(t, ver, reply); n > testMaxBodySize {
			t.Fatalf("ver=%d 回执 body %d 字节 > 单包上限 %d：整包会被丢弃，客户端 ack 不了",
				ver, n, testMaxBodySize)
		}
	}
}

// TestSyncFramesBehindClientCatchesUp 验证落后的客户端能**逐批追上**：每批都装得进一个包，
// 客户端只 ack 已收到的部分（last_seen = next_frame - 1），下一批接着送；
// 最终收到的帧号从首批起点起连续无断档，且追平后明确给出 next_frame = current_frame + 1。
func TestSyncFramesBehindClientCatchesUp(t *testing.T) {
	env, player := newBehindEnv(t, 500)
	got, err := catchUp(t, env, player, 0, 20)
	if err != nil {
		t.Fatalf("落后 500 帧未追上: %v", err)
	}
	if got.batches < 2 {
		t.Fatalf("500 帧只用了 %d 批（回执应按单包上限分批）", got.batches)
	}
	if len(got.frames) < 500 {
		t.Fatalf("补回帧数 = %d，期望 ≥500", len(got.frames))
	}
	assertConsecutive(t, got.frames)
	last := got.last
	if last.GetNextFrame() != last.GetCurrentFrame()+1 || last.GetHasMore() {
		t.Fatalf("追平后应为 next_frame=current_frame+1 且 has_more=false，实际 next=%d current=%d more=%v",
			last.GetNextFrame(), last.GetCurrentFrame(), last.GetHasMore())
	}
	for i, group := range last.GetMissed() {
		if group.GetFrameId() == 0 {
			t.Fatalf("第 %d 个帧组没带 frame_id（客户端只能去猜组属于哪一帧）", i)
		}
	}
}

// TestSyncFramesPartialAckAdvancesDownlink 钉住本轮症状：**下行窗口能持续前进**。
//
// 现场：回执超包被整包丢弃 ⇒ 客户端 ack 水位钉死 ⇒ 每连接下行恒等于连上后先到的那 60 帧
// （4096 连接 × 60 = 恰好 245,760）。用例把这条链复现到可断言粒度：客户端在服务端跑到
// 500 帧时才建立下行（此前的推送按现场口径丢在连接之外），随后只 ack 已收到的部分；
// 断言它最终拿到的**连续**下行帧数远大于「连接后先到的那些推送」——旧实现首批超包，
// 补帧一帧也送不到，这个数会精确等于推送数（窗口没前进）。
func TestSyncFramesPartialAckAdvancesDownlink(t *testing.T) {
	env, player := newBehindEnv(t, 500)
	connectedAt := len(pushesOf(t, env, player)) // 客户端此刻才连上
	got, err := catchUp(t, env, player, 0, 20)
	// 追平后再推进一段：ack 之后的常态推送应继续送达（窗口前进后才有的现象）。
	from := maxBroadcastFrame(env) + 1
	sendIdleInputs(t, env, newTestContext(), from, from+59)
	waitFrameBroadcast(t, env, from+59)

	pushed := pushesOf(t, env, player)[connectedAt:]
	held := uniqueSorted(append(append([]uint64(nil), got.frames...), pushed...))
	heldLen := contiguousLen(held)
	if heldLen <= len(pushed) {
		t.Fatalf("下行帧数被钉在 %d（连接后推送 %d 帧、补帧未送达 err=%v）：窗口没有前进",
			heldLen, len(pushed), err)
	}
	if err != nil {
		t.Fatalf("补帧未追上: %v", err)
	}
	if heldLen < 500 {
		t.Fatalf("连续可用的下行帧数 = %d < 500（历史帧没补齐）", heldLen)
	}
}

// synced 是一次「逐批 ack 补帧」的结果。
type synced struct {
	batches int      // 请求批数（含最后一通）
	frames  []uint64 // 逐批收到的帧号（按批拼接）
	last    *battlev1.SyncFramesReply
}

// catchUp 模拟客户端的补帧循环：每批只 ack 已收到的部分（last_seen = next_frame - 1），
// 直到 has_more = false。任何一批超过单包上限即按现场口径判该批失败（整包被丢弃、
// 客户端一帧也拿不到、ack 水位不动）——这正是本轮要修的自锁点，故返回错误而非断言。
func catchUp(t *testing.T, env *battleEnv, player string, lastSeen uint64, maxBatches int) (synced, error) {
	t.Helper()
	var out synced
	for out.batches < maxBatches {
		reply := syncReply(t, env, player, lastSeen)
		out.batches++
		out.last = reply
		if n := wireBodySize(t, frame.Version, reply); n > testMaxBodySize {
			return out, fmt.Errorf("第 %d 批回执 body %d 字节 > 单包上限 %d：整包被丢弃，客户端 ack 不了",
				out.batches, n, testMaxBodySize)
		}
		out.frames = append(out.frames, frameIDsOf(reply)...)
		if !reply.GetHasMore() {
			return out, nil
		}
		next := reply.GetNextFrame()
		if next <= lastSeen+1 {
			return out, fmt.Errorf("续传点没有前进（last_seen=%d next_frame=%d）：客户端会原地打转",
				lastSeen, next)
		}
		lastSeen = next - 1
	}
	return out, fmt.Errorf("超过 %d 批仍未追上（last_seen=%d）", maxBatches, lastSeen)
}

// frameIDsOf 返回回执里各帧组的帧号：优先用帧组自带的 frame_id，缺省回落到组内第一个
// 输入的帧号（旧实现只填后者，客户端就是靠它定位的）。
func frameIDsOf(reply *battlev1.SyncFramesReply) []uint64 {
	groups := reply.GetMissed()
	out := make([]uint64, 0, len(groups))
	for _, group := range groups {
		frame := group.GetFrameId()
		if frame == 0 && len(group.GetInputs()) > 0 {
			frame = group.GetInputs()[0].GetFrameId()
		}
		out = append(out, frame)
	}
	return out
}

// pushesOf 返回直连下行已送达该玩家的帧号（推送顺序即帧号顺序）。
func pushesOf(t *testing.T, env *battleEnv, player string) []uint64 {
	t.Helper()
	frames, _, _ := env.pusher.snapshots()
	list := frames[player]
	out := make([]uint64, 0, len(list))
	for _, f := range list {
		out = append(out, f.GetFrameId())
	}
	return out
}

// uniqueSorted 去重并升序排序。
func uniqueSorted(ids []uint64) []uint64 {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := ids[:0]
	for i, id := range ids {
		if i == 0 || id != ids[i-1] {
			out = append(out, id)
		}
	}
	return out
}

// contiguousLen 返回从最小帧号起连续覆盖到的帧数（断档即停）。
func contiguousLen(ids []uint64) int {
	if len(ids) == 0 {
		return 0
	}
	n := 1
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			break
		}
		n++
	}
	return n
}

// assertConsecutive 断言帧号序列严格连续（续传点算错会断档或重发）。
func assertConsecutive(t *testing.T, ids []uint64) {
	t.Helper()
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			t.Fatalf("第 %d 个帧号 %d 与上一个 %d 不连续（续传点算错会断档或重发）", i, ids[i], ids[i-1])
		}
	}
}

// newBehindEnv 起一局战斗并把时钟推进 frames 帧，返回环境与一个「落后 frames 帧」的
// 参战玩家（其 ack 水位仍为 0，与服务端当前帧差 frames 帧）。
func newBehindEnv(t *testing.T, frames uint64) (*battleEnv, string) {
	t.Helper()
	cfg := testBattleConfig()
	cfg.TickInterval = int64(time.Millisecond) // 500 帧 ≈ 0.5s（缺省 10ms 要 5s）
	cfg.MaxFrames = frames + 100000            // 别让帧数上限抢先结算（结算会关停 actor、名单被重置）
	cfg.TrackLen = 1 << 20                     // 赛道足够长：两人原地不动，不会冲线结算
	env := newBattleEnvWith(t, cfg)
	ctx := newTestContext()
	seedAndJoin(t, env, ctx)
	from := maxBroadcastFrame(env) + 1
	sendIdleInputs(t, env, ctx, from, from+frames-1)
	waitFrameBroadcast(t, env, from+frames-1)
	assertNotSettled(t, env, from+frames-1) // 场景前提：对局还活着（结算会清空名单，补帧会被拒）
	return env, "p-a"
}

// sendIdleInputs 发送 [from, to] 闭区间的帧输入：两名参战玩家都原地不动（步长 0），
// 保证对局不会因冲线提前结算，只把帧历史做长。
func sendIdleInputs(t *testing.T, env *battleEnv, ctx context.Context, from, to uint64) {
	t.Helper()
	for i := from; i <= to; i++ {
		for _, p := range []string{"p-a", "p-b"} {
			sender, err := types.NewPID(playerType, p)
			if err != nil {
				t.Fatalf("sender PID %s: %v", p, err)
			}
			if err := env.rt.Tell(ctx, env.pid, &battlev1.FrameInputReq{
				BattleId: "b-test01",
				Input:    &locksteppb.LockstepInput{FrameId: i, PlayerId: p, Payload: []byte{0}},
			}, core.WithSender(sender)); err != nil {
				t.Fatalf("帧输入 %s@%d: %v", p, i, err)
			}
		}
	}
}

// syncReply 以指定 ack 水位发起一次补帧（客户端 op 的发起者身份经 sender 注入）。
func syncReply(t *testing.T, env *battleEnv, playerID string, lastSeen uint64) *battlev1.SyncFramesReply {
	t.Helper()
	raw, err := env.askAs(newTestContext(), playerID,
		&battlev1.SyncFramesReq{BattleId: "b-test01", LastSeenFrame: lastSeen})
	if err != nil {
		t.Fatalf("SyncFrames(last_seen=%d): %v", lastSeen, err)
	}
	reply, ok := raw.(*battlev1.SyncFramesReply)
	if !ok {
		t.Fatalf("补帧回执类型 %T 不符", raw)
	}
	return reply
}

// wireBodySize 返回回执在帧面上的 **body** 字节数：载荷按请求版本编码后套 Reply 信封
// （[hasError 1][dataLen 4]，见 atlas transport/frame/reply.go），与引擎实际写出的口径一致。
func wireBodySize(t *testing.T, ver uint8, reply *battlev1.SyncFramesReply) int {
	t.Helper()
	var payload []byte
	var err error
	if ver == frame.Version2 {
		payload, err = proto.Marshal(reply)
	} else {
		payload, err = atlasjson.MarshalOptions.Marshal(reply)
	}
	if err != nil {
		t.Fatalf("ver=%d 回执编码失败: %v", ver, err)
	}
	body := frame.EncodeReply(payload, nil, "")
	defer frame.PutBuf(body)
	return len(body)
}
