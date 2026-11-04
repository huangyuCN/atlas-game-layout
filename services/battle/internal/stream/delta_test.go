// 下行增量编码（ver=3）的验收用例：不变量 I1/I4/I5 + 恢复三层（lastSeq / 关键帧 / 限流）。
//
// 这些用例是**验收断言**，不是收益指标：增量只省字节，包数/帧数与 ver=2 完全相同。

package stream

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/downlink"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newDeltaRegistry 返回挂了 KCP/UDP 假端口、已开增量的注册表。
func newDeltaRegistry(t *testing.T, cfg DeltaConfig) (*Registry, *fakePort, *fakePort) {
	t.Helper()
	reg, err := NewRegistryDelta(DefaultEndedTTL, cfg)
	if err != nil {
		t.Fatalf("NewRegistryDelta: %v", err)
	}
	kcpPort, udpPort := &fakePort{}, &fakePort{}
	reg.BindPort(transport.KindKCP, kcpPort)
	reg.BindPort(transport.KindUDP, udpPort)
	return reg, kcpPort, udpPort
}

// deltaBenchPlayers 是属性测试的玩家数（与生产 10 人局同量级）。
const deltaBenchPlayers = 10

// frameHash 返回第 seq 帧的**当帧**状态哈希（每帧都不同，便于断言「不是基线的 hash」）。
func frameHash(seq uint64) []byte { return []byte{byte(seq >> 8), byte(seq), 0x5a} }

// randomFrame 生成第 seq 帧的完整快照（10 人、每人 32 B payload）。
func randomFrame(_ *rand.Rand, seq uint64, snap bool) *locksteppb.LockstepFrame {
	f := &locksteppb.LockstepFrame{
		FrameId:    seq,
		StateHash:  frameHash(seq),
		ServerTime: timestamppb.New(time.Unix(1700000000, int64(seq)*1000)),
	}
	if snap {
		f.Snapshot = &locksteppb.SnapshotMeta{FrameId: seq, StateHash: []byte{byte(seq)}, SizeBytes: 512}
	}
	for i := 0; i < deltaBenchPlayers; i++ {
		f.Inputs = append(f.Inputs, &locksteppb.LockstepInput{
			FrameId:  seq,
			PlayerId: fmt.Sprintf("p-%d", i),
			Payload:  []byte{byte(i)},
		})
	}
	return f
}

// nextFrame 从 prev 派生下一帧：多数输入「无新输入」（逐字节相同，只改 frame_id）、
// 少数玩家换 payload、快照按低概率出现/消失。
func nextFrame(rng *rand.Rand, prev *locksteppb.LockstepFrame) *locksteppb.LockstepFrame {
	cur := randomFrame(rng, prev.GetFrameId()+1, rng.IntN(8) == 0)
	cur.Snapshot = prev.GetSnapshot()
	if cur.Snapshot != nil && cur.Snapshot.GetFrameId() > 0 {
		cur.Snapshot = &locksteppb.SnapshotMeta{FrameId: cur.GetFrameId(), StateHash: cur.Snapshot.GetStateHash(), SizeBytes: cur.Snapshot.GetSizeBytes()}
	}
	for i, in := range cur.Inputs {
		if rng.IntN(10) != 0 { // 90% 无新输入：payload 与上一帧逐字节相同
			in.Payload = prev.GetInputs()[i].GetPayload()
		} else {
			in.Payload = []byte{byte(i), byte(cur.GetFrameId())}
		}
		in.ClientSendTimeUnixNano = prev.GetInputs()[i].GetClientSendTimeUnixNano()
	}
	return cur
}

// TestDeltaMergeEqualsFull ①（属性测试）：对多组随机输入序列，
// 「关键帧 + 逐帧增量合并」必须与同帧全量编码逐字段相等（I1/I2）。
func TestDeltaMergeEqualsFull(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	prev := randomFrame(rng, 1, true)
	for step := 0; step < 300; step++ {
		cur := nextFrame(rng, prev)
		delta := diffFrames(prev, cur)
		merged := downlink.Merge(&battlev1.FrameBroadcast{BattleId: "b-1", Frame: prev}, delta)
		want := &battlev1.FrameBroadcast{BattleId: "b-1", Frame: cur}
		if merged.GetFrame().GetFrameId() != cur.GetFrameId() {
			t.Fatalf("第 %d 步 frame_id = %d, 期望 %d", step, merged.GetFrame().GetFrameId(), cur.GetFrameId())
		}
		if !bytes.Equal(merged.GetFrame().GetStateHash(), cur.GetStateHash()) {
			t.Fatalf("第 %d 步 state_hash 停在基线（= %x），期望当帧 %x",
				step, merged.GetFrame().GetStateHash(), cur.GetStateHash())
		}
		if !proto.Equal(merged.GetFrame().GetServerTime(), cur.GetServerTime()) {
			t.Fatalf("第 %d 步 server_time 不等: %v vs %v", step, merged.GetFrame().GetServerTime(), cur.GetServerTime())
		}
		if !proto.Equal(merged.GetFrame().GetSnapshot(), cur.GetSnapshot()) {
			t.Fatalf("第 %d 步 snapshot 不等: %v vs %v", step, merged.GetFrame().GetSnapshot(), cur.GetSnapshot())
		}
		if len(merged.GetFrame().GetInputs()) != len(cur.GetInputs()) {
			t.Fatalf("第 %d 步 inputs 条数 = %d, 期望 %d", step, len(merged.GetFrame().GetInputs()), len(cur.GetInputs()))
		}
		for i := range cur.GetInputs() {
			if !proto.Equal(merged.GetFrame().GetInputs()[i], cur.GetInputs()[i]) {
				t.Fatalf("第 %d 步第 %d 条输入不等: %v vs %v", step, i, merged.GetFrame().GetInputs()[i], cur.GetInputs()[i])
			}
		}
		_ = want
		prev = cur
	}
}

// TestDeltaEmptyCarriesCurrentHash ②：**没有任何字段变化**的增量（changed 为空、只有帧号推进）
// 也必须携带当帧 state_hash，合并后不得停在基线 hash 上。
func TestDeltaEmptyCarriesCurrentHash(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	prev := randomFrame(rng, 1, true)
	cur := proto.Clone(prev).(*locksteppb.LockstepFrame)
	cur.FrameId = prev.GetFrameId() + 1
	cur.StateHash = frameHash(cur.GetFrameId()) // 输入/时间/快照全同，只有帧号与 hash 变

	delta := diffFrames(prev, cur)
	if len(delta.GetChanged()) != 0 {
		t.Fatalf("空增量的 changed = %v, 期望为空", delta.GetChanged())
	}
	if !bytes.Equal(delta.GetStateHash(), cur.GetStateHash()) {
		t.Fatalf("空增量 state_hash = %x, 期望当帧 %x（不得缺省为基线）", delta.GetStateHash(), cur.GetStateHash())
	}
	merged := downlink.Merge(&battlev1.FrameBroadcast{BattleId: "b-1", Frame: prev}, delta)
	if !bytes.Equal(merged.GetFrame().GetStateHash(), cur.GetStateHash()) {
		t.Fatalf("空增量合并后 state_hash = %x, 期望当帧 %x", merged.GetFrame().GetStateHash(), cur.GetStateHash())
	}
}

// TestDeltaWireCarriesCurrentHash ③：真走 wire（encodePlan → protobuf 编解码），
// 第 2 帧的增量载荷解码后的 state_hash 必须等于当帧 hash（不是基线第 1 帧的）。
func TestDeltaWireCarriesCurrentHash(t *testing.T) {
	reg, port, _ := newDeltaRegistry(t, DeltaConfig{KeyframeEvery: 1000})
	reg.Register("p-a", kcpConn(7, "b-1"))
	reg.NegotiatePayloadVersion("p-a", []battlev1.FramePayloadVersion{3})
	rng := rand.New(rand.NewPCG(13, 14))
	prev := randomFrame(rng, 1, false)
	var cur *locksteppb.LockstepFrame
	for seq := uint64(1); seq <= 2; seq++ { // 第 1 帧必全量，第 2 帧才是增量
		cur = nextFrame(rng, prev)
		cur.FrameId = seq
		if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, cur); err != nil {
			t.Fatalf("PublishFrames: %v", err)
		}
		prev = cur
	}
	last := port.raw[len(port.raw)-1]
	if last.version != VersionDelta {
		t.Fatalf("第 2 帧 ver = %d, 期望 %d", last.version, VersionDelta)
	}
	var d battlev1.FrameBroadcastDelta
	if err := proto.Unmarshal(last.payload, &d); err != nil {
		t.Fatalf("增量载荷解码失败: %v", err)
	}
	if !bytes.Equal(d.GetStateHash(), cur.GetStateHash()) {
		t.Fatalf("线上增量 state_hash = %x, 期望当帧 %x", d.GetStateHash(), cur.GetStateHash())
	}
}

// TestDeltaUDPConnectionGetsVersion2 ④：裸 UDP 面**自动降级**发 ver=2 全量，
// 即使该连接声明了 ver=3（udp 客户端零改动）。
func TestDeltaUDPConnectionGetsVersion2(t *testing.T) {
	reg, _, udpPort := newDeltaRegistry(t, DeltaConfig{})
	reg.Register("p-a", Conn{Kind: transport.KindUDP, Peer: "1.2.3.4:9", BattleID: "b-1"})
	got := reg.NegotiatePayloadVersion("p-a", []battlev1.FramePayloadVersion{battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_DELTA})
	if got != battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_PROTOBUF {
		t.Fatalf("UDP 协商版本 = %v, 期望 PROTOBUF（该面永不见 ver=3）", got)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, randomFrame(rand.New(rand.NewPCG(1, 2)), seq, false)); err != nil {
			t.Fatalf("PublishFrames: %v", err)
		}
	}
	for i, rec := range udpPort.raw {
		if rec.version != frame.Version2 {
			t.Fatalf("UDP 第 %d 帧 ver = %d, 期望 %d", i, rec.version, frame.Version2)
		}
	}
}

// TestDeltaPacketCountUnchanged ⑤（验收断言，不是收益）：ver=2 轮与 ver=3 轮的写帧次数相等。
func TestDeltaPacketCountUnchanged(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	full := randomFrame(rng, 1, false)
	deltaReg, deltaPort, _ := newDeltaRegistry(t, DeltaConfig{})
	plainReg, plainPort, _ := newTestRegistry()
	for _, reg := range []*Registry{deltaReg, plainReg} {
		reg.Register("p-a", kcpConn(7, "b-1"))
	}
	deltaReg.NegotiatePayloadVersion("p-a", []battlev1.FramePayloadVersion{battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_DELTA})
	prev := full
	for seq := uint64(1); seq <= 5; seq++ {
		cur := nextFrame(rng, prev)
		cur.FrameId = seq
		if err := deltaReg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, cur); err != nil {
			t.Fatalf("delta PublishFrames: %v", err)
		}
		if err := plainReg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, cur); err != nil {
			t.Fatalf("full PublishFrames: %v", err)
		}
		prev = cur
	}
	if len(deltaPort.raw) != len(plainPort.raw) {
		t.Fatalf("ver=3 写帧次数 %d ≠ ver=2 的 %d（包数必须不变）", len(deltaPort.raw), len(plainPort.raw))
	}
	if len(deltaPort.raw) != 5 {
		t.Fatalf("写帧次数 = %d, 期望 5（每 tick 每连接一次）", len(deltaPort.raw))
	}
}

// TestDeltaVersionMatrix ⑥：{客户端声明} × {服务端开关} × {连接 Kind} 的跨版本矩阵，
// 断言下发的帧头 ver 与载荷自洽：未声明 3 的连接永不见 ver=3，UDP 永不见 ver=3。
func TestDeltaVersionMatrix(t *testing.T) {
	cases := []struct {
		name     string
		delta    bool
		kind     transport.Kind
		accepted []battlev1.FramePayloadVersion
		want     uint8
	}{
		{"未开开关-缺省客户端", false, transport.KindKCP, nil, 1},
		{"未开开关-声明3", false, transport.KindKCP, []battlev1.FramePayloadVersion{3}, 1},
		{"开开关-未声明", true, transport.KindKCP, nil, 2},
		{"开开关-声明2", true, transport.KindKCP, []battlev1.FramePayloadVersion{2}, 2},
		{"开开关-声明3", true, transport.KindKCP, []battlev1.FramePayloadVersion{1, 2, 3}, 3},
		{"开开关-UDP声明3", true, transport.KindUDP, []battlev1.FramePayloadVersion{3}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var reg *Registry
			var port *fakePort
			if tc.delta {
				var udpPort *fakePort
				reg, port, udpPort = newDeltaRegistry(t, DeltaConfig{})
				if tc.kind == transport.KindUDP {
					port = udpPort
				}
			} else {
				reg, port, _ = newTestRegistry()
			}
			c := kcpConn(7, "b-1")
			if tc.kind == transport.KindUDP {
				c = Conn{Kind: transport.KindUDP, Peer: "1.2.3.4:9", BattleID: "b-1"}
			}
			reg.Register("p-a", c)
			reg.NegotiatePayloadVersion("p-a", tc.accepted)
			rng := rand.New(rand.NewPCG(5, 6))
			prev := randomFrame(rng, 1, false)
			for seq := uint64(1); seq <= 3; seq++ { // 第一帧必全量（无基线），第三帧才是判定点
				cur := nextFrame(rng, prev)
				cur.FrameId = seq
				if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, cur); err != nil {
					t.Fatalf("PublishFrames: %v", err)
				}
				prev = cur
			}
			last := port.raw[len(port.raw)-1]
			if last.version != tc.want {
				t.Fatalf("第 3 帧 ver = %d, 期望 %d", last.version, tc.want)
			}
		})
	}
}

// TestDeltaKeyframeTriggers 验证恢复三层里的服务端两层：新连接首帧必全量、
// 客户端请求后下一帧必全量、周期兜底、以及按连接 ≥200ms 的请求限流。
func TestDeltaKeyframeTriggers(t *testing.T) {
	reg, port, _ := newDeltaRegistry(t, DeltaConfig{KeyframeEvery: 2})
	reg.Register("p-a", kcpConn(7, "b-1"))
	reg.NegotiatePayloadVersion("p-a", []battlev1.FramePayloadVersion{3})
	rng := rand.New(rand.NewPCG(7, 8))
	prev := randomFrame(rng, 1, false)
	versions := make([]uint8, 0, 4)
	for seq := uint64(1); seq <= 4; seq++ {
		cur := nextFrame(rng, prev)
		cur.FrameId = seq
		if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, cur); err != nil {
			t.Fatalf("PublishFrames: %v", err)
		}
		versions = append(versions, port.raw[len(port.raw)-1].version)
		prev = cur
	}
	// 第 1 帧：无基线 ⇒ 全量(2)；第 2 帧：周期关键帧(seq%2==0) ⇒ 全量(2)；
	// 第 3 帧：链连续且非周期 ⇒ 增量(3)；第 4 帧：周期 ⇒ 全量(2)。
	want := []uint8{2, 2, 3, 2}
	for i := range want {
		if versions[i] != want[i] {
			t.Fatalf("第 %d 帧 ver = %d, 期望 %d（%v）", i+1, versions[i], want[i], versions)
		}
	}

	// 请求限流：200ms 窗口内第二次请求被丢弃。
	if !reg.RequestKeyframe("p-a", 4) {
		t.Fatal("首次关键帧请求应被接受")
	}
	if reg.RequestKeyframe("p-a", 4) {
		t.Fatal("200ms 窗口内的第二次请求应被限流丢弃")
	}
}

// TestDeltaAtMostTwoEncodingsPerTick 是「全局单链」的硬断言：同一 tick 内**至多两次编码**
// （增量一次 + 全量一次），绝不随目标数增长（严禁 per-client 差分）。
func TestDeltaAtMostTwoEncodingsPerTick(t *testing.T) {
	reg, _, _ := newDeltaRegistry(t, DeltaConfig{})
	calls := 0
	inner := reg.encode
	reg.encode = func(msg any) ([]byte, error) { calls++; return inner(msg) }
	reg.Register("p-a", kcpConn(7, "b-1"))
	reg.Register("p-b", kcpConn(8, "b-1"))
	reg.NegotiatePayloadVersion("p-a", []battlev1.FramePayloadVersion{3}) // 增量
	// p-b 未声明 3：永远全量 ⇒ 每 tick 增量 + 全量各一次，恰好 2 次。
	rng := rand.New(rand.NewPCG(9, 10))
	prev := randomFrame(rng, 1, false)
	for seq := uint64(1); seq <= 4; seq++ {
		cur := nextFrame(rng, prev)
		cur.FrameId = seq
		calls = 0
		if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a", "p-b"}, cur); err != nil {
			t.Fatalf("PublishFrames: %v", err)
		}
		if seq > 1 && calls > 2 {
			t.Fatalf("第 %d tick 编码 %d 次, 上限 2（增量一次 + 全量一次）", seq, calls)
		}
		prev = cur
	}
}

// TestDeltaStateSurvivesTransportReconnect ⑦：传输层重连（旧连接断开 + 同一玩家新连接登记）
// **不等于重新入局**——玩家会话仍持有 JoinBattle 协商出的 payloadVer，不得因换 socket 被降级：
// 重连后首帧因无基线必全量，其后仍必须回到增量（现场 v3=0 的根因即此处丢版本）。
func TestDeltaStateSurvivesTransportReconnect(t *testing.T) {
	reg, port, _ := newDeltaRegistry(t, DeltaConfig{KeyframeEvery: 1000})
	reg.Register("p-a", kcpConn(7, "b-1"))
	reg.NegotiatePayloadVersion("p-a", []battlev1.FramePayloadVersion{3})
	rng := rand.New(rand.NewPCG(21, 22))
	prev := randomFrame(rng, 1, false)
	publish := func(seq uint64) {
		cur := nextFrame(rng, prev)
		cur.FrameId = seq
		if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, cur); err != nil {
			t.Fatalf("PublishFrames(seq=%d): %v", seq, err)
		}
		prev = cur
	}
	publish(1) // 无基线 ⇒ 全量
	publish(2) // 增量
	// 传输层重连：帧引擎回调断开事件，随后同一玩家的新连接验票登记；客户端不再入局。
	reg.Disconnect(Endpoint{Kind: transport.KindKCP, ID: "7"})
	reg.Register("p-a", kcpConn(8, "b-1"))
	publish(3) // 无基线 ⇒ 必全量
	publish(4) // 基线恢复 + 版本仍在 ⇒ 必增量
	if got, want := port.raw[2].version, uint8(frame.Version2); got != want {
		t.Fatalf("重连后首帧 ver = %d, 期望 %d（无基线必全量）", got, want)
	}
	if got, want := port.raw[3].version, VersionDelta; got != want {
		t.Fatalf("重连后第二帧 ver = %d, 期望 %d（换 socket 不得丢掉会话协商出的 payloadVer）", got, want)
	}
}

// TestDeltaRenegotiationDowngrades ⑧：重新入局只声明 ver=2 时，生效版本必须**改回** ver=2——
// 协商结果每次都要写回（不是只在拿到 ver=3 时才写），否则旧连接的 ver=3 会一直沿用到新会话。
func TestDeltaRenegotiationDowngrades(t *testing.T) {
	reg, port, _ := newDeltaRegistry(t, DeltaConfig{KeyframeEvery: 1000})
	reg.Register("p-a", kcpConn(7, "b-1"))
	if got := reg.NegotiatePayloadVersion("p-a", []battlev1.FramePayloadVersion{3}); got != battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_DELTA {
		t.Fatalf("首次协商 = %v, 期望 DELTA", got)
	}
	rng := rand.New(rand.NewPCG(23, 24))
	prev := randomFrame(rng, 1, false)
	for seq := uint64(1); seq <= 2; seq++ {
		cur := nextFrame(rng, prev)
		cur.FrameId = seq
		if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, cur); err != nil {
			t.Fatalf("PublishFrames(seq=%d): %v", seq, err)
		}
		prev = cur
	}
	// 重新入局只声明 ver=2：生效版本必须降回 2。
	got := reg.NegotiatePayloadVersion("p-a", []battlev1.FramePayloadVersion{2})
	if got != battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_PROTOBUF {
		t.Fatalf("重新协商 = %v, 期望 PROTOBUF", got)
	}
	cur := nextFrame(rng, prev)
	cur.FrameId = 3
	if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, cur); err != nil {
		t.Fatalf("PublishFrames(seq=3): %v", err)
	}
	if v := port.raw[2].version; v != frame.Version2 {
		t.Fatalf("重新协商为 ver=2 后帧头 ver = %d, 期望 %d", v, frame.Version2)
	}
}
