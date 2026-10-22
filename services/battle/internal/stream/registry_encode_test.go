package stream

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// TestRegistryPublishFramesSerializesOnce 验证「一次序列化、多人复用」：一帧广播给 N 个目标，
// 真实推送上只发生**一次**序列化，且每个目标仍各写一帧（写是必需的 syscall）。
//
// 计数口径覆盖两条可能发生序列化的路径：帧引擎自身的编码（注入计数 codec）与注册表的预编码
// （注入同一个计数器）——两者之和就是这一帧的序列化次数。修复前逐目标各编码一次（= N）。
func TestRegistryPublishFramesSerializesOnce(t *testing.T) {
	const targets = 10
	codec := &countingCodec{}
	port, connIDs, adapter := newEnginePort(t, targets, codec)
	reg := NewRegistry()
	reg.BindPort(transport.KindKCP, port)
	reg.encode = codec.Marshal // 预编码与引擎编码共用同一份计数
	players := make([]string, 0, targets)
	for i := 0; i < targets; i++ {
		playerID := "p-" + strconv.Itoa(i)
		players = append(players, playerID)
		reg.Register(playerID, Conn{Kind: transport.KindKCP, ConnID: connIDs[i], BattleID: "b-1"})
	}

	if err := reg.PublishFrames(context.Background(), "b-1", players, pushTestFrame(7)); err != nil {
		t.Fatalf("PublishFrames: %v", err)
	}
	if got := codec.marshals.Load(); got != 1 {
		t.Fatalf("一次广播 %d 个目标发生 %d 次序列化, 期望 1（每帧只编码一次）", targets, got)
	}
	frames, bytes := adapter.written()
	if frames != targets {
		t.Fatalf("写出帧数 = %d, 期望 %d（每个目标各写一帧，写是必需的）", frames, targets)
	}
	if bytes == 0 {
		t.Fatal("写出字节数 = 0：帧体未真正下发")
	}
}

// TestRegistryPublishFramesReusesPayload 验证同一帧的全部目标复用**同一份载荷字节**：
// 编码只发生一次，各目标拿到的 payload 指向同一底层数组（不逐目标重新编码/复制）。
func TestRegistryPublishFramesReusesPayload(t *testing.T) {
	const targets = 10
	reg, kcpPort, _ := newTestRegistry()
	reg.encode = func(any) ([]byte, error) { return []byte("payload-for-one-frame"), nil }
	players := make([]string, 0, targets)
	for i := 0; i < targets; i++ {
		playerID := "p-" + strconv.Itoa(i)
		players = append(players, playerID)
		reg.Register(playerID, kcpConn(uint64(i+1), "b-1"))
	}

	if err := reg.PublishFrames(context.Background(), "b-1", players, pushTestFrame(7)); err != nil {
		t.Fatalf("PublishFrames: %v", err)
	}
	if len(kcpPort.raw) != targets {
		t.Fatalf("已编码载荷下发次数 = %d, 期望 %d", len(kcpPort.raw), targets)
	}
	if len(kcpPort.pushed) != 0 {
		t.Fatalf("仍走了逐目标编码路径（push 次数 = %d）", len(kcpPort.pushed))
	}
	first := kcpPort.raw[0]
	for _, rec := range kcpPort.raw {
		if rec.operation != battlev1opclient.BattleServicePushOps.FrameBroadcast {
			t.Fatalf("operation = %q, 期望帧广播", rec.operation)
		}
		if len(rec.payload) == 0 || &rec.payload[0] != &first.payload[0] {
			t.Fatalf("目标 %+v 的载荷不是同一份字节（未复用编码产物）", rec.conn)
		}
	}
	seen := make(map[Conn]struct{}, targets)
	for _, rec := range kcpPort.raw {
		seen[rec.conn] = struct{}{}
	}
	if len(seen) != targets {
		t.Fatalf("收到载荷的连接数 = %d, 期望 %d（每个目标各一帧）", len(seen), targets)
	}
}

// TestRegistryPublishFramesEmptyRosterSkipsEncode 验证空名单不编码：没有目标就不该为这一帧
// 付编码与分配（掉线判负会把名单清空，而对局仍要推进到帧数上限）。
func TestRegistryPublishFramesEmptyRosterSkipsEncode(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	encodes := 0
	reg.encode = func(any) ([]byte, error) { encodes++; return []byte("unused"), nil }

	if err := reg.PublishFrames(context.Background(), "b-1", nil, pushTestFrame(7)); err != nil {
		t.Fatalf("空名单广播不应报错: %v", err)
	}
	if encodes != 0 || len(kcpPort.raw) != 0 {
		t.Fatalf("空名单仍编码/下发: 编码 %d 次, 下发 %d 次", encodes, len(kcpPort.raw))
	}
}

// TestRegistryPublishFramesEncodeFailureSkipsPush 验证编码失败即不下发：
// 不把半成品或空载荷写给任何目标，并返回带原因的包装错误。
func TestRegistryPublishFramesEncodeFailureSkipsPush(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	boom := errors.New("编码炸了")
	reg.encode = func(any) ([]byte, error) { return nil, boom }
	reg.Register("p-a", kcpConn(7, "b-1"))

	err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, pushTestFrame(7))
	if !errors.Is(err, boom) {
		t.Fatalf("错误 = %v, 期望包装 %v", err, boom)
	}
	if len(kcpPort.raw) != 0 || len(kcpPort.pushed) != 0 {
		t.Fatalf("编码失败后仍发生下发: raw=%d push=%d", len(kcpPort.raw), len(kcpPort.pushed))
	}
}

// TestRegistryPublishFramesKeepsOtherTargetsOnFailure 验证单目标写失败不影响其余目标：
// 死链按失效注销并返回首个错误，其余目标照常收到同一帧（广播不得被一条死链打断）。
func TestRegistryPublishFramesKeepsOtherTargetsOnFailure(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	reg.Register("p-dead", kcpConn(7, "b-1"))
	reg.Register("p-live", kcpConn(8, "b-1"))
	kcpPort.failErr = engine.ErrConnNotFound
	kcpPort.failConn = 7

	err := reg.PublishFrames(context.Background(), "b-1", []string{"p-dead", "p-live"}, pushTestFrame(7))
	if !errors.Is(err, engine.ErrConnNotFound) {
		t.Fatalf("错误 = %v, 期望 ErrConnNotFound", err)
	}
	if reg.Online("p-dead") {
		t.Fatal("死链未按失效注销")
	}
	if !reg.Online("p-live") {
		t.Fatal("存活目标被误注销")
	}
	if len(kcpPort.raw) != 1 || kcpPort.raw[0].conn.ConnID != 8 {
		t.Fatalf("存活目标未收到帧: %+v", kcpPort.raw)
	}
}

// TestRegistryPublishFramesKeepsWireFormat 验证预编码不改变线格式：默认编码器的产物与
// 帧引擎 Push 用同一条路径（json codec → protojson），且仍是 JSON（字段名 battleId/frameId），
// 故下发由 Push 换成 PushRaw 后客户端看到的字节不变。
func TestRegistryPublishFramesKeepsWireFormat(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	reg.Register("p-a", kcpConn(7, "b-1"))
	frame := pushTestFrame(7)
	if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, frame); err != nil {
		t.Fatalf("PublishFrames: %v", err)
	}
	if len(kcpPort.raw) != 1 {
		t.Fatalf("载荷下发次数 = %d, 期望 1", len(kcpPort.raw))
	}
	got := kcpPort.raw[0].payload
	want, err := encodeJSON(&battlev1.FrameBroadcast{BattleId: "b-1", Frame: frame})
	if err != nil {
		t.Fatalf("对照编码失败: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("预编码载荷与引擎编码不一致:\n got=%s\nwant=%s", got, want)
	}
	for _, field := range []string{`"battleId"`, `"frameId"`} {
		if !bytes.Contains(got, []byte(field)) {
			t.Fatalf("载荷缺少 protojson 字段 %s（线格式被改）: %s", field, got)
		}
	}
}

// 静态断言：fakePort 必须满足帧面端口契约（含已编码载荷下发）。
var _ Port = (*fakePort)(nil)
