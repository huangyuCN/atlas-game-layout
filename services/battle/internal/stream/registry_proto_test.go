package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas/encoding"
	_ "github.com/huangyuCN/atlas/encoding/proto"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"google.golang.org/protobuf/proto"
)

// protoCodec 返回 protobuf 二进制编解码器（ver=2 的编码实现）。
func protoCodec(t *testing.T) encoding.Codec {
	t.Helper()
	codec := encoding.GetCodec("proto")
	if codec == nil {
		t.Fatal("proto codec 未注册（缺少 encoding/proto 导入）")
	}
	return codec
}

// TestBroadcastProtobufSemanticParity 验证「同一条广播，protobuf 与 protojson 反序列化后
// 语义完全相同」（字段逐一相等），且 protobuf 编码后字节数 ≤ protojson 的 50%
// （实测约 3.5×：1463 B 级别的载荷降到 ~465 B，是本次切换的目标）。
func TestBroadcastProtobufSemanticParity(t *testing.T) {
	msg := &battlev1.FrameBroadcast{BattleId: "b-1", Frame: pushTestFrame(42)}

	jsonBytes, err := encodeJSON(msg)
	if err != nil {
		t.Fatalf("protojson 编码失败: %v", err)
	}
	protoBytes, err := EncodeProtobuf(msg)
	if err != nil {
		t.Fatalf("protobuf 编码失败: %v", err)
	}

	var fromJSON, fromProto battlev1.FrameBroadcast
	if err := decodePayload(jsonBytes, &fromJSON); err != nil {
		t.Fatalf("protojson 解码失败: %v", err)
	}
	if err := protoCodec(t).Unmarshal(protoBytes, &fromProto); err != nil {
		t.Fatalf("protobuf 解码失败: %v", err)
	}

	// 逐字段相等（proto.Equal 覆盖全部字段与嵌套消息；再显式点名关键字段便于定位回归）。
	if !proto.Equal(&fromJSON, &fromProto) {
		t.Fatalf("两种编码解码后语义不同:\n protojson=%v\n protobuf=%v", &fromJSON, &fromProto)
	}
	if fromProto.GetBattleId() != msg.GetBattleId() {
		t.Fatalf("battle_id = %q, 期望 %q", fromProto.GetBattleId(), msg.GetBattleId())
	}
	if fromProto.GetFrame().GetFrameId() != msg.GetFrame().GetFrameId() {
		t.Fatalf("frame_id = %d, 期望 %d", fromProto.GetFrame().GetFrameId(), msg.GetFrame().GetFrameId())
	}
	if len(fromProto.GetFrame().GetInputs()) != len(msg.GetFrame().GetInputs()) {
		t.Fatalf("inputs 条数 = %d, 期望 %d",
			len(fromProto.GetFrame().GetInputs()), len(msg.GetFrame().GetInputs()))
	}

	if len(protoBytes)*2 > len(jsonBytes) {
		t.Fatalf("protobuf %d B 超过 protojson %d B 的 50%%（目标 ≤ 50%%，约 3×）",
			len(protoBytes), len(jsonBytes))
	}
	t.Logf("同一广播：protojson %d B → protobuf %d B（%.2f 倍）",
		len(jsonBytes), len(protoBytes), float64(len(jsonBytes))/float64(len(protoBytes)))
}

// TestRegistryDefaultPushEncodingIsJSON 验证缺省编码零变化：帧广播仍按 protojson（ver=1）下发。
func TestRegistryDefaultPushEncodingIsJSON(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	reg.Register("p-a", kcpConn(7, "b-1"))
	if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, pushTestFrame(42)); err != nil {
		t.Fatalf("PublishFrames: %v", err)
	}
	if len(kcpPort.raw) != 1 {
		t.Fatalf("推送次数 = %d, 期望 1", len(kcpPort.raw))
	}
	got := kcpPort.raw[0]
	if got.version != frame.Version {
		t.Fatalf("缺省推送 ver = %d, 期望 %d（缺省仍是 protojson）", got.version, frame.Version)
	}
	var bc battlev1.FrameBroadcast
	if err := decodePayload(got.payload, &bc); err != nil {
		t.Fatalf("缺省载荷应为 protojson，解码失败: %v", err)
	}
}

// TestRegistryProtobufPushEncoding 验证开关打开后帧广播走 protobuf：
// 载荷是 protobuf 二进制（按 proto 解回与源消息相等），帧头 ver=2 与载荷编码自洽，
// 且载荷体积不超过 protojson 的 50%。
func TestRegistryProtobufPushEncoding(t *testing.T) {
	reg, err := NewRegistryWithEncoding(DefaultEndedTTL, EncodeProtobuf, frame.Version2)
	if err != nil {
		t.Fatalf("NewRegistryWithEncoding: %v", err)
	}
	kcpPort := &fakePort{}
	reg.BindPort(transport.KindKCP, kcpPort)
	reg.Register("p-a", kcpConn(7, "b-1"))

	sent := pushTestFrame(42)
	if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, sent); err != nil {
		t.Fatalf("PublishFrames: %v", err)
	}
	if len(kcpPort.raw) != 1 {
		t.Fatalf("推送次数 = %d, 期望 1", len(kcpPort.raw))
	}
	got := kcpPort.raw[0]
	if got.version != frame.Version2 {
		t.Fatalf("推送 ver = %d, 期望 %d（帧头版本必须与载荷编码一致）", got.version, frame.Version2)
	}
	var bc battlev1.FrameBroadcast
	if err := protoCodec(t).Unmarshal(got.payload, &bc); err != nil {
		t.Fatalf("载荷应为 protobuf 二进制，解码失败: %v", err)
	}
	want := &battlev1.FrameBroadcast{BattleId: "b-1", Frame: sent}
	if !proto.Equal(&bc, want) {
		t.Fatalf("解码后的广播与源消息不等: %+v", &bc)
	}

	jsonBytes, err := encodeJSON(want)
	if err != nil {
		t.Fatalf("protojson 编码失败: %v", err)
	}
	if len(got.payload)*2 > len(jsonBytes) {
		t.Fatalf("protobuf 载荷 %d B 超过 protojson %d B 的 50%%", len(got.payload), len(jsonBytes))
	}
}

// TestNewRegistryWithEncodingValidation 验证开关的装配期校验：编码器为 nil 或版本不在
// 已知集合（ver=1/ver=2）即报错——编码器与帧头版本不自洽会让客户端按键字解错。
func TestNewRegistryWithEncodingValidation(t *testing.T) {
	if _, err := NewRegistryWithEncoding(time.Minute, nil, frame.Version2); err == nil {
		t.Fatal("nil 编码器应报错")
	}
	if _, err := NewRegistryWithEncoding(time.Minute, EncodeProtobuf, 7); err == nil {
		t.Fatal("未知版本应报错")
	}
	reg, err := NewRegistryWithEncoding(time.Minute, EncodeProtobuf, frame.Version2)
	if err != nil {
		t.Fatalf("合法参数应成功: %v", err)
	}
	if reg == nil {
		t.Fatal("合法参数应返回注册表")
	}
}

// BenchmarkPublishFrameBroadcast10Protobuf 是 BenchmarkPublishFrameBroadcast10 的
// protobuf 对照：同一条帧广播、同样 10 个目标，只换载荷编码（ver=2）。
// 用于核对「每包分配不变、字节数下降」——编码与下发路径与缺省路径同构。
func BenchmarkPublishFrameBroadcast10Protobuf(b *testing.B) {
	port, connIDs, _ := newEnginePort(b, benchPlayers, nil)
	reg, err := NewRegistryProtobuf(DefaultEndedTTL)
	if err != nil {
		b.Fatalf("NewRegistryProtobuf: %v", err)
	}
	reg.BindPort(transport.KindKCP, port)
	battleID := "b-bench"
	players := make([]string, 0, benchPlayers)
	for i := 0; i < benchPlayers; i++ {
		playerID := fmt.Sprintf("p-%d", i)
		players = append(players, playerID)
		reg.Register(playerID, Conn{Kind: transport.KindKCP, ConnID: connIDs[i], BattleID: battleID})
	}
	frame := pushTestFrame(42)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := reg.PublishFrames(ctx, battleID, players, frame); err != nil {
			b.Fatalf("PublishFrames: %v", err)
		}
	}
}

// TestRegistryProtobufBroadcastOnWire 是端到端 wire 断言：走真实帧引擎（ConnPort → StreamEngine →
// 适配器写出），确认打开开关后线上那一帧的**帧头 ver=2**、载荷是 protobuf 二进制，
// 且整帧体积仍显著小于 protojson 路径。
func TestRegistryProtobufBroadcastOnWire(t *testing.T) {
	port, connIDs, adapter := newEnginePort(t, 1, nil)
	adapter.captureWrites()

	reg, err := NewRegistryProtobuf(DefaultEndedTTL)
	if err != nil {
		t.Fatalf("NewRegistryProtobuf: %v", err)
	}
	reg.BindPort(transport.KindKCP, port)
	reg.Register("p-a", Conn{Kind: transport.KindKCP, ConnID: connIDs[0], BattleID: "b-1"})

	sent := pushTestFrame(42)
	if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, sent); err != nil {
		t.Fatalf("PublishFrames: %v", err)
	}
	written := adapter.captured()
	if len(written) != 1 {
		t.Fatalf("写帧次数 = %d, 期望 1（每连接每 tick 一次写，包数不变）", len(written))
	}
	if written[0].version != frame.Version2 {
		t.Fatalf("线上帧 ver = %d, 期望 %d", written[0].version, frame.Version2)
	}

	op := battlev1opclient.BattleServicePushOps.FrameBroadcast
	prefix := make([]byte, 2, 2+len(op))
	binary.BigEndian.PutUint16(prefix, uint16(len(op)))
	prefix = append(prefix, op...)
	if !bytes.HasPrefix(written[0].body, prefix) {
		t.Fatalf("帧体前缀不是 [opLen][op]: %x", written[0].body[:min(len(written[0].body), 16)])
	}
	payload := written[0].body[len(prefix):]
	var got battlev1.FrameBroadcast
	if err := protoCodec(t).Unmarshal(payload, &got); err != nil {
		t.Fatalf("线上载荷应为 protobuf 二进制: %v", err)
	}
	if !proto.Equal(&got, &battlev1.FrameBroadcast{BattleId: "b-1", Frame: sent}) {
		t.Fatalf("线上载荷与源广播不等: %+v", &got)
	}

	jsonPayload, err := encodeJSON(&battlev1.FrameBroadcast{BattleId: "b-1", Frame: sent})
	if err != nil {
		t.Fatalf("protojson 编码失败: %v", err)
	}
	if len(payload)*2 > len(jsonPayload) {
		t.Fatalf("线上载荷 %d B 超过 protojson %d B 的 50%%", len(payload), len(jsonPayload))
	}
}
