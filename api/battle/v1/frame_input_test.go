package battlev1

// frame_input_test.go 是战斗帧通道（客户端 op：帧输入上行 / 加入回执 / 补帧）的协议编解码用例。
// 这三条消息都是本包（battle.v1）契约，故用例随协议留在本包——原先放在 api/gateway/v1
// 只是因为会话通道复用它们，读起来像网关的消息，实际与网关协议无关。

import (
	"testing"

	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TestSendFrameInputRoundtrip 校验帧输入上行消息（battle.v1.FrameInputReq，
// 与 lockstep 帧数据复用）的编解码往返。
func TestSendFrameInputRoundtrip(t *testing.T) {
	in := &FrameInputReq{
		BattleId: "b-0001",
		Input: &locksteppb.LockstepInput{
			FrameId:   7,
			PlayerId:  "p-0001",
			Payload:   []byte{0x01, 0x02, 0x03},
			Predicted: true,
		},
	}
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatalf("protojson.Marshal 失败: %v", err)
	}
	var out FrameInputReq
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("protojson.Unmarshal 失败: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("往返不一致: in=%v out=%v", in, &out)
	}
}

// TestJoinBattleReplyCarriesSnapshot 校验加入战斗回执（FrameInputReq 的同伴消息）携带断线重连所需字段。
func TestJoinBattleReplyCarriesSnapshot(t *testing.T) {
	in := &JoinBattleReply{
		Meta:         &locksteppb.SessionMeta{SessionId: "b-0001", MaxPlayers: 2},
		CurrentFrame: 42,
		Snapshot:     &locksteppb.SnapshotMeta{FrameId: 40, StorageKey: "snap/b-0001/40"},
	}
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatalf("protojson.Marshal 失败: %v", err)
	}
	var out JoinBattleReply
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("protojson.Unmarshal 失败: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("往返不一致: in=%v out=%v", in, &out)
	}
}

// TestSyncFramesReusesLockstep 校验补帧请求（battle.v1.SyncFramesReq）携带客体字段与断点帧号。
func TestSyncFramesReusesLockstep(t *testing.T) {
	req := &SyncFramesReq{
		BattleId:      "b-0001",
		LastSeenFrame: 5,
	}
	if req.GetBattleId() != "b-0001" || req.GetLastSeenFrame() != 5 {
		t.Fatalf("SyncFramesReq 字段不符: %v", req)
	}
}
