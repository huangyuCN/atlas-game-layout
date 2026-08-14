package gatewayv1

import (
	"testing"

	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TestSendFrameInputRoundtrip 校验帧输入上行消息与 lockstep 帧数据复用后的编解码往返。
func TestSendFrameInputRoundtrip(t *testing.T) {
	in := &SendFrameInputRequest{
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
	var out SendFrameInputRequest
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("protojson.Unmarshal 失败: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("往返不一致: in=%v out=%v", in, &out)
	}
}

// TestJoinBattleReplyCarriesSnapshot 校验加入战斗回执携带断线重连所需字段。
func TestJoinBattleReplyCarriesSnapshot(t *testing.T) {
	in := &JoinBattleReply{
		Ok:           true,
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

// TestSyncFramesReusesLockstep 校验补帧 RPC 直接复用 lockstep.SyncFrameRequest。
func TestSyncFramesReusesLockstep(t *testing.T) {
	req := &locksteppb.SyncFrameRequest{
		SessionId:   "b-0001",
		FromFrameId: 5,
		Limit:       10,
	}
	if req.GetSessionId() != "b-0001" || req.GetFromFrameId() != 5 || req.GetLimit() != 10 {
		t.Fatalf("SyncFrameRequest 字段不符: %v", req)
	}
}
