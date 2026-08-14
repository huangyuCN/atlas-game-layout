package battlev1

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TestBattleSettledEventRoundtrip 校验战斗结算事件（nats 总线）的编解码往返。
func TestBattleSettledEventRoundtrip(t *testing.T) {
	in := &BattleSettledEvent{
		BattleId: "b-0001",
		MatchId:  "m-0001",
		Players: []*PlayerResult{
			{PlayerId: "p-0001", Win: true, Score: 100},
			{PlayerId: "p-0002", Win: false, Score: 60},
		},
	}
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatalf("protojson.Marshal 失败: %v", err)
	}
	var out BattleSettledEvent
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("protojson.Unmarshal 失败: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("往返不一致: in=%v out=%v", in, &out)
	}
}

// TestCreateBattleRoundtrip 校验开局请求的编解码往返。
func TestCreateBattleRoundtrip(t *testing.T) {
	in := &CreateBattleRequest{
		MatchId:   "m-0001",
		PlayerIds: []string{"p-0001", "p-0002"},
	}
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatalf("protojson.Marshal 失败: %v", err)
	}
	var out CreateBattleRequest
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("protojson.Unmarshal 失败: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("往返不一致: in=%v out=%v", in, &out)
	}
}
