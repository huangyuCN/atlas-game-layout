package matcherv1

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TestQueryMatchRoundtrip 校验匹配状态查询消息的编解码往返。
func TestQueryMatchRoundtrip(t *testing.T) {
	in := &QueryMatchReply{State: "matched", MatchId: "m-0001"}
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatalf("protojson.Marshal 失败: %v", err)
	}
	var out QueryMatchReply
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("protojson.Unmarshal 失败: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("往返不一致: in=%v out=%v", in, &out)
	}
}

// TestMatchStartedEventRoundtrip 校验成局事件（nats 总线）的编解码往返。
func TestMatchStartedEventRoundtrip(t *testing.T) {
	in := &MatchStartedEvent{
		MatchId:        "m-0001",
		BattleId:       "b-0001",
		PlayerIds:      []string{"p-0001", "p-0002"},
		BattleEndpoint: "10.10.9.36:19090",
	}
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatalf("protojson.Marshal 失败: %v", err)
	}
	var out MatchStartedEvent
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("protojson.Unmarshal 失败: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("往返不一致: in=%v out=%v", in, &out)
	}
}
