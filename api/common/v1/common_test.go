package commonv1

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TestPlayerSummaryRoundtrip 校验公共消息跨服务传输的 JSON 编解码往返一致。
func TestPlayerSummaryRoundtrip(t *testing.T) {
	in := &PlayerSummary{PlayerId: "p-0001", Nickname: "小明", Level: 3}
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatalf("protojson.Marshal 失败: %v", err)
	}
	var out PlayerSummary
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("protojson.Unmarshal 失败: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("往返不一致: in=%v out=%v", in, &out)
	}
}

// TestPlayerSummaryJSONKeys 校验 JSON 键名（驼峰）与空字段省略行为。
func TestPlayerSummaryJSONKeys(t *testing.T) {
	b, err := protojson.Marshal(&PlayerSummary{PlayerId: "p-1", Level: 2})
	if err != nil {
		t.Fatalf("protojson.Marshal 失败: %v", err)
	}
	s := string(b)
	for _, key := range []string{"playerId", "level"} {
		if !strings.Contains(s, key) {
			t.Fatalf("JSON 缺少键 %q: %s", key, s)
		}
	}
	if strings.Contains(s, "nickname") {
		t.Fatalf("空字段不应出现在 JSON 中: %s", s)
	}
}
