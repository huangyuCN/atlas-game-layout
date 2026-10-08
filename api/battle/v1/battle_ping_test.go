package battlev1

// battle_ping_test.go 是直连保活探针（battle.v1.BattleService/Ping）的协议契约用例。
// 保活之所以要钉死协议：客户端在无输入期间周期发它维持帧面活跃（数据报面 idle 判掉线、
// NAT 映射保活），一旦路由契约被改动（变成 INTERNAL、变成 Ask 要回执、不再按 battle_id
// 寻址），客户端会静默发不出去或收不到回执，而现象是「连接还在但被判掉线」，极难定位。

import (
	"testing"

	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TestPingReqRoundtrip 校验保活探针请求（battle.v1.PingReq，仅客体寻址字段 battle_id）
// 的编解码往返。
func TestPingReqRoundtrip(t *testing.T) {
	in := &PingReq{BattleId: "b-0001"}
	b, err := protojson.Marshal(in)
	if err != nil {
		t.Fatalf("protojson.Marshal 失败: %v", err)
	}
	var out PingReq
	if err := protojson.Unmarshal(b, &out); err != nil {
		t.Fatalf("protojson.Unmarshal 失败: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("往返不一致: in=%v out=%v", in, &out)
	}
}

// TestPingRouteContract 校验 Ping 的路由契约：access=CLIENT（客户端可达）、returns Empty
// 即 Tell（无回执）、按 battle_id 客体寻址（沿用 service 级 uid_field，不加 rpc_route 覆盖）。
func TestPingRouteContract(t *testing.T) {
	const op = "/battle.v1.BattleService/Ping"
	entry, ok := BattleServiceRouteTable[op]
	if !ok {
		t.Fatalf("路由表缺少保活 op %s（客户端无保活探针可用）", op)
	}
	if entry.Access != relay.AccessClient {
		t.Fatalf("%s 的 access = %v，期望 ACCESS_CLIENT（直连客户端必须可达）", op, entry.Access)
	}
	if !entry.IsTell || entry.NewReply != nil {
		t.Fatalf("%s 必须是 Tell（returns Empty，无回执）: IsTell=%v hasReply=%v", op, entry.IsTell, entry.NewReply != nil)
	}
	if entry.Actor != "battle" || entry.UidSource != relay.UidField {
		t.Fatalf("%s 寻址不符: actor=%q uid_source=%v", op, entry.Actor, entry.UidSource)
	}
	uid, ok := entry.UidOf(&PingReq{BattleId: "b-0001"})
	if !ok || uid != "b-0001" {
		t.Fatalf("%s 未按 battle_id 取路由 UID: uid=%q ok=%v", op, uid, ok)
	}
	if uid, ok := entry.UidOf(&PingReq{}); ok {
		t.Fatalf("%s 空 battle_id 竟然取到 UID %q（应拒绝寻址）", op, uid)
	}
}
