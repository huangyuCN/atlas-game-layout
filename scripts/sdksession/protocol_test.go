package sdksession

// protocol_test.go 钉死接缝「被挤下线」解码的版本分派（SDK 接缝 S0.5 修订 1）：
// 同一原因在 ver=1（protojson）与 ver=2（protobuf wire）下都要解出，未知帧头版本
// 不 panic、不猜编码；非本会话推送一律 ok=false。

import (
	"testing"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-game-layout/api/gateway/v1/opclient"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	sdkframe "github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// kickedOp 是生成物声明的被挤下线推送 op（接缝按它识别，测试不手写字面量）。
var kickedOp = gatewayv1opclient.SessionPushOps.KickedNotify

// wantKickedReason 是推送载荷里的原因枚举名（接缝对外返回枚举名，不返回数字）。
const wantKickedReason = "KICKED_REASON_LOGGED_IN_ELSEWHERE"

// kickedBodies 生成同一原因的两种编码载荷：ver=1 的 protojson 字节与 ver=2 的 wire 字节。
func kickedBodies(t *testing.T) (jsonBody, wireBody []byte) {
	t.Helper()
	notify := &gatewayv1.KickedNotify{Reason: gatewayv1.KickedReason_KICKED_REASON_LOGGED_IN_ELSEWHERE}
	jsonBody, err := protojson.Marshal(notify)
	if err != nil {
		t.Fatalf("protojson 编码被挤下线通知: %v", err)
	}
	wireBody, err = proto.Marshal(notify)
	if err != nil {
		t.Fatalf("protobuf 编码被挤下线通知: %v", err)
	}
	return jsonBody, wireBody
}

// TestProtocolKickedDecodesByFrameVersion 验证接缝按推送信封的帧头版本选解码器：
// ver=1 走 protojson、ver=2 走 protobuf，两者都解出同一原因；未知版本/坏载荷/非信封
// 载荷一律 ok=true + 空 reason（不 panic、不误判），非本会话推送返回 ok=false。
func TestProtocolKickedDecodesByFrameVersion(t *testing.T) {
	jsonBody, wireBody := kickedBodies(t)
	cases := []struct {
		name       string
		op         string
		msg        any
		wantOK     bool
		wantReason string
	}{
		{"ver1-protojson", kickedOp, sdkclient.PushEnvelope{Op: kickedOp, Version: sdkframe.Version, Body: jsonBody}, true, wantKickedReason},
		{"ver2-protobuf", kickedOp, sdkclient.PushEnvelope{Op: kickedOp, Version: sdkframe.Version2, Body: wireBody}, true, wantKickedReason},
		{"unknown-version", kickedOp, sdkclient.PushEnvelope{Op: kickedOp, Version: 9, Body: jsonBody}, true, ""},
		{"empty-body", kickedOp, sdkclient.PushEnvelope{Op: kickedOp, Version: sdkframe.Version}, true, ""},
		{"bad-body", kickedOp, sdkclient.PushEnvelope{Op: kickedOp, Version: sdkframe.Version, Body: []byte("{")}, true, ""},
		{"raw-bytes-not-envelope", kickedOp, jsonBody, true, ""},
		{"other-op", gatewayv1opclient.SessionProtocolOps.Login, sdkclient.PushEnvelope{Op: kickedOp, Version: sdkframe.Version, Body: jsonBody}, false, ""},
	}
	protoSeam := Protocol()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, ok := protoSeam.Kicked(tc.op, tc.msg)
			if ok != tc.wantOK || reason != tc.wantReason {
				t.Fatalf("Kicked() = (%q, %v)，期望 (%q, %v)", reason, ok, tc.wantReason, tc.wantOK)
			}
		})
	}
}
