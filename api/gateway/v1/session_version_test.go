package gatewayv1

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// assertStringField 断言字段号、类型与 JSON 名（缺失即失败）。
func assertStringField(t *testing.T, md protoreflect.MessageDescriptor, name string, num int32, jsonName string) {
	t.Helper()
	fd := md.Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		t.Fatalf("%s 缺少字段 %s", md.Name(), name)
	}
	if int32(fd.Number()) != num {
		t.Errorf("%s.%s 字段号 = %d, 期望 %d", md.Name(), name, fd.Number(), num)
	}
	if fd.Kind() != protoreflect.StringKind {
		t.Errorf("%s.%s 类型 = %v, 期望 string", md.Name(), name, fd.Kind())
	}
	if fd.JSONName() != jsonName {
		t.Errorf("%s.%s JSON 名 = %q, 期望 %q", md.Name(), name, fd.JSONName(), jsonName)
	}
}

// TestClientVersionField 验证 M1 客户端版本上报字段落在两条会话入口（登录与恢复）上，
// 且既有字段号未移动（会话协议是线上契约，改号即破坏兼容）。
func TestClientVersionField(t *testing.T) {
	login := (&LoginRequest{}).ProtoReflect().Descriptor()
	assertStringField(t, login, "client_version", 3, "clientVersion")
	assertStringField(t, login, "player_id", 1, "playerId")
	assertStringField(t, login, "password", 2, "password")

	resume := (&ResumeRequest{}).ProtoReflect().Descriptor()
	assertStringField(t, resume, "client_version", 3, "clientVersion")
	assertStringField(t, resume, "token", 1, "token")
	assertStringField(t, resume, "player_id", 2, "playerId")
}
