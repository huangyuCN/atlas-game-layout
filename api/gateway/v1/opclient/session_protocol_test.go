package gatewayv1opclient

import (
	"testing"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
)

// sessionOpWants 是会话 op 的权威取值（与网关 metrics.go 的手抄常量同源，
// P3 会把那处改为引用本描述符，从而消掉最后一份手抄）。
var sessionOpWants = map[string]string{
	"Register":  "/gateway.v1.Session/Register",
	"Login":     "/gateway.v1.Session/Login",
	"Resume":    "/gateway.v1.Session/Resume",
	"Logout":    "/gateway.v1.Session/Logout",
	"Heartbeat": "/gateway.v1.Session/Heartbeat",
}

// TestSessionProtocolOps 验证描述符的 5 个 op 名与权威取值逐条一致。
func TestSessionProtocolOps(t *testing.T) {
	got := map[string]string{
		"Register":  SessionProtocolOps.Register,
		"Login":     SessionProtocolOps.Login,
		"Resume":    SessionProtocolOps.Resume,
		"Logout":    SessionProtocolOps.Logout,
		"Heartbeat": SessionProtocolOps.Heartbeat,
	}
	for name, want := range sessionOpWants {
		if got[name] != want {
			t.Errorf("op[%s] = %q, 期望 %q", name, got[name], want)
		}
	}
}

// TestSessionTokenExtractor 验证凭据提取：命中 LoginReply/ResumeRequest，其余返回空串。
func TestSessionTokenExtractor(t *testing.T) {
	if v := SessionToken(&gatewayv1.LoginReply{Token: "t-1"}); v != "t-1" {
		t.Errorf("Token(LoginReply) = %q, 期望 t-1", v)
	}
	if v := SessionToken(&gatewayv1.ResumeRequest{Token: "t-2"}); v != "t-2" {
		t.Errorf("Token(ResumeRequest) = %q, 期望 t-2", v)
	}
	if v := SessionToken(&gatewayv1.HeartbeatReply{}); v != "" {
		t.Errorf("Token(HeartbeatReply) = %q, 期望空串（无该字段）", v)
	}
}

// TestSessionPlayerIDExtractor 验证玩家身份提取：会话建立/恢复各消息都能取到。
func TestSessionPlayerIDExtractor(t *testing.T) {
	cases := []struct {
		name string
		msg  any
		want string
	}{
		{"RegisterReply", &gatewayv1.RegisterReply{PlayerId: "p-1"}, "p-1"},
		{"LoginReply", &gatewayv1.LoginReply{PlayerId: "p-2"}, "p-2"},
		{"ResumeRequest", &gatewayv1.ResumeRequest{PlayerId: "p-3"}, "p-3"},
		{"ResumeReply", &gatewayv1.ResumeReply{PlayerId: "p-4"}, "p-4"},
		{"HeartbeatReply", &gatewayv1.HeartbeatReply{}, ""},
	}
	for _, c := range cases {
		if got := SessionPlayerID(c.msg); got != c.want {
			t.Errorf("PlayerID(%s) = %q, 期望 %q", c.name, got, c.want)
		}
	}
}

// TestSessionExpiresAtExtractor 验证过期时间提取器是**可选钩子**：
// 模板当前无 expiry 字段 ⇒ 恒返回 0（不透明 TTL，本轮不启用续期；R13）。
func TestSessionExpiresAtExtractor(t *testing.T) {
	for _, msg := range []any{&gatewayv1.LoginReply{}, &gatewayv1.ResumeReply{}, &gatewayv1.HeartbeatReply{}} {
		if got := SessionExpiresAt(msg); got != 0 {
			t.Errorf("ExpiresAt(%T) = %d, 期望 0（无过期字段）", msg, got)
		}
	}
}
