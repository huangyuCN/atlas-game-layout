package gatewayv1

import (
	"testing"

	atlasroutepb "github.com/huangyuCN/atlas/api/atlas/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// sessionService 返回会话服务描述符（缺失即失败）。
func sessionService(t *testing.T) protoreflect.ServiceDescriptor {
	t.Helper()
	svc := File_api_gateway_v1_session_proto.Services().ByName("Session")
	if svc == nil {
		t.Fatal("未找到 Session 服务描述符")
	}
	return svc
}

// TestSessionRouteAnnotation 验证会话服务带 atlas.route.v1 注解：
// access=CLIENT（客户端 op 生成）且 actor 留空（网关自留本地实现，不入透传表）。
func TestSessionRouteAnnotation(t *testing.T) {
	svc := sessionService(t)
	v := proto.GetExtension(svc.Options(), atlasroutepb.E_ServiceRoute)
	rule, ok := v.(*atlasroutepb.RouteRule)
	if !ok || rule == nil {
		t.Fatal("Session 缺少 atlas.route.v1.service_route 注解")
	}
	if rule.GetAccess() != atlasroutepb.Access_ACCESS_CLIENT {
		t.Errorf("access = %v, 期望 ACCESS_CLIENT", rule.GetAccess())
	}
	if rule.GetActor() != "" {
		t.Errorf("actor = %q, 期望留空（网关自留本地服务）", rule.GetActor())
	}
}

// TestSessionOpsUnchanged 验证 5 个 op 串零变更（会话协议是既有线上契约，不得随注解调整漂移）。
func TestSessionOpsUnchanged(t *testing.T) {
	svc := sessionService(t)
	want := map[string]bool{
		"/gateway.v1.Session/Register":  true,
		"/gateway.v1.Session/Login":     true,
		"/gateway.v1.Session/Resume":    true,
		"/gateway.v1.Session/Logout":    true,
		"/gateway.v1.Session/Heartbeat": true,
	}
	got := make(map[string]bool, svc.Methods().Len())
	for i := 0; i < svc.Methods().Len(); i++ {
		m := svc.Methods().Get(i)
		got["/"+string(svc.FullName())+"/"+string(m.Name())] = true
	}
	if len(got) != len(want) {
		t.Fatalf("op 数量 = %d, 期望 %d（%v）", len(got), len(want), got)
	}
	for op := range want {
		if !got[op] {
			t.Errorf("缺少 op %s", op)
		}
	}
}
