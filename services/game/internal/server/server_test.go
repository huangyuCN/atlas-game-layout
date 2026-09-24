package server

import (
	"testing"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas/transport"
)

// adminStub 是管理面服务的最小桩：只为注册点断言提供实现（方法不会被调用）。
type adminStub struct {
	admingamev1.UnimplementedAdminServiceServer
}

// faceOf 把 servers 组里的值还原为面服务端（测试断言服务清单用）。
func faceOf(t *testing.T, srv transport.Server) *serverutil.FaceServer {
	t.Helper()
	face, ok := srv.(*serverutil.FaceServer)
	if !ok {
		t.Fatalf("server 值 %T 不是 *serverutil.FaceServer", srv)
	}
	return face
}

// assertFaceMethods 断言某面上指定服务的已注册方法：present 必须全在，absent 必须全不在。
func assertFaceMethods(t *testing.T, face *serverutil.FaceServer, service string, present, absent []string) {
	t.Helper()
	info, ok := face.GetServiceInfo()[service]
	if !ok {
		t.Fatalf("%s 面缺少服务 %s", face.Face(), service)
	}
	got := make(map[string]bool, len(info.Methods))
	for _, m := range info.Methods {
		got[m.Name] = true
	}
	for _, name := range present {
		if !got[name] {
			t.Errorf("%s 面缺少方法 %s", face.Face(), name)
		}
	}
	for _, name := range absent {
		if got[name] {
			t.Errorf("%s 面出现了不应属于本面的方法 %s", face.Face(), name)
		}
	}
}

// TestNewGRPCServersSplitsFaces 验证 game 的域 rpc/ 平面按面注册：
// Edge 接口（access=CLIENT）只进 edge listener，Internal 接口（access=INTERNAL）只进 internal listener
// ——注册错面在编译期即失败（形参类型即本面接口），本测试断言运行时的服务清单同样隔离。
func TestNewGRPCServersSplitsFaces(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{Grpc: &configspb.Server_GRPC{
		EdgeAddr: "127.0.0.1:0", InternalAddr: "127.0.0.1:0",
	}}}
	set, err := NewGRPCServers(cfg, nil, nil, adminStub{})
	if err != nil {
		t.Fatalf("NewGRPCServers() 错误 = %v", err)
	}
	if set.Edge == nil || set.Internal == nil {
		t.Fatalf("两面都应启用，实际 edge=%v internal=%v", set.Edge, set.Internal)
	}
	const service = "game.v1.PlayerService"
	assertFaceMethods(t, faceOf(t, set.Edge), service,
		[]string{"Register", "GetPlayerData", "QueueParty"}, []string{"Login", "Logout", "GrantItem", "GetPlayer"})
	assertFaceMethods(t, faceOf(t, set.Internal), service,
		[]string{"Login", "Logout", "GrantItem", "GetPlayer"}, []string{"Register", "GetPlayerData", "QueueParty"})
}

// TestAdminServiceRegisteredOnInternalFaceOnly 验证管理面只注册在 internal 面（安全边界钉死）：
// edge 面（不可信区、客户端 op 经网关转发）零管理面服务，internal 面（可信区）四个方法齐备。
func TestAdminServiceRegisteredOnInternalFaceOnly(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{Grpc: &configspb.Server_GRPC{
		EdgeAddr: "127.0.0.1:0", InternalAddr: "127.0.0.1:0",
	}}}
	set, err := NewGRPCServers(cfg, nil, nil, adminStub{})
	if err != nil {
		t.Fatalf("NewGRPCServers() 错误 = %v", err)
	}
	const service = "admin.game.v1.AdminService"
	if _, ok := faceOf(t, set.Edge).GetServiceInfo()[service]; ok {
		t.Fatalf("edge 面不得注册管理面服务 %s（管理面是内网面，仅 internal listener 可达）", service)
	}
	assertFaceMethods(t, faceOf(t, set.Internal), service,
		[]string{"GrantItem", "QueryPlayer", "QueryBackpack", "QueryAudits"}, nil)
}

// TestAdminServiceAbsentWhenFaceDisabled 验证 internal 面未启用时不注册管理面
// （地址为空 = 该面不启用，不得回落到 edge 面）。
func TestAdminServiceAbsentWhenFaceDisabled(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{Grpc: &configspb.Server_GRPC{EdgeAddr: "127.0.0.1:0"}}}
	set, err := NewGRPCServers(cfg, nil, nil, adminStub{})
	if err != nil {
		t.Fatalf("NewGRPCServers() 错误 = %v", err)
	}
	if set.Internal != nil {
		t.Fatalf("internal_addr 为空时期望不启用 internal 面，实际 %v", set.Internal)
	}
	if _, ok := faceOf(t, set.Edge).GetServiceInfo()["admin.game.v1.AdminService"]; ok {
		t.Fatal("internal 面未启用时管理面不得回落到 edge 面")
	}
}

// TestNewGRPCServersDisablesFaceByEmptyAddr 验证地址为空 = 该面不启用（nil 接口，不进启停组）。
func TestNewGRPCServersDisablesFaceByEmptyAddr(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{Grpc: &configspb.Server_GRPC{InternalAddr: "127.0.0.1:0"}}}
	set, err := NewGRPCServers(cfg, nil, nil, adminStub{})
	if err != nil {
		t.Fatalf("NewGRPCServers() 错误 = %v", err)
	}
	if set.Edge != nil {
		t.Errorf("edge_addr 为空时期望不启用 edge 面，实际 %v", set.Edge)
	}
	if set.Internal == nil {
		t.Error("internal_addr 非空时期望启用 internal 面")
	}
}
