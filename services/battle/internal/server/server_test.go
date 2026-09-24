package server

import (
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas/transport"
)

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

// TestNewGRPCServersSplitsFaces 验证 battle 的域 rpc/ 平面按面注册：
// Edge 接口（access=CLIENT：加入对局/帧上行/帧同步）只进 edge listener，
// Internal 接口（access=INTERNAL：开局/查询）只进 internal listener。
func TestNewGRPCServersSplitsFaces(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{Grpc: &configspb.Server_GRPC{
		EdgeAddr: "127.0.0.1:0", InternalAddr: "127.0.0.1:0",
	}}}
	set, err := NewGRPCServers(cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewGRPCServers() 错误 = %v", err)
	}
	if set.Edge == nil || set.Internal == nil {
		t.Fatalf("两面都应启用，实际 edge=%v internal=%v", set.Edge, set.Internal)
	}
	const service = "battle.v1.BattleService"
	assertFaceMethods(t, faceOf(t, set.Edge), service,
		[]string{"JoinBattle", "SendFrameInput", "SyncFrames"}, []string{"Create", "GetState"})
	assertFaceMethods(t, faceOf(t, set.Internal), service,
		[]string{"Create", "GetState"}, []string{"JoinBattle", "SendFrameInput", "SyncFrames"})
}

// TestNewGRPCServersDisablesFaceByEmptyAddr 验证地址为空 = 该面不启用（nil 接口，不进启停组）。
func TestNewGRPCServersDisablesFaceByEmptyAddr(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{Grpc: &configspb.Server_GRPC{EdgeAddr: "127.0.0.1:0"}}}
	set, err := NewGRPCServers(cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewGRPCServers() 错误 = %v", err)
	}
	if set.Internal != nil {
		t.Errorf("internal_addr 为空时期望不启用 internal 面，实际 %v", set.Internal)
	}
	if set.Edge == nil {
		t.Error("edge_addr 非空时期望启用 edge 面")
	}
}
