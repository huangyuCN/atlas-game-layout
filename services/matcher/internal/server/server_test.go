package server

import (
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
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

// TestNewGRPCServersRegistersMatcherOnInternal 验证 Matcher 服务只注册在 internal 面：
// 它属**服务间**调用（game 的 PlayerActor → matcher），必须落在可信面；
// edge 面不得出现 Matcher 方法（matcher 无客户端 op；服务间调用一律走可信的 internal 面）。
func TestNewGRPCServersRegistersMatcherOnInternal(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{Grpc: &configspb.Server_GRPC{
		EdgeAddr: "127.0.0.1:0", InternalAddr: "127.0.0.1:0",
	}}}
	set, err := NewGRPCServers(cfg, handler.NewMatcherHandler(handler.MatcherDeps{}), nil)
	if err != nil {
		t.Fatalf("NewGRPCServers() 错误 = %v", err)
	}
	if set.Edge == nil || set.Internal == nil {
		t.Fatalf("两面都应启用，实际 edge=%v internal=%v", set.Edge, set.Internal)
	}
	const service = "matcher.v1.Matcher"
	if _, ok := faceOf(t, set.Internal).GetServiceInfo()[service]; !ok {
		t.Fatalf("internal 面缺少 %s（服务间调用会打到 Unimplemented）", service)
	}
	if _, ok := faceOf(t, set.Edge).GetServiceInfo()[service]; ok {
		t.Fatalf("edge 面出现了 %s：服务间服务泄漏到不可信区", service)
	}
}

// TestNewGRPCServersDisablesFaceByEmptyAddr 验证地址为空 = 该面不启用（nil 接口，不进启停组）。
func TestNewGRPCServersDisablesFaceByEmptyAddr(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{Grpc: &configspb.Server_GRPC{InternalAddr: "127.0.0.1:0"}}}
	set, err := NewGRPCServers(cfg, handler.NewMatcherHandler(handler.MatcherDeps{}), nil)
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
