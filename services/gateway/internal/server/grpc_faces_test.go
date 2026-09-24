package server

import (
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
)

// TestNewGRPCServersEdgeOnly 验证 gateway 本轮只启用 edge 面且不注册域服务：
// internal 面留空 = 不启用（P7 管理面再启用）；edge 面除框架内置的 health 外无业务服务。
func TestNewGRPCServersEdgeOnly(t *testing.T) {
	cfg := &conf.Bootstrap{Server: &configspb.Server{Grpc: &configspb.Server_GRPC{EdgeAddr: "127.0.0.1:0"}}}
	set, err := NewGRPCServers(cfg, nil)
	if err != nil {
		t.Fatalf("NewGRPCServers() 错误 = %v", err)
	}
	if set.Internal != nil {
		t.Errorf("internal_addr 留空时期望不启用 internal 面，实际 %v", set.Internal)
	}
	if set.Edge == nil {
		t.Fatal("edge_addr 非空时期望启用 edge 面")
	}
	edge, ok := set.Edge.(*serverutil.FaceServer)
	if !ok {
		t.Fatalf("edge 值 %T 不是 *serverutil.FaceServer", set.Edge)
	}
	for name := range edge.GetServiceInfo() {
		if name != "grpc.health.v1.Health" {
			t.Errorf("edge 面本轮不应注册域服务，实际出现 %s", name)
		}
	}
}
