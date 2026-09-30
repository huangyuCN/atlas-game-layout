package assemble

import (
	"context"
	"path/filepath"
	"testing"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/server"
)

// TestServiceConfigLoads 验证模板真实 config.yaml 能被 protojson 解组：
// proto 配置结构变更后，配置与代码不漂移（未知/缺失字段在此暴露）。
func TestServiceConfigLoads(t *testing.T) {
	var cfg conf.Bootstrap
	if err := config.Load(filepath.Join("..", "configs", "config.yaml"), &cfg); err != nil {
		t.Fatalf("加载 services/gateway/configs/config.yaml 失败: %v", err)
	}
	if cfg.GetRuntime().GetName() != "gateway" {
		t.Fatalf("runtime.name = %q, 期望 gateway", cfg.GetRuntime().GetName())
	}
	if cfg.GetSession().GetTtl() != "30s" {
		t.Fatalf("session.ttl = %q, 期望 30s", cfg.GetSession().GetTtl())
	}
	// R9 严格模式：真实配置必须显式带 runtime.namespace（样例值 test）——
	// 删掉该字段或用旧名 actor_namespace 时本用例即失败，错误信息含字段全名。
	if got := cfg.GetRuntime().GetNamespace(); got != "test" {
		t.Fatalf("runtime.namespace = %q, 期望 test（必须显式配置，缺失即启动失败）", got)
	}
	if err := bootstrap.RequireNamespace(cfg.GetRuntime().GetNamespace()); err != nil {
		t.Fatalf("runtime.namespace 未通过 R9 校验: %v", err)
	}
}

// TestServiceConfigEnforcesMinClientVersion 验证模板配置把版本门槛设为**登录期强制**：
// 门槛非空即强制（无需显式 mode）——低于门槛的客户端在登录入口被拒。防回退成「仅记录」：
// 空门槛会让老客户端「匹配成功但连不上」（阶段 3 战斗帧直连需新 SDK，规格 §7）。
func TestServiceConfigEnforcesMinClientVersion(t *testing.T) {
	var cfg conf.Bootstrap
	if err := config.Load(filepath.Join("..", "configs", "config.yaml"), &cfg); err != nil {
		t.Fatalf("加载 services/gateway/configs/config.yaml 失败: %v", err)
	}
	min := cfg.GetRuntime().GetMinClientVersion()
	if min == "" {
		t.Fatal("模板必须显式配置 runtime.min_client_version：空 = 仅记录不拒绝，老客户端会「匹配成功但连不上」")
	}
	gate := server.NewVersionGate(cfg.GetRuntime())
	if err := gate.Check(context.Background(), "0.0.1"); !errorv1.IsClientVersionTooLow(err) {
		t.Fatalf("门槛 %s 下 0.0.1 应被拒（登录期强制），实际 %v", min, err)
	}
	if err := gate.Check(context.Background(), min); err != nil {
		t.Fatalf("等于门槛应放行，实际 %v", err)
	}
}
