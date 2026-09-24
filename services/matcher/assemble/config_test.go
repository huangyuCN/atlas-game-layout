package assemble

import (
	"path/filepath"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
)

// TestServiceConfigLoads 验证模板真实 config.yaml 能被 protojson 解组：
// proto 配置结构变更后，配置与代码不漂移（未知/缺失字段在此暴露）。
func TestServiceConfigLoads(t *testing.T) {
	var cfg conf.Bootstrap
	if err := config.Load(filepath.Join("..", "configs", "config.yaml"), &cfg); err != nil {
		t.Fatalf("加载 services/matcher/configs/config.yaml 失败: %v", err)
	}
	if cfg.GetRuntime().GetName() != "matcher" {
		t.Fatalf("runtime.name = %q, 期望 matcher", cfg.GetRuntime().GetName())
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
