package assemble

import (
	"path/filepath"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
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
}
