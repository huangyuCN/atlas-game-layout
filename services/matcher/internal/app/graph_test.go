package app

import (
	"testing"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas/transport"
	"go.uber.org/fx"
)

// TestGraphStaticValidation 对 matcher 依赖图做静态校验：
// 清单中无重复提供的类型、Invoke 依赖闭包完整、泛型提供器类型解析正确
// （已知局限：部分 Provider 可能沿 Invoke 链被实例化但构造错误会被忽略，
// 组件级行为由 services/matcher/assemble 与 test/e2e 兜底）。
func TestGraphStaticValidation(t *testing.T) {
	cfg := &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "matcher", Id: "graph-test"},
	}
	if err := fx.ValidateApp(
		fx.Supply(cfg),
		Module,
		// 消费 servers 组：强制 ValidateApp 展开传输层构造依赖链
		fx.Invoke(fx.Annotate(
			func([]transport.Server) {},
			fx.ParamTags(`group:"servers"`),
		)),
	); err != nil {
		t.Fatalf("依赖图静态校验失败: %v", err)
	}
}
