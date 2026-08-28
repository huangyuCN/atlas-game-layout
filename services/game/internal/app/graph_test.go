package app

import (
	"testing"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas/transport"
	"go.uber.org/fx"
)

// TestGraphStaticValidation 对依赖图做静态校验：
// 清单中无重复提供的类型、Invoke 依赖闭包完整、泛型提供器类型解析正确。
// 已知局限：ValidateApp 会沿 Invoke 依赖链实例化部分 Provider
// （构造期错误会被忽略、由真实启动兜底），因此本测试只守护「清单合法性」，
// 组件级行为回归由 services/game/assemble 与 test/e2e 覆盖。
func TestGraphStaticValidation(t *testing.T) {
	cfg := &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "game", Id: "graph-test"},
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
