package bootstrap

import (
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	pkglog "github.com/huangyuCN/atlas-game-layout/pkg/log"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

// ConfigLike 是各服务配置 Bootstrap 的公共 getter 子集：
// 所有服务的 internal/conf/conf.proto 都引用公共配置消息，
// 生成的 *Bootstrap 自动满足该接口。
type ConfigLike interface {
	GetRuntime() *configspb.Runtime
	GetRegistry() *configspb.Registry
	GetLog() *configspb.Log
}

// Assemble 按服务名加载配置并装配通用模块：
// 配置加载（services/<name>/configs/config.yaml）、日志初始化、
// Atlas App 装配（Module）。extra 用于追加服务自定义的 fx 模块。
//
// 注册中心（etcd 客户端 / Registrar）由各服务的 fx 模块自行声明，
// 可直接复用 pkg/fxkit 的泛型提供器。
func Assemble(name string, cfg proto.Message, extra ...fx.Option) ([]fx.Option, error) {
	if name == "" {
		return nil, fmt.Errorf("bootstrap: 服务名不能为空")
	}
	if err := config.FromService(name, cfg); err != nil {
		return nil, err
	}
	return AssembleLoaded(cfg, extra...)
}

// AssembleLoaded 对已加载的配置装配通用模块（拆分出来便于单测）。
func AssembleLoaded(cfg proto.Message, extra ...fx.Option) ([]fx.Option, error) {
	like, ok := cfg.(ConfigLike)
	if !ok {
		return nil, fmt.Errorf("bootstrap: 配置类型 %T 未引用公共配置消息", cfg)
	}
	runtime := like.GetRuntime()
	if runtime == nil || runtime.GetName() == "" {
		return nil, fmt.Errorf("bootstrap: 配置缺少 runtime.name")
	}

	logOpts := pkglog.Options{Service: runtime.GetName()}
	if l := like.GetLog(); l != nil {
		logOpts.Level = l.GetLevel()
		logOpts.Format = l.GetFormat()
	}
	if err := pkglog.Init(logOpts); err != nil {
		return nil, err
	}

	opts := []fx.Option{
		fx.Supply(cfg),
		Module(Options{Name: runtime.GetName(), ID: runtime.GetId()}),
	}
	opts = append(opts, extra...)
	return opts, nil
}
