package bootstrap

import (
	"fmt"

	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas-game-layout/lib/confbase"
	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	pkglog "github.com/huangyuCN/atlas-game-layout/pkg/log"
	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// HasBase 是内嵌了配置底座的服务配置接口。
type HasBase interface {
	BaseConfig() *confbase.Base
}

// Assemble 按服务名加载配置并装配通用模块：
// 配置加载（configs/<name>.yaml）、日志初始化、etcd 客户端与注册器（可选）、
// Atlas App 装配（Module）。extra 用于追加服务自定义的 fx 模块。
func Assemble(name string, cfg HasBase, extra ...fx.Option) ([]fx.Option, error) {
	if name == "" {
		return nil, fmt.Errorf("bootstrap: 服务名不能为空")
	}
	if err := config.FromService(name, cfg); err != nil {
		return nil, err
	}
	base := cfg.BaseConfig()
	if base == nil {
		return nil, fmt.Errorf("bootstrap: 配置底座 BaseConfig 不能返回 nil")
	}
	if err := pkglog.Init(pkglog.Options{
		Level:   base.Log.Level,
		Format:  base.Log.Format,
		Service: base.Name,
	}); err != nil {
		return nil, err
	}

	opts := []fx.Option{
		fx.Supply(cfg),
	}
	// etcd 配置存在时才装配注册中心（本地开发可省略）。
	if len(base.Etcd.Endpoints) > 0 {
		opts = append(opts,
			fx.Provide(func() (*clientv3.Client, error) {
				return etcd.NewClient(etcd.Options{Endpoints: base.Etcd.Endpoints})
			}),
			fx.Provide(func(c *clientv3.Client) (registry.Registrar, error) {
				return pkgregistry.NewEtcd(c, pkgregistry.Options{})
			}),
		)
	}
	opts = append(opts, Module(Options{Name: base.Name, ID: base.ID}))
	opts = append(opts, extra...)
	return opts, nil
}
