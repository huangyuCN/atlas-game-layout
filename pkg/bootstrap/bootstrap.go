// Package bootstrap 提供游戏模板统一的 Atlas App 启动装配：
// 各服务通过 fx 提供 transport.Server 与注册器，本包负责组装 App
// 并接入 fx 生命周期（OnStart 启动、OnStop 优雅停机）。
package bootstrap

import (
	"context"

	"github.com/huangyuCN/atlas"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/transport"
	"go.uber.org/fx"
)

// Options 是 bootstrap 装配的显式参数（服务元数据）。
type Options struct {
	// Name 是服务名（注册中心中的服务名）。
	Name string
	// ID 是实例 ID（空则由 Atlas 自动生成 UUID）。
	ID string
	// Version 是服务版本。
	Version string
}

// Params 是 fx 注入的依赖：各服务提供的 Server 与注册器/日志器。
type Params struct {
	fx.In

	Servers   []transport.Server `group:"servers"`
	Registrar registry.Registrar `optional:"true"`
	Logger    atlaslog.Logger    `optional:"true"`
}

// Result 是 bootstrap 装配的输出：组装完成的 Atlas App。
type Result struct {
	fx.Out

	App *atlas.App
}

// New 组装 Atlas App：注入服务元数据、Server 列表与注册器。
func New(p Params, o Options) (Result, error) {
	opts := make([]atlas.Option, 0, 6)
	if o.Name != "" {
		opts = append(opts, atlas.Name(o.Name))
	}
	if o.ID != "" {
		opts = append(opts, atlas.ID(o.ID))
	}
	if o.Version != "" {
		opts = append(opts, atlas.Version(o.Version))
	}
	if len(p.Servers) > 0 {
		opts = append(opts, atlas.Server(p.Servers...))
	}
	if p.Registrar != nil {
		opts = append(opts, atlas.Registrar(p.Registrar))
	}
	if p.Logger != nil {
		opts = append(opts, atlas.Logger(p.Logger))
	}
	return Result{App: atlas.New(opts...)}, nil
}

// Module 返回 bootstrap 的 fx 装配：
// 提供 App 构造 + 生命周期注册（OnStart 后台 Run、OnStop 优雅停机）。
func Module(o Options) fx.Option {
	return fx.Options(
		fx.Supply(o),
		fx.Provide(New),
		fx.Invoke(RegisterLifecycle),
	)
}

// RegisterLifecycle 把 Atlas App 接入 fx 生命周期：
// OnStart 后台启动 App（监听信号/优雅停机），OnStop 触发 App.Stop。
func RegisterLifecycle(lc fx.Lifecycle, app *atlas.App) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				if err := app.Run(); err != nil {
					atlaslog.Errorf("app run failed: %v", err)
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			return app.Stop()
		},
	})
}
