// Package bootstrap 提供游戏模板统一的 Atlas App 启动装配：
// 各服务通过 fx 提供 transport.Server 与注册器，本包负责组装 App
// 并接入 fx 生命周期（OnStart 启动、OnStop 优雅停机）。
package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/transport"
	"go.uber.org/fx"
)

// startTimeout 是等待 App 启动结果的上限：超时不再阻断启动（避免注册中心抖动
// 拖死进程），交由 App.Run 的后台日志暴露。
const startTimeout = 30 * time.Second

// Options 是 bootstrap 装配的显式参数（服务元数据）。
type Options struct {
	// Name 是服务名（注册中心中的服务名）。
	Name string
	// ID 是实例 ID；进程形态由 AssembleLoaded 回填 runtime.id（缺省主机名），
	// 留空时 Atlas 会自行生成 UUID——那会与 actor NodeID 不一致。
	ID string
	// Version 是服务版本。
	Version string
	// DisableSignal 为 true 时不注册信号处理：进程内/嵌入式形态（services/*/assemble）
	// 与宿主/测试进程共用信号，不能把 SIGTERM 拦成自己的优雅停机。
	DisableSignal bool
}

// Params 是 fx 注入的依赖：各服务提供的 Server 与注册器/日志器。
type Params struct {
	fx.In

	Servers   []transport.Server `group:"servers"`
	Registrar registry.Registrar `optional:"true"`
	Logger    atlaslog.Logger    `optional:"true"`
}

// StartSignal 传递 App 启动结果与退出完成：服务注册完成时得到 nil，启动失败时得到错误；
// App.Run 返回（服务端已停完、资源已回收）后 stopped 关闭。
// 由 New 产出、RegisterLifecycle 消费，把「注册失败」升级为进程启动失败——
// 否则实例会带着「端口已监听但注册中心里没有」的半死状态继续运行
// （实例键冲突即属此类，见 atlas registry.ErrInstanceConflict）。
type StartSignal struct {
	ch      chan error
	stopped chan struct{}
}

// notify 非阻塞上报启动结果（缓冲 1：先到者胜，重复上报忽略）。
func (s StartSignal) notify(err error) {
	select {
	case s.ch <- err:
	default:
	}
}

// markStopped 标记 App.Run 已退出（幂等）。
func (s StartSignal) markStopped() {
	select {
	case <-s.stopped:
	default:
		close(s.stopped)
	}
}

// wait 等待启动结果：收到返回 (err, true)，超过 timeout 返回 (nil, false)。
func (s StartSignal) wait(timeout time.Duration) (error, bool) {
	select {
	case err := <-s.ch:
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

// Result 是 bootstrap 装配的输出：组装完成的 Atlas App 与启动结果信号。
type Result struct {
	fx.Out

	App   *atlas.App
	Start StartSignal
}

// New 组装 Atlas App：注入服务元数据、Server 列表与注册器。
func New(p Params, o Options) (Result, error) {
	start := StartSignal{ch: make(chan error, 1), stopped: make(chan struct{})}
	opts := make([]atlas.Option, 0, 8)
	// AfterStart 在服务注册成功之后执行：用它上报「已就绪」。
	opts = append(opts, atlas.AfterStart(func(context.Context) error {
		start.notify(nil)
		return nil
	}))
	if o.DisableSignal {
		opts = append(opts, atlas.Signal())
	}
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
	return Result{App: atlas.New(opts...), Start: start}, nil
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
// OnStart 后台启动 App 并等待启动结果（注册完成即返回，注册失败则启动失败），
// OnStop 触发 App.Stop。
func RegisterLifecycle(lc fx.Lifecycle, app *atlas.App, start StartSignal) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer start.markStopped() // 标记 Run 已退出：Stop 等到它才返回
				if err := app.Run(); err != nil {
					atlaslog.Errorf("app run failed: %v", err)
					start.notify(err)
				}
			}()
			err, ok := start.wait(startTimeout)
			if !ok {
				// 注册中心抖动等慢启动场景：不阻断启动，交由 App.Run 的后台日志暴露。
				atlaslog.Warnf("bootstrap: 等待服务启动结果超时（%s），继续启动", startTimeout)
				return nil
			}
			if err != nil {
				return fmt.Errorf("bootstrap: 服务启动失败: %w", err)
			}
			return nil
		},
		OnStop: func(context.Context) error {
			return app.Stop()
		},
	})
}
