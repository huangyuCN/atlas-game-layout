package bootstrap

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	"go.uber.org/fx"
)

// stopWaitTimeout 是进程内 Stop 等待服务端真正停止的上限。
const stopWaitTimeout = 10 * time.Second

// Instance 是进程内启动的服务实例：App 为 fx 根应用，Stop 会等到服务端真正停止后返回。
type Instance struct {
	App   *fx.App
	start StartSignal
}

// Stop 停止实例：App.Stop 触发注销（同步）与停机，真正的 server.Stop 在 App.Run 的
// errgroup 里执行——不等它就会出现「Stop 已返回但端口还没关」，故这里等 Run 退出。
func (i *Instance) Stop(ctx context.Context) error {
	err := i.App.Stop(ctx)
	select {
	case <-i.start.stopped:
	case <-ctx.Done():
		if err == nil {
			err = ctx.Err()
		}
	case <-time.After(stopWaitTimeout):
		if err == nil {
			err = fmt.Errorf("bootstrap: 等待服务停止超时（%s）", stopWaitTimeout)
		}
	}
	return err
}

// Servers 是 servers 值组的回捞载体：各服务的进程内 handles 内嵌它，
// Boot 借此归集端点（fx 值组只能经带 tag 的结构体字段回捞）。
type Servers struct {
	fx.In

	List []transport.Server `group:"servers"`
}

// All 返回回捞到的服务端（供端点归集）。
func (s *Servers) All() []transport.Server { return s.List }

// Boot 以进程内（嵌入式）形态启动一张服务依赖图：装配 fx（服务模块 + ModuleForEmbedded）
// 并启动，返回实例句柄与 require 声明的 scheme → 就绪端点。返回时服务端已启动、实例已注册
// （ModuleForEmbedded 的就绪信号保证）；停机走 root.Stop（注销 → 停服务端 → 资源逆序回收）。
//
// handles 是要回捞的句柄（须内嵌 Servers）；require 里任一 scheme 缺端点即报错并回收——
// 缺端点意味着该协议没起来或没进启停组，属装配错误，不能静默留空。
func Boot(ctx context.Context, cfg ConfigLike, app fx.Option,
	handles interface{ All() []transport.Server }, require ...string) (*Instance, map[string]*url.URL, error) {
	// own 回捞 Boot 自用的组件（servers 值组 + 启动信号），与调用方句柄一次 Populate。
	var own struct {
		fx.In

		Servers
		Start StartSignal
	}
	root := fx.New(
		fx.NopLogger,
		// 嵌入式形态默认 noop 采集器（观测由进程形态经 AssembleLoaded 接线）。
		fx.Provide(func() metrics.Collector { return metrics.Noop() }),
		fx.Supply(cfg),
		app,
		ModuleForEmbedded(cfg),
		fx.Populate(handles, &own),
	)
	if err := root.Err(); err != nil {
		return nil, nil, fmt.Errorf("bootstrap: 依赖图校验失败: %w", err)
	}
	if err := root.Start(ctx); err != nil {
		// 组件图失败时 fx 已按逆序回收已构造组件（含服务端与资源关闭钩子）。
		return nil, nil, fmt.Errorf("bootstrap: 启动组件失败: %w", err)
	}
	eps, err := serverutil.Endpoints(handles.All())
	if err != nil {
		_ = root.Stop(context.Background())
		return nil, nil, fmt.Errorf("bootstrap: %w", err)
	}
	urls, err := requiredEndpoints(eps, require)
	if err != nil {
		_ = root.Stop(context.Background())
		return nil, nil, fmt.Errorf("bootstrap: %w", err)
	}
	return &Instance{App: root, start: own.Start}, urls, nil
}

// requiredEndpoints 按 require 取端点；缺任一即报错。
func requiredEndpoints(eps map[string]*url.URL, require []string) (map[string]*url.URL, error) {
	out := make(map[string]*url.URL, len(require))
	for _, scheme := range require {
		ep, ok := eps[scheme]
		if !ok {
			return nil, fmt.Errorf("缺少 %s 端点", scheme)
		}
		out[scheme] = ep
	}
	return out, nil
}
