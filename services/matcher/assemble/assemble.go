// Package assemble 提供 matcher 服务的进程内（嵌入式）装配入口：
// 与生产形态共用 internal/app 的同一张 fx 依赖图，仅驱动方式不同
// （进程形态由 atlas.App 管信号与启停；本形态以 fx 编程式 Start/Stop
// + serverutil.ServeAsync 驱动），供集成测试与嵌入式部署复用。
package assemble

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	matcherapp "github.com/huangyuCN/atlas-game-layout/services/matcher/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas/matchmaker"
	"github.com/huangyuCN/atlas/transport"
	"go.uber.org/fx"
)

// ServiceVersion 是注册到注册中心的服务版本（进程内形态固定值）。
const ServiceVersion = "0.1.0"

// Options 是进程内装配参数。
type Options struct {
	NodeID        string
	EtcdEndpoints []string
	NatsURL       string
	RedisAddr     string
	// SinkOverride 是成局观察方的测试注入点
	//（nil 时用默认组合：nats 发布 + actor 开局调用，见 internal/app newSink）。
	SinkOverride biz.MatchEventSink
}

// Matcher 是装配完成的 matcher 服务句柄。
type Matcher struct {
	GRPCURL string             // host:port（battle/测试直连用）
	Service matchmaker.Service // 撮合运行时 API（测试/观测用）
	stop    func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件（含 servers 值组）。
type graphHandles struct {
	fx.In

	Service matchmaker.Service
	Servers []transport.Server `group:"servers"`
}

// New 装配并启动 matcher 服务：映射配置 → 启动 fx 依赖图
// （含撮合 tick 循环与 actor 客户端生命周期）→ 后台起传输层。
// matcher 无自注册需求（battle 经服务发现被本服务调用，反向不需要）。
func New(ctx context.Context, o Options) (*Matcher, error) {
	var h graphHandles
	root := fx.New(
		fx.NopLogger,
		fx.Supply(newBootstrap(o)),
		overrideSink(o.SinkOverride),
		matcherapp.Module,
		fx.Populate(&h),
	)
	if err := root.Err(); err != nil {
		return nil, fmt.Errorf("assemble: 依赖图校验失败: %w", err)
	}
	if err := root.Start(ctx); err != nil {
		return nil, fmt.Errorf("assemble: 启动组件失败: %w", err)
	}

	stopServers, err := startServers(h.Servers)
	if err != nil {
		_ = root.Stop(context.Background())
		return nil, err
	}

	m := &Matcher{GRPCURL: grpcHostOf(h.Servers), Service: h.Service}
	m.stop = func(ctx context.Context) error {
		// 先停服务器，再走 fx 根应用逆序回收（撮合循环/actor/外部资源）。
		// 服务器已停时 stopServers 返回错误不应阻断资源回收。
		sErr := stopServers(ctx)
		rErr := root.Stop(ctx)
		if sErr != nil {
			return sErr
		}
		return rErr
	}
	return m, nil
}

// Stop 停止 matcher 服务并释放全部资源（停服务器 → 组件逆序回收）。
func (m *Matcher) Stop(ctx context.Context) error {
	if m.stop == nil {
		return nil
	}
	return m.stop(ctx)
}

// overrideSink 以 fx.Decorate 覆盖依赖图中的成局观察方默认组合；
// override 为 nil 时透传基础值（装饰器恒挂载、行为零差异）。
func overrideSink(override biz.MatchEventSink) fx.Option {
	return fx.Decorate(func(base biz.MatchEventSink) biz.MatchEventSink {
		if override != nil {
			return override
		}
		return base
	})
}

// startServers 以进程内形态后台启动全部传输层（gRPC + HTTP 健康端）。
func startServers(servers []transport.Server) (func(context.Context) error, error) {
	_, stop, err := serverutil.ServeAsync(5*time.Second, servers...)
	if err != nil {
		return nil, fmt.Errorf("assemble: 启动传输层: %w", err)
	}
	return stop, nil
}

// grpcHostOf 从 servers 值组中找 gRPC 端点（fx 值组不保证提供顺序，
// 不能依赖下标；缺端点说明图装配异常，返回空串由调用方观测）。
func grpcHostOf(servers []transport.Server) string {
	for _, srv := range servers {
		if ep, ok := srv.(interface{ Endpoint() (*url.URL, error) }); ok {
			if u, err := ep.Endpoint(); err == nil && u.Scheme == "grpc" {
				return u.Host
			}
		}
	}
	return ""
}

// newBootstrap 把进程内装配参数映射为服务配置：
// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 监听地址固定随机端口（进程内形态不做端口管理）。
func newBootstrap(o Options) *conf.Bootstrap {
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "matcher", Id: o.NodeID},
		Registry: &configspb.Registry{
			Etcd: &configspb.Registry_Etcd{Endpoints: o.EtcdEndpoints},
		},
		Server: &configspb.Server{
			Grpc: &configspb.Server_GRPC{Addr: randomPort},
			Http: &configspb.Server_HTTP{Addr: randomPort},
		},
		Data: &configspb.Data{
			Redis: &configspb.Data_Redis{Addr: o.RedisAddr},
			Nats:  &configspb.Data_Nats{Url: o.NatsURL},
		},
	}
}
