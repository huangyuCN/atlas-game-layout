// Package assemble 提供 matcher 服务的进程内（嵌入式）装配入口：
// 与进程形态共用 internal/app 的同一张 fx 依赖图与同一套启停路径
// （bootstrap.Boot → atlas.App：启动服务端、注册实例、注销与停机），
// 仅不注册进程信号——宿主/测试进程的信号不能被本实例拦下。
// 供集成测试与嵌入式部署复用。
package assemble

import (
	"context"

	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	matcherapp "github.com/huangyuCN/atlas-game-layout/services/matcher/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas/matchmaker"
	"go.uber.org/fx"
)

// Options 是进程内装配参数。
type Options struct {
	NodeID        string
	EtcdEndpoints []string
	NatsURL       string
	RedisAddrs    []string
	// SinkOverride 是成局观察方的测试注入点
	//（nil 时用默认组合：nats 发布 + actor 开局调用，见 internal/app newSink）。
	SinkOverride biz.MatchEventSink
	// Namespace 是注册中心键前缀（可选）：嵌入式/测试形态用它把实例与常驻进程隔离，
	// 并避免上一次运行残留的实例键（租约未过期）导致注册冲突；
	// 缺省按 runtime.env 派生（/atlas/services/<env>）。
	Namespace string
}

// Matcher 是装配完成的 matcher 服务句柄。
type Matcher struct {
	GRPCURL string             // host:port（battle/测试直连用）
	Service matchmaker.Service // 撮合运行时 API（测试/观测用）
	stop    func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件。
type graphHandles struct {
	fx.In

	bootstrap.Servers
	Service matchmaker.Service
}

// New 装配并启动 matcher 服务：映射配置 → bootstrap.Boot 启动依赖图
// （含撮合 tick 循环与 actor 客户端生命周期），由 atlas.App 统一启动服务端并注册实例。
func New(ctx context.Context, o Options) (*Matcher, error) {
	var h graphHandles
	inst, urls, err := bootstrap.Boot(ctx, newBootstrap(o),
		fx.Options(matcherapp.Module, overrideSink(o.SinkOverride)), &h, serverutil.SchemeGRPC)
	if err != nil {
		return nil, err
	}
	return &Matcher{GRPCURL: urls[serverutil.SchemeGRPC].Host, Service: h.Service, stop: inst.Stop}, nil
}

// Stop 停止 matcher 服务并释放全部资源
// （注销实例 → 停服务端 → 组件逆序回收，均由 atlas.App 驱动）。
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

// newBootstrap 把进程内装配参数映射为服务配置：
// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 监听地址固定随机端口（进程内形态不做端口管理）。
func newBootstrap(o Options) *conf.Bootstrap {
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "matcher", Id: o.NodeID},
		Registry: &configspb.Registry{
			Etcd:      &configspb.Registry_Etcd{Endpoints: o.EtcdEndpoints},
			Namespace: o.Namespace,
		},
		Server: &configspb.Server{
			Grpc: &configspb.Server_GRPC{Addr: randomPort},
			Http: &configspb.Server_HTTP{Addr: randomPort},
		},
		Data: &configspb.Data{
			Redis: &configspb.Data_Redis{Addrs: o.RedisAddrs},
			Nats:  &configspb.Data_Nats{Url: o.NatsURL},
		},
	}
}
