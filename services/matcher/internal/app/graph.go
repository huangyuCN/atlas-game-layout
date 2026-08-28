// Package app 是 matcher 服务的唯一装配之家：
// Module 列出全部组件清单（infra → biz → server 分层），
// 进程形态（cmd/main + atlas App 驱动启停）与进程内形态
// （assemble + serverutil.ServeAsync 驱动启停）共用同一张依赖图。
package app

import (
	"context"

	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/fxkit"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/infra"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/server"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// Module 是 matcher 服务的完整装配清单。
// 依赖来源（*conf.Bootstrap / *clientv3.Client）由驱动方供给；
// biz.MatchEventSink 由 newSink 提供默认组合，测试注入经 fx.Decorate 覆盖。
var Module = fx.Module("matcher",
	fx.Provide(
		// ── infra：注册中心 + 外部客户端 + 集群客户端 ──
		fxkit.NewEtcdClient[*conf.Bootstrap],
		fxkit.NewRegistrar, // → registry.Registrar，供 atlas.App 服务注册
		NewRedisClient,
		NewNatsConn,
		NewActorRuntime,
		NewMatchmakerRuntime,
		infra.NewRedisPlayerTicketMapper,
		infra.NewRedisSettleDeduper,
		ticketMapperOf,
		settleDeduperOf,
		// ── biz：撮合 API 绑定 + 成局观察方 + grpc handler ──
		serviceOf,
		newSink,
		handler.NewMatcherHandler,
		// ── server：传输层构造（启停归属驱动方）──
		fx.Annotate(server.NewHTTPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(server.NewGRPCServer, fx.ResultTags(`group:"servers"`)),
	),
	fx.Invoke(
		registerRuntime,
		registerActorLifecycle,
		registerResources,
	),
)

// ticketMapperOf 把 redis 票据映射实现绑定为 biz 接口（供 fx 按接口注入）。
func ticketMapperOf(m *infra.RedisPlayerTicketMapper) biz.PlayerTicketMapper { return m }

// settleDeduperOf 把 redis 结算去重实现绑定为 biz 接口。
func settleDeduperOf(d *infra.RedisSettleDeduper) biz.MatchSettleDeduper { return d }

// registerRuntime 把撮合运行时接入生命周期。
// 注意：tick 循环是长驻 goroutine，不能用 fx OnStart 的 ctx（15s 超时取消会杀掉循环），
// 故以进程级 context 启动；停止经 Matcher.Stop 的 stopCh 通道完成。
func registerRuntime(lc fx.Lifecycle, rt *matchredis.Runtime) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { return rt.Start(context.Background()) },
		OnStop:  func(ctx context.Context) error { return rt.Stop(ctx) },
	})
}

// registerActorLifecycle 把 actor 集群客户端接入生命周期。
func registerActorLifecycle(lc fx.Lifecycle, rt *pkgactor.Runtime) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return rt.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return rt.Shutdown(ctx) },
	})
}

// closers 汇总需要优雅释放的外部资源。
type closers struct {
	fx.In

	Etcd  *clientv3.Client
	Nats  *natsgo.Conn
	Redis *pkredis.Client
}

// registerResources 把外部资源释放接入 fx 生命周期 OnStop。
func registerResources(lc fx.Lifecycle, c closers) {
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			firstErr := c.Redis.Close()
			c.Nats.Close() // NATS 连接关闭无返回值
			if err := c.Etcd.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			return firstErr
		},
	})
}
