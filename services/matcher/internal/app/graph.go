// Package app 是 matcher 服务的唯一装配之家：
// Module 列出全部组件清单（infra → biz → server 分层），
// 进程形态（cmd/main + atlas App 驱动启停）与进程内形态
// （assemble 经 bootstrap.Boot 驱动启停）共用同一张依赖图。
package app

import (
	"context"

	"github.com/huangyuCN/atlas-game-layout/pkg/fxkit"
	"github.com/huangyuCN/atlas-game-layout/pkg/middleware"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/infra"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/server"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/transport"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// Module 是 matcher 服务的完整装配清单。
// 依赖来源（*conf.Bootstrap / *clientv3.Client）由驱动方供给；
// biz.MatchEventSink 由 newSink 提供默认组合，测试注入经 fx.Decorate 覆盖。
var Module = fx.Module("matcher",
	// 中间件/过滤器默认链（业务可用 fx.Decorate 追加自己的）。
	middleware.Module,
	fx.Provide(
		// ── infra：注册中心 + 外部客户端 + 集群客户端 ──
		fxkit.Topics[*conf.Bootstrap], // 业务 topic 命名空间（与 actor 平面同源）
		fxkit.NewPublisher,            // 业务事件发布入口（连接 + 命名空间收口）
		fxkit.NewEtcdClient[*conf.Bootstrap],
		fxkit.NewRegistrar[*conf.Bootstrap], // → registry.Registrar，供 atlas.App 服务注册
		// → registry.Discovery：battle 的 gRPC 寻址（键前缀与注册端同源）
		fxkit.NewEtcdDiscovery[*conf.Bootstrap],
		fxkit.NewRedisClient[*conf.Bootstrap],
		NewNatsConn,
		NewBattleConn,
		NewBattleClient,
		NewMatchmakerRuntime,
		NewMatchmakerParty,
		infra.NewNatsEventPublisher,
		rosterOf,
		infra.NewRedisPlayerTicketMapper,
		infra.NewRedisPartyQueueMapper,
		infra.NewRedisSettleDeduper,
		ticketMapperOf,
		partyMapperOf,
		settleDeduperOf,
		// ── biz：撮合 API 绑定 + 成局观察方 + grpc handler ──
		serviceOf,
		newSink,
		newMatcherDeps,
		handler.NewMatcherHandler,
		// ── server：传输层构造（启停归属驱动方）──
		// fx.As：构造函数返回具体类型（便于直接调 Server 字段/方法），仍以 transport.Server 进组。
		fx.Annotate(server.NewHTTPServer, fx.As(new(transport.Server)), fx.ResultTags(`group:"servers"`)),
		// gRPC 双面：Matcher 服务注册在 internal 面（服务间调用），
		// 未启用的面为 nil，由 ActiveServers 在消费侧过滤。
		server.NewGRPCServers,
	),
	fx.Invoke(
		registerResources,
		registerRuntime,
		registerBattleConn,
	),
)

// ticketMapperOf 把 redis 票据映射实现绑定为 biz 接口（供 fx 按接口注入）。
func ticketMapperOf(m *infra.RedisPlayerTicketMapper) biz.PlayerTicketMapper { return m }

// settleDeduperOf 把 redis 结算去重实现绑定为 biz 接口。
func settleDeduperOf(d *infra.RedisSettleDeduper) biz.MatchSettleDeduper { return d }

// partyMapperOf 把 redis 队伍票映射实现绑定为 biz 接口。
func partyMapperOf(m *infra.RedisPartyQueueMapper) biz.PartyQueueMapper { return m }

// registerRuntime 把撮合运行时接入生命周期。
// 注意：tick 循环是长驻 goroutine，不能用 fx OnStart 的 ctx（15s 超时取消会杀掉循环），
// 故以进程级 context 启动；停止经 Matcher.Stop 的 stopCh 通道完成。
func registerRuntime(lc fx.Lifecycle, rt *matchredis.Runtime) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { return rt.Start(context.Background()) },
		OnStop:  func(ctx context.Context) error { return rt.Stop(ctx) },
	})
}

// registerBattleConn 把 battle internal 面连接接入生命周期（OnStop 关闭）。
func registerBattleConn(lc fx.Lifecycle, conn *serverutil.LazyConn) {
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error { return conn.Close() },
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
