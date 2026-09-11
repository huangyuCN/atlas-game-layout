package app

import (
	"context"

	"github.com/huangyuCN/atlas-game-layout/pkg/fxkit"
	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/server"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// Module 是 game 服务的完整装配清单。
// 依赖来源（*conf.Bootstrap / *clientv3.Client）由驱动方供给：
// 进程形态经 bootstrap.Assemble 载入配置 Supply；
// 进程内形态由 assemble 以 Options 映射出配置后 Supply。
//
// 注册中心、集群运行时与业务 actor 均在 fx.Invoke 中接入生命周期，
// fx.OnStop 逆序执行：registerResources 先注册，其 OnStop 最后运行——
// 即先停 actor/relay/撮合循环，最后才关闭外部资源（Mongo/Redis/NATS/etcd）。
var Module = fx.Module("game",
	fx.Provide(
		DefaultTuning,
		// ── infra：注册中心 + 外部客户端 + 集群运行时 ──
		fxkit.NewEtcdClient[*conf.Bootstrap],
		fxkit.NewRegistrar, // → registry.Registrar，供 atlas.App 服务注册
		NewRedisClient,
		NewNatsConn,
		NewMongoClient,
		NewActorRuntime,
		// ── data：仓储与会话存储 ──
		repo.NewRedisPlayerCache,
		repo.NewRedisSessionStore,
		newMatchQueueClient,
		matchQueueClientOf,
		newMongoPlayerRepo,
		newPlayerStore,
		// ── biz：业务服务与 actor 访问客户端 ──
		newPlayerService,
		newPlayerStateAccess,
		handler.NewGameHandler,
		// ── server：传输层构造（启停归属驱动方）──
		fx.Annotate(server.NewHTTPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(server.NewGRPCServer, fx.ResultTags(`group:"servers"`)),
	),
	fx.Invoke(
		registerResources,
		registerActor,
	),
)

// closers 汇总需要优雅释放的外部资源（按声明顺序执行 Close/Shutdown）。
type closers struct {
	fx.In

	Etcd  *clientv3.Client
	Nats  *natsgo.Conn
	Mongo *mongo.Client
	Redis *pkredis.Client
}

// registerResources 把外部资源释放接入 fx 生命周期 OnStop：
// 与手写装配时代的 stop 闭包等价，但只写一份且随图自动生效。
func registerResources(lc fx.Lifecycle, c closers) {
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			var firstErr error
			for _, err := range []error{
				c.Mongo.Close(ctx),
				c.Redis.Close(),
				c.Etcd.Close(),
			} {
				if err != nil && firstErr == nil {
					firstErr = err
				}
			}
			c.Nats.Close() // NATS 连接关闭无返回值
			return firstErr
		},
	})
}
