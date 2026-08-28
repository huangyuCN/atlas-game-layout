// Package app 是 battle 服务的唯一装配之家：
// Module 列出全部组件清单（infra → data → biz → actor → server 分层），
// 进程形态（cmd/main + atlas App 驱动启停）与进程内形态
// （assemble + serverutil.ServeAsync 驱动启停）共用同一张依赖图。
package app

import (
	"context"

	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/fxkit"
	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/repo"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/infra"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/server"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// Module 是 battle 服务的完整装配清单。
// 依赖来源（*conf.Bootstrap / *clientv3.Client）由驱动方供给；
// battleactor.Config 由 DefaultConfig 提供默认值，
// 嵌入式形态可用 fx.Decorate 覆盖（见 assemble）。
var Module = fx.Module("battle",
	fx.Provide(
		actor.DefaultConfig,
		// ── infra：注册中心 + 外部客户端 + 集群运行时 + 帧广播 ──
		fxkit.NewEtcdClient[*conf.Bootstrap],
		fxkit.NewRegistrar, // → registry.Registrar，供 atlas.App 服务注册
		NewNatsConn,
		NewMongoClient,
		NewActorRuntime,
		NewPubSub,
		// ── data：结算仓储 ──
		newResultRepo,
		// ── biz：下行通知/结算事件实现与 grpc handler ──
		infra.NewNatsBattleNotifier,
		infra.NewNatsSettlePublisher,
		notifierOf,
		publisherOf,
		handler.NewBattleHandler,
		// ── server：传输层构造（启停归属驱动方）──
		fx.Annotate(server.NewHTTPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(server.NewGRPCServer, fx.ResultTags(`group:"servers"`)),
	),
	fx.Invoke(
		registerActor,
		registerResources,
	),
)

// notifierOf 把 NATS 下行通知实现绑定为 biz 接口（供 fx 按接口注入）。
func notifierOf(f *infra.NatsBattleNotifier) biz.BattleNotifier { return f }

// publisherOf 把 NATS 结算发布实现绑定为 biz 接口。
func publisherOf(p *infra.NatsSettlePublisher) biz.SettlePublisher { return p }

// registerActor 注册 BattleActor 并接入生命周期：
// OnStart 启动集群运行时；OnStop 关闭 pubsub、触发运行时优雅停机。
func registerActor(
	lc fx.Lifecycle,
	rt *pkgactor.Runtime,
	reg *pubsub.Registry,
	resultRepo repo.ResultRepo,
	notifier biz.BattleNotifier,
	publisher biz.SettlePublisher,
	cfg actor.Config,
) error {
	err := rt.Register(actor.NewRuntimeProps(actor.DefaultDeps{
		Rt:         rt,
		Registry:   reg,
		ResultRepo: resultRepo,
		Notifier:   notifier,
		Publisher:  publisher,
	}, cfg))
	if err != nil {
		return err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return rt.Start(ctx) },
		OnStop: func(ctx context.Context) error {
			reg.Close()
			return rt.Shutdown(ctx)
		},
	})
	return nil
}

// closers 汇总需要优雅释放的外部资源。
type closers struct {
	fx.In

	Etcd  *clientv3.Client
	Nats  *natsgo.Conn
	Mongo *mongo.Client
}

// registerResources 把外部资源释放接入 fx 生命周期 OnStop。
func registerResources(lc fx.Lifecycle, c closers) {
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			var firstErr error
			for _, err := range []error{
				c.Mongo.Close(ctx),
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
