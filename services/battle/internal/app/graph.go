// Package app 是 battle 服务的唯一装配之家：
// Module 列出全部组件清单（infra → data → biz → actor → server 分层），
// 进程形态（cmd/main + atlas App 驱动启停）与进程内形态
// （assemble 经 bootstrap.Boot 驱动启停）共用同一张依赖图。
package app

import (
	"context"

	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/fxkit"
	"github.com/huangyuCN/atlas-game-layout/pkg/middleware"
	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/repo"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/infra"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/ledger"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/server"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// Module 是 battle 服务的完整装配清单。
// 依赖来源（*conf.Bootstrap / *clientv3.Client）由驱动方供给；
// battleactor.Config 由 DefaultConfig 提供默认值，
// 嵌入式形态可用 fx.Decorate 覆盖（见 assemble）。
var Module = fx.Module("battle",
	// 中间件/过滤器默认链（业务可用 fx.Decorate 追加自己的）。
	middleware.Module,
	fx.Provide(
		// 战斗参数 + 出票三件（票据密钥/TTL/接入层地址）：配置缺失即启动失败。
		newActorConfig,
		// ── infra：注册中心 + 外部客户端 + 集群运行时 + 帧广播 ──
		fxkit.Topics[*conf.Bootstrap], // 业务 topic 命名空间（与 actor 平面同源）
		fxkit.NewPublisher,            // 业务事件发布入口（连接 + 命名空间收口） // 业务 topic 命名空间（runtime.env，与订阅方同源）
		fxkit.NewEtcdClient[*conf.Bootstrap],
		fxkit.NewRegistrar[*conf.Bootstrap], // → registry.Registrar，供 atlas.App 服务注册
		// → registry.Discovery：actor 集群选节点与 gRPC 客户端寻址共用（键前缀与注册端同源）
		fxkit.NewEtcdDiscovery[*conf.Bootstrap],
		NewNatsConn,
		NewMongoClient,
		NewActorRuntime,
		NewPubSub,
		// 跨节点结算留档（etcd）+ 激活闸门（任意节点拒绝复活已结束的对局，P1-7）；
		// 闸门以框架接口 cluster.ActivationGate 进图——注入点只认框架接缝，不认实现类型。
		newLedgerStore,
		fx.Annotate(newActivationGate, fx.As(new(cluster.ActivationGate))),
		// ── data：结算仓储 ──
		newResultRepo,
		// ── biz：结算事件实现与 grpc handler ──
		infra.NewNatsSettlePublisher,
		publisherOf,
		// ── 帧面（直连）：注册表 → 连接生命周期桥 → 帧 op 服务端 → KCP/UDP/WS 三监听 → 帧面实例注册器 ──
		newStreamRegistry, // player_id → 连接注册表（帧槽验票登记 + 直连推送端口 + 在场复核 + 结束留档）
		newStreamBridge,   // 连接生命周期桥（引擎断开事件 → 非阻塞投递到战斗 actor）
		newFramePolicy,    // 帧面掉线策略（数据报面空闲超时按掉线窗口推导/校验）
		newFrameMetrics,   // 帧面观测出口（票据拒绝按 reason、帧 op 耗时与失败，P1-4）
		newFrameOps,       // 帧 op 服务端（路由表 + 帧槽验票身份 + 目标校验 + 本地 actor 投递）
		newFrameServers,   // 三个直连帧面（构造 + 观测包装）
		// 帧面实例注册器（服务名 battle-frame）：接入层按 node_id + 元数据端口发现本节点帧面。
		NewFrameRegistrar,
		// ── server：传输层构造（启停归属驱动方）──
		// fx.As：构造函数返回具体类型（便于直接调 Server 字段/方法），仍以 transport.Server 进组。
		fx.Annotate(server.NewHTTPServer, fx.As(new(transport.Server)), fx.ResultTags(`group:"servers"`)),
		// ── 迁移（规格 §8）：编排面（框架 ClusterOps + 状态搬运 + rebalance 选点）──
		// 以迁移编排端口（接口）进图：收件箱按接口依赖，便于单测替换实现。
		fx.Annotate(NewMigrateOps, fx.As(new(actor.MigrateRunner))),
		// gRPC 双面：Edge/Internal 各自独立 listener（fx.Out 自带 servers 组标签），
		// 未启用的面为 nil，由 ActiveServers 在消费侧过滤。
		server.NewGRPCServers,
	),
	fx.Invoke(
		registerResources,
		registerActor,
		registerMigration,
		registerFramePorts,
		registerFrameInstance,
	),
)

// publisherOf 把 NATS 结算发布实现绑定为 biz 接口。
func publisherOf(p *infra.NatsSettlePublisher) biz.SettlePublisher { return p }

// registerActor 注册 BattleActor 并接入生命周期：
// OnStart 启动集群运行时；OnStop 关闭 pubsub、触发运行时优雅停机。
func registerActor(
	lc fx.Lifecycle,
	rt *pkgactor.Runtime,
	reg *pubsub.Registry,
	resultRepo repo.ResultRepo,
	pusher *stream.Registry,
	publisher biz.SettlePublisher,
	cfg actor.Config,
	meter metrics.Collector,
	store *ledger.EtcdStore,
) error {
	err := rt.Register(actor.NewRuntimeProps(actor.DefaultDeps{
		Rt:         rt,
		Registry:   reg,
		ResultRepo: resultRepo,
		Pusher:     pusher,
		Publisher:  publisher,
		// 在场复核端口就是直连注册表：应用掉线事件前复核玩家是否仍有存活连接（规格 §9.2）。
		Presence: pusher,
		// 结算留档端口也是它：帧面据此在懒激活之前拒绝迟到 op，并在重连后补投结果。
		Ledger: pusher,
		// 跨节点留档（P1-7）：结算时写一笔到 etcd（TTL 与本地留档同源），
		// 任意节点的激活闸门据此拒绝复活已结束的对局。
		SharedLedger:    store,
		SharedLedgerTTL: stream.EndedTTL(cfg.TicketTTL, cfg.OfflineTimeout),
		Metrics:         meter,
	}, cfg))
	if err != nil {
		return err
	}
	registerOnlineGauge(meter, pusher)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return rt.Start(ctx) },
		OnStop: func(ctx context.Context) error {
			reg.Close()
			return rt.Shutdown(ctx)
		},
	})
	return nil
}

// registerOnlineGauge 登记拉取式仪表 battle_online_players：采集时回调直连注册表的在册连接数。
// 用拉取式而非增减式的原因与 game_players_online 一致：值来自真相源（注册表），不依赖生命周期
// 事件成对增减——急停、崩溃重启等不回调 OnStop 的路径会让增减式计数永久漂移。
func registerOnlineGauge(meter metrics.Collector, reg *stream.Registry) {
	observability.RegisterObservableGauge(meter, actor.MetricOnlinePlayers, func() float64 {
		return float64(reg.Count())
	})
}

// newStreamBridge 组装连接生命周期桥：注册表 + 非阻塞的本机投递端口（见 tellport.go）。// 投递走 Tell（入队即返回，不做 Ask）——帧引擎在读循环里同步回调，阻塞会拖慢帧处理；
// 目标恒为本节点属主（帧面监听在本节点，注册表里只有本节点的连接），不涉及跨节点寻址。
func newStreamBridge(rt *pkgactor.Runtime, reg *stream.Registry) *stream.Bridge {
	return stream.NewBridge(reg, newLifecyclePort(rt.Raw().Local()).Tell)
}

// newStreamRegistry 组装帧面直连注册表（含结束留档）：留档 TTL 由既有配置派生，不新增配置项——
// 票据有效期 + 掉线窗口（见 stream.EndedTTL：票据过期后不再有任何合法帧 op 能指向该局）。
func newStreamRegistry(cfg actor.Config) *stream.Registry {
	return stream.NewRegistryWithTTL(stream.EndedTTL(cfg.TicketTTL, cfg.OfflineTimeout))
}

// newFramePolicy 从战斗参数取帧面策略（掉线窗口）：数据报面空闲超时按它推导并在装配期校验
// （用生效值而非配置文件原文——嵌入式形态可用 fx.Decorate 覆盖窗口，两处必须同源）。
func newFramePolicy(cfg actor.Config) server.FramePolicy {
	return server.FramePolicy{OfflineTimeout: cfg.OfflineTimeout}
}

// newFrameMetrics 构造帧面观测出口（P1-4）：票据 hook 与 op 包装共用同一份句柄缓存。
func newFrameMetrics(meter metrics.Collector) *server.FrameMetrics {
	return server.NewFrameMetrics(meter)
}

// newFrameOps 组装帧面的帧 op 服务端（配置里的票据密钥在此注入身份解析器，
// 验票通过即经连接生命周期桥登记直连并上报上线；观测出口同源注入）。
func newFrameOps(rt *pkgactor.Runtime, bridge *stream.Bridge, cfg actor.Config,
	fm *server.FrameMetrics) (*frameops.Handler, error) {
	return server.NewFrameOps(rt, bridge, cfg.TicketKey, server.WithFrameMetrics(fm))
}

// newFrameServers 构造三个直连帧面并注入观测包装（耗时直方图 + 失败计数，P1-4②）。
func newFrameServers(cfg *conf.Bootstrap, ops *frameops.Handler, policy server.FramePolicy,
	life *stream.Bridge, fm *server.FrameMetrics) (server.FrameServers, error) {
	return server.NewFrameServers(cfg, ops, policy, life, server.WithFrameMetrics(fm))
}

// registerFramePorts 把三个帧面的直连推送端口挂到注册表（端口即传输服务端，构造完成后才有）；
// 未启用的面（nil）不挂——对应帧面的连接不会被登记，直连推送自然不可达。
func registerFramePorts(faces server.FrameFaces, reg *stream.Registry) {
	if faces.KCP != nil {
		reg.BindPort(transport.KindKCP, stream.ConnPort{Srv: faces.KCP})
	}
	if faces.WS != nil {
		reg.BindPort(transport.KindWebSocket, stream.ConnPort{Srv: faces.WS})
	}
	if faces.UDP != nil {
		reg.BindPort(transport.KindUDP, stream.DatagramPort{Srv: faces.UDP})
	}
}

// registerFrameInstance 把帧面实例的注册/注销接入生命周期：启动注册（失败即启动失败，
// 不让进程带着「帧端口在听但注册中心里没有」的半死状态运行）、停机注销（接入层不再解析到它）。
func registerFrameInstance(lc fx.Lifecycle, r *frameRegistrar) {
	lc.Append(fx.Hook{
		OnStart: r.Register,
		OnStop:  r.Deregister,
	})
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
