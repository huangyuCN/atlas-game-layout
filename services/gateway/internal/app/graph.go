// Package app 是 gateway 服务的唯一装配之家：
// Module 列出全部组件清单（infra → 会话 → 域服务客户端 → server 分层），
// 进程形态（cmd/main + atlas.App 驱动启停）与进程内形态
// （assemble 经 bootstrap.Boot 驱动启停）共用同一张依赖图。
package app

import (
	"context"
	"fmt"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gamev1rpc "github.com/huangyuCN/atlas-game-layout/api/game/v1/rpc"
	"github.com/huangyuCN/atlas-game-layout/pkg/fxkit"
	"github.com/huangyuCN/atlas-game-layout/pkg/middleware"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/domainclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/server"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// Module 是 gateway 服务的完整装配清单。
// 依赖来源（*conf.Bootstrap / *clientv3.Client）由驱动方供给。
//
// 业务协议 Server 一律进 group:"servers"，两种形态（进程形态 cmd/main、
// 进程内形态 services/gateway/assemble）都由 atlas.App 启停与注册。
var Module = fx.Module("gateway",
	// 中间件/过滤器默认链（业务可用 fx.Decorate 追加自己的）。
	middleware.Module,
	fx.Provide(
		// ── infra：注册中心 + 外部客户端 ──
		fxkit.NewEtcdClient[*conf.Bootstrap],
		fxkit.NewRegistrar[*conf.Bootstrap], // → registry.Registrar，供 atlas.App 服务注册
		// → registry.Discovery：域服务 gRPC 寻址（Edge 面 / Internal 面同源）
		fxkit.NewEtcdDiscovery[*conf.Bootstrap],
		fxkit.NewRedisClient[*conf.Bootstrap],
		fxkit.Topics[*conf.Bootstrap], // 业务 topic 命名空间（与 actor 平面同源）
		fxkit.NewPublisher,            // 业务事件发布入口（连接 + 命名空间收口）
		NewNatsConn,
		// ── server：业务协议传输层 + gRPC edge 面构造（启停归属驱动方）──
		server.NewHTTPServer,
		server.NewTCPServer,
		server.NewWSServer,
		// gRPC 双面：本轮只启用 edge 面（暂不注册域服务），internal 面留空不启用；
		// fx.Out 自带 servers 组标签，未启用的面为 nil。
		server.NewGRPCServers,
		newServerSet,
		// ── 会话 + 域服务客户端（Edge 面：业务 op 透传；Internal 面：会话联动）──
		newSessionManager,
		NewDomainResolver,
		NewDomainInvoker,
		NewPlayerConn,
		NewPlayerClient,
		newGateway,
	),
	fx.Invoke(
		registerResources,
		registerRelay,
		registerDomainConns,
	),
)

// serverSet 把业务协议 Server 汇入 servers 组（供 atlas.App 统一启停）：
// 具体类型同时直供 newGateway 装配（fx 组注解会把结果移出类型空间，
// 故用聚合器双路提供；组值统一为 transport.Server 接口形态）。
//
// 战斗帧面（KCP/UDP）不在网关：客户端凭 battle_ticket 直连接入层 → battle 帧面
// （阶段 3 批次 5 的破坏性切换，旧承载路径已删除）。
type serverSet struct {
	fx.Out

	HTTP transport.Server `group:"servers"`
	// 业务协议按配置可选：未启用的协议为 nil（fx 值组不允许 optional，
	// 故由 pkg/bootstrap.activeServers 在消费侧过滤——nil 进 atlas.App 会 panic）。
	TCP transport.Server `group:"servers"`
	WS  transport.Server `group:"servers"`
}

// newServerSet 聚合业务协议 Server 到 servers 组与嵌入式子组。
// 两类协议（TCP/WS）按配置可选：conf 协议节 nil 时不进启停组
// （Server 已构造但不被 Start——不监听端口；handler 注册同样跳过，对外不可用），
// 模板可按部署形态只暴露需要的协议；HTTP（管理面/健康检查）始终启用。
func newServerSet(
	cfg *conf.Bootstrap,
	httpSrv *atlashttp.Server,
	tcpSrv *tcpt.Server, wsSrv *wst.Server,
) serverSet {
	set := serverSet{HTTP: httpSrv}
	if cfg.TCPEnabled() {
		set.TCP = tcpSrv
	}
	if cfg.WebSocketEnabled() {
		set.WS = wsSrv
	}
	return set
}

// newGateway 装配统一 handler 并注册到各业务协议 Server。
func newGateway(
	cfg *conf.Bootstrap,
	sess *session.Manager,
	inv opcall.MethodInvoker,
	players gamev1rpc.PlayerServiceClient,
	meter metrics.Collector,
	nc *natsgo.Conn,
	pub *pkgnats.Publisher,
	tcpSrv *tcpt.Server, wsSrv *wst.Server,
) (*server.Gateway, error) {
	instanceID := ""
	if r := cfg.GetRuntime(); r != nil {
		instanceID = r.GetId()
	}
	// 透传引擎：合并各域生成的注解路由表（operation → actor 寻址规则），
	// 业务 op 按条目寻址经 Edge 面（gRPC）投递到域服务；新增域 op 时 gateway 无需改代码
	// （表由 protoc 生成，access=CLIENT 的方法注册传输路由）。
	//
	// **服务白名单在这里显式声明**：battle 的 CLIENT op 自阶段 3 批次 5 起不再经网关
	// （客户端直连接入层 → battle 帧面），故 BattleServiceRouteTable 不参与合并——
	// 经网关发战斗 op 在帧引擎层明确失败，不再是静默的"表里没有"。
	table, err := relay.Merge(gamev1.PlayerServiceRouteTable)
	if err != nil {
		return nil, fmt.Errorf("gateway: 透传路由表合并失败: %w", err)
	}
	g := server.NewGateway(instanceID, table, sess, inv, players, meter, nc, pub, tcpSrv, wsSrv,
		server.NewVersionGate(cfg.GetRuntime()))
	if err := server.RegisterGatewayHandlers(cfg, tcpSrv, wsSrv, g); err != nil {
		return nil, err
	}
	return g, nil
}

// registerRelay 把推送订阅与会话心跳清扫接入 fx 生命周期。
func registerRelay(lc fx.Lifecycle, g *server.Gateway, sess *session.Manager) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ctx, c := context.WithCancel(context.Background())
			cancel = c
			if err := g.StartRelay(ctx); err != nil {
				return fmt.Errorf("app: 启动推送订阅失败: %w", err)
			}
			sess.Start(ctx)
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
}

// registerDomainConns 把域服务连接接入生命周期：OnStop 关闭解析器缓存的 Edge 面连接
// 与会话联动用的 Internal 面连接（连接随进程结束一并释放）。
func registerDomainConns(lc fx.Lifecycle, resolver *domainclient.Resolver, playerConn *serverutil.LazyConn) {
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			firstErr := resolver.Close()
			if err := playerConn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			return firstErr
		},
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
