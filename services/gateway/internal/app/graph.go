// Package app 是 gateway 服务的唯一装配之家：
// Module 列出全部组件清单（infra → 会话 → actor 客户端 → server 分层），
// 进程形态（cmd/main + atlas App 驱动启停）与进程内形态
// （assemble + serverutil.ServeAsync 驱动启停）共用同一张依赖图。
package app

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/fxkit"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/server"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/transport"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// Module 是 gateway 服务的完整装配清单。
// 依赖来源（*conf.Bootstrap / *clientv3.Client）由驱动方供给。
//
// 五协议 Server 有两种归宿：
//   - group:"servers"：完整五台，供生产形态的 atlas.App 启停；
//   - group:"embed_servers"：四台（不含 WS），供嵌入式形态启停——
//     单通道形态下 WS 经 assemble 以「/ws 路径 + httptest」包装挂载，
//     不能被独立 Start，故排除在嵌入式子组之外。
var Module = fx.Module("gateway",
	fx.Provide(
		// ── infra：注册中心 + 外部客户端 ──
		fxkit.NewEtcdClient[*conf.Bootstrap],
		fxkit.NewRegistrar, // → registry.Registrar，供 atlas.App 服务注册
		NewRedisClient,
		NewNatsConn,
		// ── server：五协议传输层构造（启停归属驱动方）──
		server.NewHTTPServer,
		server.NewTCPServer,
		server.NewWSServer,
		server.NewKCPServer,
		server.NewUDPServer,
		newServerSet,
		// ── 会话 + actor 集群客户端 ──
		newSessionManager,
		NewActorClient,
		newGateway,
	),
	fx.Invoke(
		registerRelay,
		registerActorLifecycle,
		registerResources,
	),
)

// serverSet 把五协议 Server 汇入两个 servers 组：
// 具体类型同时直供 newGateway 装配（fx 组注解会把结果移出类型空间，
// 故用聚合器双路提供；组值统一为 transport.Server 接口形态）。
type serverSet struct {
	fx.Out

	HTTP transport.Server `group:"servers"`
	TCP  transport.Server `group:"servers"`
	WS   transport.Server `group:"servers"`
	KCP  transport.Server `group:"servers"`
	UDP  transport.Server `group:"servers"`

	HTTPEmbed transport.Server `group:"embed_servers"`
	TCPEmbed  transport.Server `group:"embed_servers"`
	KCPEmbed  transport.Server `group:"embed_servers"`
	UDPEmbed  transport.Server `group:"embed_servers"`
}

// newServerSet 聚合五协议 Server 到 servers 组与嵌入式子组。
func newServerSet(
	httpSrv transport.Server,
	tcpSrv *tcpt.Server, wsSrv *wst.Server, kcpSrv *kcpt.Server, udpSrv *udpt.Server,
) serverSet {
	return serverSet{
		HTTP: httpSrv, TCP: tcpSrv, WS: wsSrv, KCP: kcpSrv, UDP: udpSrv,
		HTTPEmbed: httpSrv, TCPEmbed: tcpSrv, KCPEmbed: kcpSrv, UDPEmbed: udpSrv,
	}
}

// newGateway 装配统一 handler 并注册到各协议 Server。
func newGateway(
	cfg *conf.Bootstrap,
	sess *session.Manager,
	actors *actorclient.Client,
	nc *natsgo.Conn,
	tcpSrv *tcpt.Server, wsSrv *wst.Server, kcpSrv *kcpt.Server, udpSrv *udpt.Server,
) (*server.Gateway, error) {
	instanceID := ""
	if r := cfg.GetRuntime(); r != nil {
		instanceID = r.GetId()
	}
	g := server.NewGateway(instanceID, sess, actors, nc, tcpSrv, wsSrv, kcpSrv, udpSrv)
	if err := server.RegisterGatewayHandlers(tcpSrv, wsSrv, kcpSrv, udpSrv, g); err != nil {
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

// registerActorLifecycle 把 actor 集群运行时接入生命周期。
func registerActorLifecycle(lc fx.Lifecycle, actors *actorclient.Client) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return actors.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return actors.Shutdown(ctx) },
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
