package app

import (
	"context"
	"fmt"

	gamev1rpc "github.com/huangyuCN/atlas-game-layout/api/game/v1/rpc"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/domainclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"
	"github.com/huangyuCN/atlas/contrib/actor/opgrpc"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/registry"
	natsgo "github.com/nats-io/nats.go"
	"google.golang.org/grpc"
)

// NewNatsConn 装配 NATS 连接（推送订阅 + gateway 控制通道）。
func NewNatsConn(cfg *conf.Bootstrap) (*natsgo.Conn, error) {
	url := ""
	name := consts.ServiceGateway
	if r := cfg.GetRuntime(); r != nil && r.GetName() != "" {
		name = r.GetName()
	}
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		url = d.GetNats().GetUrl()
	}
	conn, err := nats.Connect(nats.Options{URL: url, Name: name})
	if err != nil {
		return nil, fmt.Errorf("app: 构造 NATS 连接失败: %w", err)
	}
	return conn, nil
}

// sessionOptionsOf 把会话租期配置映射为管理器参数：空值透传零值
// （默认值由 session 包兜底，见 session.DefaultTTL），非法值启动期报错。
func sessionOptionsOf(cfg *conf.Bootstrap) (session.Options, error) {
	ttl, err := config.ParseDuration(cfg.GetSession().GetTtl())
	if err != nil {
		return session.Options{}, fmt.Errorf("app: session.ttl 无效: %w", err)
	}
	sweep, err := config.ParseDuration(cfg.GetSession().GetSweepInterval())
	if err != nil {
		return session.Options{}, fmt.Errorf("app: session.sweep_interval 无效: %w", err)
	}
	return session.Options{TTL: ttl, SweepInterval: sweep}, nil
}

// newSessionManager 装配分布式会话管理器（并注入指标采集器）。
func newSessionManager(cfg *conf.Bootstrap, cli *pkredis.Client, meter metrics.Collector) (*session.Manager, error) {
	opts, err := sessionOptionsOf(cfg)
	if err != nil {
		return nil, err
	}
	instanceID := ""
	if r := cfg.GetRuntime(); r != nil {
		instanceID = r.GetId()
	}
	m := session.NewManager(session.NewRedisStore(cli), instanceID, opts)
	m.SetMeter(meter)
	return m, nil
}

// NewDomainResolver 装配域服务连接解析器：业务 op 透传按路由条目寻址，
// 拨到该域 rpc/ 平面的 **Edge 面**（`grpc-edge` 端点；连接惰性建立、按服务复用）。
func NewDomainResolver(discovery registry.Discovery, mws serverutil.ClientMiddlewares) *domainclient.Resolver {
	return domainclient.NewResolver(discovery, mws)
}

// NewDomainInvoker 把连接解析器包成「按方法寻址」的投递端口
// （opcall.DeliverRemote 的唯一后端：网关不持有任何 actor 集群运行时）。
func NewDomainInvoker(r *domainclient.Resolver) opcall.MethodInvoker {
	return opgrpc.NewInvoker(r)
}

// NewPlayerConn 装配 game **internal 面**（可信区）的惰性连接：网关的会话联动
// （Register/Login/Logout）经类型化客户端调用——这些方法只注册在 internal listener，
// 与客户端 op 走的 edge 面端口隔离。惰性拨号使网关启动不依赖 game 已在线
// （四个服务可任意顺序启动/重启，见 serverutil.LazyConn）。
func NewPlayerConn(discovery registry.Discovery, mws serverutil.ClientMiddlewares) *serverutil.LazyConn {
	return serverutil.NewLazyConn(func(ctx context.Context) (grpc.ClientConnInterface, error) {
		conn, err := serverutil.DialDomain(ctx, discovery, consts.ServiceGame, serverutil.SchemeGRPC, mws)
		if err != nil {
			return nil, err
		}
		return conn, nil
	})
}

// NewPlayerClient 基于 internal 面连接构造 PlayerService 类型化客户端。
func NewPlayerClient(conn *serverutil.LazyConn) gamev1rpc.PlayerServiceClient {
	return gamev1rpc.NewPlayerServiceClient(conn)
}
