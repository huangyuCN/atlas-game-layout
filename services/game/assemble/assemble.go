// Package assemble 提供 game 服务的进程内（嵌入式）装配入口：
// 与生产形态共用 internal/app 的同一张 fx 依赖图，仅驱动方式不同
// （进程形态由 atlas.App 管信号与启停；本形态以 fx 编程式 Start/Stop
// + serverutil.ServeAsync 驱动），供集成测试与嵌入式部署复用，
// 装配逻辑只此一份、不再手工重复接线。
package assemble

import (
	"context"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/actor"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	gameapp "github.com/huangyuCN/atlas-game-layout/services/game/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	MongoURI      string
	MongoDB       string
}

// Game 是装配完成的 game 服务句柄。
type Game struct {
	Runtime *actor.Runtime
	GRPCURL string // host:port（gRPC 直连用）
	HTTPURL string // host:port（健康检查 + 玩家 REST 管理接口）
	stop    func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件（含 servers 值组）。
type graphHandles struct {
	fx.In

	Runtime *actor.Runtime
	Etcd    *clientv3.Client
	Servers []transport.Server `group:"servers"`
}

// New 装配并启动 game 服务：映射配置 → 启动 fx 依赖图（含 actor 运行时）
// → 后台起传输层 → 注册服务实例。实例 ID 必须 == actor NodeID
// （集群懒激活按服务实例选节点，见 cluster placement 的硬约束）。
func New(ctx context.Context, o Options) (*Game, error) {
	var h graphHandles
	root := fx.New(
		fx.NopLogger,
		fx.Supply(newBootstrap(o)),
		gameapp.Module,
		fx.Populate(&h),
	)
	if err := root.Err(); err != nil {
		return nil, fmt.Errorf("assemble: 依赖图校验失败: %w", err)
	}
	if err := root.Start(ctx); err != nil {
		// 组件图失败时 fx 已按逆序回收已构造组件（含资源关闭钩子）。
		return nil, fmt.Errorf("assemble: 启动组件失败: %w", err)
	}

	urls, stopServers, err := startServers(h.Servers)
	if err != nil {
		_ = root.Stop(context.Background())
		return nil, err
	}
	reg, err := registerInstance(ctx, o.NodeID, h.Etcd, urls)
	if err != nil {
		_ = stopServers(context.Background())
		_ = root.Stop(context.Background())
		return nil, err
	}

	g := &Game{Runtime: h.Runtime, GRPCURL: urls.grpc, HTTPURL: urls.http}
	g.stop = func(ctx context.Context) error {
		firstErr := reg.Deregister(ctx, instanceOf(o.NodeID, urls))
		if err := stopServers(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := root.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return g, nil
}

// Stop 停止 game 服务并释放全部资源（注销实例 → 停服务器 → 组件逆序回收）。
func (g *Game) Stop(ctx context.Context) error {
	if g.stop == nil {
		return nil
	}
	return g.stop(ctx)
}

// serverURLs 是两台传输层的就绪端点（host:port）。
type serverURLs struct {
	grpc string
	http string
}

// startServers 以进程内形态后台启动全部传输层，并按 scheme 分派端点
// （fx 值组不保证提供顺序，不能依赖下标）。
func startServers(servers []transport.Server) (serverURLs, func(context.Context) error, error) {
	eps, stop, err := serverutil.ServeAsync(5*time.Second, servers...)
	if err != nil {
		return serverURLs{}, nil, fmt.Errorf("assemble: 启动传输层: %w", err)
	}
	var urls serverURLs
	for _, ep := range eps {
		switch ep.Scheme {
		case "grpc":
			urls.grpc = ep.Host
		case "http":
			urls.http = ep.Host
		}
	}
	if urls.grpc == "" || urls.http == "" {
		_ = stop(context.Background())
		return serverURLs{}, nil, fmt.Errorf("assemble: 未识别的传输层端点集: %v", eps)
	}
	return urls, stop, nil
}

// registerInstance 构造注册中心并注册服务实例。
func registerInstance(ctx context.Context, nodeID string, ec *clientv3.Client, urls serverURLs) (registry.Registrar, error) {
	reg, err := pkgregistry.NewEtcd(ec, pkgregistry.Options{})
	if err != nil {
		return nil, fmt.Errorf("assemble: 构造注册中心: %w", err)
	}
	if err := reg.Register(ctx, instanceOf(nodeID, urls)); err != nil {
		return nil, fmt.Errorf("assemble: 注册服务实例: %w", err)
	}
	return reg, nil
}

// instanceOf 构造注册实例（etcd 注册端点带 grpc:// scheme，供发现方解析拨号）。
func instanceOf(nodeID string, urls serverURLs) *registry.ServiceInstance {
	return &registry.ServiceInstance{
		ID:        nodeID,
		Name:      "game",
		Version:   ServiceVersion,
		Endpoints: []string{"grpc://" + urls.grpc},
	}
}

// newBootstrap 把进程内装配参数映射为服务配置：
// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 监听地址固定随机端口（进程内形态不做端口管理）。
func newBootstrap(o Options) *conf.Bootstrap {
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "game", Id: o.NodeID},
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
			Mongo: &configspb.Data_Mongo{Uri: o.MongoURI, Database: o.MongoDB},
		},
	}
}
