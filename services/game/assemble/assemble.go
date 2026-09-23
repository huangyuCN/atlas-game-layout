// Package assemble 提供 game 服务的进程内（嵌入式）装配入口：
// 与进程形态共用 internal/app 的同一张 fx 依赖图与同一套启停路径
// （bootstrap.Boot → atlas.App：启动服务端、注册实例、注销与停机），
// 仅不注册进程信号——宿主/测试进程的信号不能被本实例拦下。
// 供集成测试与嵌入式部署复用，装配逻辑只此一份、不再手工重复接线。
package assemble

import (
	"context"

	"github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	gameapp "github.com/huangyuCN/atlas-game-layout/services/game/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"go.uber.org/fx"
)

// Options 是进程内装配参数。
type Options struct {
	NodeID        string
	EtcdEndpoints []string
	NatsURL       string
	RedisAddrs    []string
	MongoURI      string
	MongoDB       string
	// Namespace 是注册中心键前缀（可选）：嵌入式/测试形态用它把实例与常驻进程隔离，
	// 并避免上一次运行残留的实例键（租约未过期）导致注册冲突；
	// 缺省按 runtime.env 派生（/atlas/services/<env>）。
	Namespace string
}

// Game 是装配完成的 game 服务句柄。
type Game struct {
	Runtime *actor.Runtime
	GRPCURL string // host:port（gRPC 直连用）
	HTTPURL string // host:port（健康检查 + 玩家 REST 管理接口）
	stop    func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件。
type graphHandles struct {
	fx.In

	bootstrap.Servers
	Runtime *actor.Runtime
}

// New 装配并启动 game 服务：映射配置 → bootstrap.Boot 启动依赖图（含 actor 运行时），
// 由 atlas.App 统一启动服务端并注册实例（返回时注册已完成）。
// 实例 ID 必须 == actor NodeID（集群懒激活按服务实例选节点的硬约束），两者同取自 runtime.id。
func New(ctx context.Context, o Options) (*Game, error) {
	var h graphHandles
	cfg, err := newBootstrap(o)
	if err != nil {
		return nil, err
	}
	inst, urls, err := bootstrap.Boot(ctx, cfg, gameapp.Module, &h, serverutil.SchemeGRPC, serverutil.SchemeHTTP)
	if err != nil {
		return nil, err
	}
	return &Game{
		Runtime: h.Runtime,
		GRPCURL: urls[serverutil.SchemeGRPC].Host,
		HTTPURL: urls[serverutil.SchemeHTTP].Host,
		stop:    inst.Stop,
	}, nil
}

// Stop 停止 game 服务并释放全部资源
// （注销实例 → 停服务端 → 组件逆序回收，均由 atlas.App 驱动）。
func (g *Game) Stop(ctx context.Context) error {
	if g.stop == nil {
		return nil
	}
	return g.stop(ctx)
}

// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 监听地址固定随机端口（进程内形态不做端口管理）。
// newBootstrap 合成进程内形态配置；返回 error 的唯一来源是 actor 命名空间派生非法。
func newBootstrap(o Options) (*conf.Bootstrap, error) {
	actorNS, err := bootstrap.ActorNamespaceOf(o.Namespace)
	if err != nil {
		return nil, err
	}
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "game", Id: o.NodeID, ActorNamespace: actorNS},
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
			Mongo: &configspb.Data_Mongo{Uri: o.MongoURI, Database: o.MongoDB},
		},
	}, nil
}
