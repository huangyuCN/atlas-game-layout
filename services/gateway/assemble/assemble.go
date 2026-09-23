// Package assemble 提供 gateway 服务的进程内（嵌入式）装配入口：
// 与进程形态共用 internal/app 的同一张 fx 依赖图与同一套启停路径
// （bootstrap.Boot → atlas.App：启动五协议服务端、注册实例、注销与停机），
// 仅不注册进程信号——宿主/测试进程的信号不能被本实例拦下。
// WS 与进程形态一致：由 WS Server 独立监听，端点取自身 Endpoint()
// （WS Handler 对路径不敏感，客户端拨 ws://host:port 即可）。
package assemble

import (
	"context"

	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	gwapp "github.com/huangyuCN/atlas-game-layout/services/gateway/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"go.uber.org/fx"
)

// Options 是进程内装配参数。
type Options struct {
	ID            string
	EtcdEndpoints []string
	NatsURL       string
	RedisAddrs    []string
	// Namespace 是注册中心键前缀（可选）：嵌入式/测试形态用它把实例与常驻进程隔离，
	// 并避免上一次运行残留的实例键（租约未过期）导致注册冲突；
	// 缺省按 runtime.env 派生（/atlas/services/<env>）。
	Namespace string
}

// Gateway 是装配完成的 gateway 实例句柄。
type Gateway struct {
	TCPURL  string              // 业务通道（tcp，host:port）
	WSURL   string              // 单通道形态（ws://host:port，业务+战斗共用）
	KCPURL  string              // 战斗通道（kcp，host:port）
	UDPURL  string              // 战斗通道（udp，host:port）
	HTTPURL string              // 健康检查（http，host:port）
	Actors  *actorclient.Client // 远程 actor 客户端（测试/观测用）
	stop    func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件。
type graphHandles struct {
	fx.In

	bootstrap.Servers
	Actors *actorclient.Client
}

// New 装配并启动一个 gateway 实例：映射配置 → bootstrap.Boot 启动依赖图
// （含推送订阅、会话清扫与 actor 客户端），由 atlas.App 统一启动五协议服务端并注册实例。
func New(ctx context.Context, o Options) (*Gateway, error) {
	var h graphHandles
	cfg, err := newBootstrap(o)
	if err != nil {
		return nil, err
	}
	inst, urls, err := bootstrap.Boot(ctx, cfg, gwapp.Module, &h,
		serverutil.SchemeTCP, serverutil.SchemeWS, serverutil.SchemeKCP,
		serverutil.SchemeUDP, serverutil.SchemeHTTP)
	if err != nil {
		return nil, err
	}
	return &Gateway{
		TCPURL:  urls[serverutil.SchemeTCP].Host,
		WSURL:   urls[serverutil.SchemeWS].String(),
		KCPURL:  urls[serverutil.SchemeKCP].Host,
		UDPURL:  urls[serverutil.SchemeUDP].Host,
		HTTPURL: urls[serverutil.SchemeHTTP].Host,
		Actors:  h.Actors,
		stop:    inst.Stop,
	}, nil
}

// Stop 停止 gateway 实例并释放全部资源
// （注销实例 → 停五协议服务端 → 组件逆序回收，均由 atlas.App 驱动）。
func (g *Gateway) Stop(ctx context.Context) error {
	if g.stop == nil {
		return nil
	}
	return g.stop(ctx)
}

// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 实例 ID 加 gw- 前缀（会话路由与跨实例踢人依赖该约定）；
// 监听地址固定随机端口（进程内形态不做端口管理）。
// newBootstrap 合成进程内形态配置；返回 error 的唯一来源是 actor 命名空间派生非法。
func newBootstrap(o Options) (*conf.Bootstrap, error) {
	actorNS, err := bootstrap.ActorNamespaceOf(o.Namespace)
	if err != nil {
		return nil, err
	}
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "gateway", Id: "gw-" + o.ID, ActorNamespace: actorNS},
		Registry: &configspb.Registry{
			Etcd:      &configspb.Registry_Etcd{Endpoints: o.EtcdEndpoints},
			Namespace: o.Namespace,
		},
		Server: &configspb.Server{
			Grpc:      &configspb.Server_GRPC{Addr: randomPort},
			Http:      &configspb.Server_HTTP{Addr: randomPort},
			Tcp:       &configspb.Server_TCP{Addr: randomPort},
			Websocket: &configspb.Server_WebSocket{Addr: randomPort},
			Kcp:       &configspb.Server_KCP{Addr: randomPort},
			Udp:       &configspb.Server_UDP{Addr: randomPort},
		},
		Data: &configspb.Data{
			Redis: &configspb.Data_Redis{Addrs: o.RedisAddrs},
			Nats:  &configspb.Data_Nats{Url: o.NatsURL},
		},
	}, nil
}
