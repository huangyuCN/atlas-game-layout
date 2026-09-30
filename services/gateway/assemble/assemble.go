// Package assemble 提供 gateway 服务的进程内（嵌入式）装配入口：
// 与进程形态共用 internal/app 的同一张 fx 依赖图与同一套启停路径
// （bootstrap.Boot → atlas.App：启动业务协议服务端、注册实例、注销与停机），
// 仅不注册进程信号——宿主/测试进程的信号不能被本实例拦下。
// WS 与进程形态一致：由 WS Server 独立监听，端点取自身 Endpoint()
// （WS Handler 对路径不敏感，客户端拨 ws://host:port 即可）。
package assemble

import (
	"context"
	"net/url"
	"slices"
	"sort"

	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
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
	// Namespace 是命名空间 token（如 e2e-1790，**不是**注册键前缀路径）：嵌入式/测试形态用它
	// 把实例与常驻进程隔离，并避免上一次运行残留的实例键（租约未过期）导致注册冲突。
	// 五面前缀（注册键/actor subject/业务 topic/redis 键/etcd 目录）全由框架 namespace.Derive
	// 按该 token 派生；**必填**，缺失即装配失败（R9：不回落 default/env）。
	Namespace string
	// MinClientVersion/MinClientVersionMode 是 M1 客户端版本门槛（可选）：
	// 缺省 0 值 = 空门槛 + OFF（不校验）；集成测试用 ENFORCE/NEGOTIATE 形态验证门槛链路。
	MinClientVersion     string
	MinClientVersionMode configspb.MinClientVersionMode
}

// Gateway 是装配完成的 gateway 实例句柄。
type Gateway struct {
	TCPURL  string // 业务通道（tcp，host:port）
	WSURL   string // 业务通道（ws://host:port）
	HTTPURL string // 健康检查（http，host:port）
	// schemes 是本实例实际监听的端点 scheme 集合（升序）：业务面 + 健康面，
	// **不含** kcp/udp（战斗帧面不在网关，见 ListenSchemes）。
	schemes []string
	stop    func(ctx context.Context) error
}

// ListenSchemes 返回本实例实际监听的端点 scheme（升序副本）。
// 破坏性断言与运维核对用：战斗帧面（kcp/udp）必须不在其中。
func (g *Gateway) ListenSchemes() []string {
	return slices.Clone(g.schemes)
}

// graphHandles 从依赖图回捞句柄所需组件。
type graphHandles struct {
	fx.In

	bootstrap.Servers
}

// New 装配并启动一个 gateway 实例：映射配置 → bootstrap.Boot 启动依赖图
// （含推送订阅、会话清扫与域服务客户端），由 atlas.App 统一启动业务协议服务端并注册实例。
func New(ctx context.Context, o Options) (*Gateway, error) {
	var h graphHandles
	cfg, err := newBootstrap(o)
	if err != nil {
		return nil, err
	}
	// 只要求业务协议与健康/edge 面就绪：战斗帧面（kcp/udp）自阶段 3 批次 5 起不在网关，
	// 故既不声明也不等待——少一个 scheme 就是"没监听"，不需要额外的运行时判空。
	inst, urls, err := bootstrap.Boot(ctx, cfg, gwapp.Module, &h,
		serverutil.SchemeTCP, serverutil.SchemeWS,
		serverutil.SchemeHTTP, serverutil.SchemeGRPCEdge)
	if err != nil {
		return nil, err
	}
	return &Gateway{
		TCPURL:  urls[serverutil.SchemeTCP].Host,
		WSURL:   urls[serverutil.SchemeWS].String(),
		HTTPURL: urls[serverutil.SchemeHTTP].Host,
		schemes: sortedSchemes(urls),
		stop:    inst.Stop,
	}, nil
}

// sortedSchemes 归集就绪端点 scheme（升序；供破坏性断言与运维核对"到底监听了哪几面"）。
func sortedSchemes(urls map[string]*url.URL) []string {
	schemes := make([]string, 0, len(urls))
	for scheme, u := range urls {
		if u != nil {
			schemes = append(schemes, scheme)
		}
	}
	sort.Strings(schemes)
	return schemes
}

// Stop 停止 gateway 实例并释放全部资源
// （注销实例 → 停业务协议服务端 → 组件逆序回收，均由 atlas.App 驱动）。
func (g *Gateway) Stop(ctx context.Context) error {
	if g.stop == nil {
		return nil
	}
	return g.stop(ctx)
}

// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 实例 ID 加 gw- 前缀（会话路由与跨实例踢人依赖该约定）；
// 监听地址固定随机端口（进程内形态不做端口管理）。
// newBootstrap 合成进程内形态配置；返回 error 的唯一来源是命名空间缺失/非法（R9）。
func newBootstrap(o Options) (*conf.Bootstrap, error) {
	// R9 严格模式：命名空间缺失/非法即装配失败（不回落 default/env）。
	if err := bootstrap.RequireNamespace(o.Namespace); err != nil {
		return nil, err
	}
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{
			Name: "gateway", Id: "gw-" + o.ID, Namespace: o.Namespace,
			MinClientVersion:     o.MinClientVersion,
			MinClientVersionMode: o.MinClientVersionMode,
		},
		Registry: &configspb.Registry{
			Etcd: &configspb.Registry_Etcd{Endpoints: o.EtcdEndpoints},
		},
		Server: &configspb.Server{
			// gRPC 只启用 edge 面（本轮暂不注册域服务）；internal 面留空 = 不启用（P7 管理面再启用）。
			Grpc:      &configspb.Server_GRPC{EdgeAddr: randomPort},
			Http:      &configspb.Server_HTTP{Addr: randomPort},
			Tcp:       &configspb.Server_TCP{Addr: randomPort},
			Websocket: &configspb.Server_WebSocket{Addr: randomPort},
			// 战斗帧面（kcp/udp）**刻意不配**：网关不再监听它们（阶段 3 批次 5）。
		},
		Data: &configspb.Data{
			Redis: &configspb.Data_Redis{Addrs: o.RedisAddrs},
			Nats:  &configspb.Data_Nats{Url: o.NatsURL},
		},
	}, nil
}
