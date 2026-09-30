// Package assemble 提供 edge 服务的进程内（嵌入式）装配入口：
// 与进程形态共用 internal/app 的同一张 fx 依赖图与同一套启停路径
// （bootstrap.Boot → atlas.App：启动各接入面与健康服务端，停机时先停新连接、再拆流），
// 仅不注册进程信号——宿主/测试进程的信号不能被本实例拦下。
//
// 接入层不注册实例（客户端地址由 battle 的 edge.endpoint 统一下发），故本形态也不需要注册器。
package assemble

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	edgeapp "github.com/huangyuCN/atlas-game-layout/services/edge/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	"go.uber.org/fx"
)

// ticketKeyLen 是入场票据 AEAD 密钥的字节数（AES-256；与进程形态同一校验口径）。
const ticketKeyLen = 32

// Options 是进程内装配参数。
type Options struct {
	// Namespace 是命名空间 token（如 e2e-1790，**不是**注册键前缀路径）：actor 目录前缀
	// /atlas/actors/<ns> 与注册键前缀由它派生。**必填**，缺失即装配失败（R9：不回落默认值）。
	Namespace string
	// EtcdEndpoints 是注册中心/目录的 etcd 端点（进程内形态仍需能构造客户端，惰性连接）。
	EtcdEndpoints []string
	// Listeners 是接入面（空 = 三面随机端口：ws(tcp)/kcp(udp)/udp(udp)，供测试用）。
	Listeners []edge.Listener
	// TicketKey 是 32 字节票据密钥（必填，必须与 battle 侧同值）。
	TicketKey []byte
	// Resolver 是可选注入点：非空时用它覆盖「actor 目录 + 帧面实例发现」的默认装配
	//（测试与嵌入式部署用；nil = 走 etcd 目录 + 注册中心发现）。
	Resolver edge.Resolver
	// MaxStreams/NewConnRate/MaxPerIP 是资源保护参数（0 = 框架缺省/不限制）。
	MaxStreams  int
	NewConnRate int
	MaxPerIP    int
	// IdleTimeout 是空闲回收时长（0 = 框架缺省 60s）。
	IdleTimeout time.Duration
}

// Edge 是装配完成的接入层实例句柄。
type Edge struct {
	// Proxy 是接入层本体（驱动方读活跃流数等运行态；如迁移用例断言拆流）。
	Proxy *edge.Proxy
	// ListenerAddrs 是各接入面实际监听地址（与生效的接入面同序）。
	ListenerAddrs []string
	// HTTPURL 是健康检查地址（host:port）。
	HTTPURL string
	stop    func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件。
type graphHandles struct {
	fx.In

	bootstrap.Servers
	Proxy *edge.Proxy
}

// New 装配并启动一个接入层实例：映射配置 → bootstrap.Boot 启动依赖图
// （接入面监听 + HTTP 健康，由 atlas.App 统一启停）。
func New(ctx context.Context, o Options) (*Edge, error) {
	var h graphHandles
	cfg, err := newBootstrap(o)
	if err != nil {
		return nil, err
	}
	opts := []fx.Option{edgeapp.Module}
	if o.Resolver != nil {
		// 注入覆盖：解析器换成宿主提供的实现（目录/注册中心仍按配置构造，惰性连接）。
		opts = append(opts, fx.Decorate(func(edge.Resolver) edge.Resolver { return o.Resolver }))
	}
	inst, urls, err := bootstrap.Boot(ctx, cfg, fx.Options(opts...), &h, serverutil.SchemeHTTP)
	if err != nil {
		return nil, err
	}
	// 接入面监听在 App 的服务端 goroutine 里完成：等它就绪再读实际地址
	//（进程内形态用随机端口，调用方拿到的是内核分配后的真实地址）。
	if err := h.Proxy.WaitReady(ctx); err != nil {
		_ = inst.Stop(ctx)
		return nil, fmt.Errorf("assemble: 等待接入层就绪失败: %w", err)
	}
	return &Edge{
		Proxy:         h.Proxy,
		ListenerAddrs: h.Proxy.ListenerAddrs(),
		HTTPURL:       urls[serverutil.SchemeHTTP].Host,
		stop:          inst.Stop,
	}, nil
}

// Stop 停止接入层实例（先停新连接、再拆流，由 atlas.App 驱动）。
func (e *Edge) Stop(ctx context.Context) error {
	if e.stop == nil {
		return nil
	}
	return e.stop(ctx)
}

// newBootstrap 合成进程内形态配置；监听地址固定随机端口（进程内形态不做端口管理）。
func newBootstrap(o Options) (*conf.Bootstrap, error) {
	if err := bootstrap.RequireNamespace(o.Namespace); err != nil {
		return nil, err
	}
	if len(o.TicketKey) != ticketKeyLen {
		return nil, fmt.Errorf("%w: 进程内装配的 TicketKey 必须 %d 字节，实际 %d",
			ticket.ErrKeySize, ticketKeyLen, len(o.TicketKey))
	}
	listeners := o.Listeners
	if len(listeners) == 0 {
		listeners = defaultListeners()
	}
	ec := &conf.Edge{
		TicketKey:   base64.StdEncoding.EncodeToString(o.TicketKey),
		MaxStreams:  int32(o.MaxStreams),
		NewConnRate: int32(o.NewConnRate),
		MaxPerIp:    int32(o.MaxPerIP),
	}
	if o.IdleTimeout > 0 {
		ec.IdleTimeout = o.IdleTimeout.String()
	}
	for _, l := range listeners {
		face, err := listenerProtoOf(l)
		if err != nil {
			return nil, err
		}
		ec.Listeners = append(ec.Listeners, face)
	}
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime:  &configspb.Runtime{Name: "edge", Namespace: o.Namespace},
		Registry: &configspb.Registry{Etcd: &configspb.Registry_Etcd{Endpoints: o.EtcdEndpoints}},
		Server:   &configspb.Server{Http: &configspb.Server_HTTP{Addr: randomPort}},
		Edge:     ec,
	}, nil
}

// defaultListeners 返回三面随机端口的默认接入面（ws/kcp/udp）。
func defaultListeners() []edge.Listener {
	return []edge.Listener{
		{Name: "ws", Network: edge.NetworkTCP, Address: "127.0.0.1:0", Carrier: edge.CarrierWSUpgrade},
		{Name: "kcp", Network: edge.NetworkUDP, Address: "127.0.0.1:0", Carrier: edge.CarrierDatagram},
		{Name: "udp", Network: edge.NetworkUDP, Address: "127.0.0.1:0", Carrier: edge.CarrierDatagram},
	}
}

// listenerProtoOf 把框架接入面反映射为配置消息（枚举与框架取值一一对应）。
func listenerProtoOf(l edge.Listener) (*conf.Edge_Listener, error) {
	var network conf.Edge_Network
	switch l.Network {
	case edge.NetworkTCP:
		network = conf.Edge_NETWORK_TCP
	case edge.NetworkUDP:
		network = conf.Edge_NETWORK_UDP
	default:
		return nil, fmt.Errorf("assemble: 不支持的接入面网络 %q", l.Network)
	}
	var carrier conf.Edge_Carrier
	switch l.Carrier {
	case edge.CarrierWSUpgrade:
		carrier = conf.Edge_CARRIER_WS_UPGRADE
	case edge.CarrierStreamHello:
		carrier = conf.Edge_CARRIER_STREAM_HELLO
	case edge.CarrierDatagram:
		carrier = conf.Edge_CARRIER_DATAGRAM
	default:
		return nil, fmt.Errorf("assemble: 不支持的接入面载体 %q", l.Carrier)
	}
	return &conf.Edge_Listener{Name: l.Name, Network: network, Addr: l.Address, Carrier: carrier}, nil
}
