// Package assemble 提供 gateway 服务的进程内（嵌入式）装配入口：
// 与生产形态共用 internal/app 的同一张 fx 依赖图，仅驱动方式不同
// （进程形态由 atlas.App 管信号与启停；本形态以 fx 编程式 Start/Stop
// + serverutil.ServeAsync 驱动），供集成测试与嵌入式部署复用。
//
// 单通道形态差异：生产形态 WS Server 独立监听；嵌入式形态把
// WS Handler 挂载在 httptest 的 /ws 路径下（与既有 e2e 约定一致），
// 因此依赖图的 embed_servers 子组不包含 WS。
package assemble

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	gwapp "github.com/huangyuCN/atlas-game-layout/services/gateway/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas/transport"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	"go.uber.org/fx"
)

// Options 是进程内装配参数。
type Options struct {
	ID            string
	EtcdEndpoints []string
	NatsURL       string
	RedisAddr     string
}

// Gateway 是装配完成的 gateway 实例句柄。
type Gateway struct {
	TCPURL  string              // 业务通道（tcp，host:port）
	WSURL   string              // 单通道形态（ws://host:port/ws，业务+战斗共用）
	KCPURL  string              // 战斗通道（kcp，host:port）
	UDPURL  string              // 战斗通道（udp，host:port）
	HTTPURL string              // 健康检查（http，host:port）
	Actors  *actorclient.Client // 远程 actor 客户端（测试/观测用）
	stop    func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件
// （WSSrv 具体类型用于 WS 的 /ws 包装；embed_servers 为四台可独立启停的子组）。
type graphHandles struct {
	fx.In

	Actors  *actorclient.Client
	WSSrv   *wst.Server
	Servers []transport.Server `group:"embed_servers"`
}

// New 装配并启动一个 gateway 实例：映射配置 → 启动 fx 依赖图
// （含推送订阅与会话清扫生命周期）→ 后台起四协议传输层 →
// 以 /ws 路径包装 WS 单通道。
func New(ctx context.Context, o Options) (*Gateway, error) {
	var h graphHandles
	root := fx.New(
		fx.NopLogger,
		fx.Supply(newBootstrap(o)),
		gwapp.Module,
		fx.Populate(&h),
	)
	if err := root.Err(); err != nil {
		return nil, fmt.Errorf("assemble: 依赖图校验失败: %w", err)
	}
	if err := root.Start(ctx); err != nil {
		return nil, fmt.Errorf("assemble: 启动组件失败: %w", err)
	}

	urls, stopServers, err := startServers(h.Servers)
	if err != nil {
		_ = root.Stop(context.Background())
		return nil, err
	}
	wsHTTP, wsURL, err := wrapWSHandler(h.WSSrv)
	if err != nil {
		_ = stopServers(context.Background())
		_ = root.Stop(context.Background())
		return nil, err
	}

	g := &Gateway{
		TCPURL:  urls["tcp"],
		WSURL:   wsURL,
		KCPURL:  urls["kcp"],
		UDPURL:  urls["udp"],
		HTTPURL: urls["http"],
		Actors:  h.Actors,
	}
	g.stop = func(ctx context.Context) error {
		wsHTTP.Close()
		// 某个协议服务停止失败不应阻断后续清理：仍需停 actor/relay 并关闭外部资源。
		sErr := stopServers(ctx)
		rErr := root.Stop(ctx)
		if sErr != nil {
			return sErr
		}
		return rErr
	}
	return g, nil
}

// Stop 停止 gateway 实例并释放全部资源（停 WS 包装 → 停四协议 → 组件逆序回收）。
func (g *Gateway) Stop(ctx context.Context) error {
	if g.stop == nil {
		return nil
	}
	return g.stop(ctx)
}

// startServers 以进程内形态后台启动嵌入式传输层子组，
// 并按 scheme 归集端点（fx 值组不保证提供顺序，不能依赖下标）。
func startServers(servers []transport.Server) (map[string]string, func(context.Context) error, error) {
	eps, stop, err := serverutil.ServeAsync(5*time.Second, servers...)
	if err != nil {
		return nil, nil, fmt.Errorf("assemble: 启动传输层: %w", err)
	}
	urls := make(map[string]string, len(eps))
	for _, ep := range eps {
		urls[ep.Scheme] = ep.Host
	}
	for _, scheme := range []string{"tcp", "kcp", "udp", "http"} {
		if urls[scheme] == "" {
			_ = stop(context.Background())
			return nil, nil, fmt.Errorf("assemble: 缺少 %s 端点: %v", scheme, eps)
		}
	}
	return urls, stop, nil
}

// wrapWSHandler 把 WS Server 的 Handler 挂载到 httptest 的 /ws 路径
// （单通道形态约定：客户端拨 ws://<host>/ws）。
func wrapWSHandler(wsSrv *wst.Server) (*httptest.Server, string, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsSrv.Handler())
	wsHTTP := httptest.NewServer(mux)
	return wsHTTP, "ws" + strings.TrimPrefix(wsHTTP.URL, "http") + "/ws", nil
}

// newBootstrap 把进程内装配参数映射为服务配置：
// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 实例 ID 加 gw- 前缀（会话路由与跨实例踢人依赖该约定）；
// 监听地址固定随机端口（进程内形态不做端口管理）。
func newBootstrap(o Options) *conf.Bootstrap {
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "gateway", Id: "gw-" + o.ID},
		Registry: &configspb.Registry{
			Etcd: &configspb.Registry_Etcd{Endpoints: o.EtcdEndpoints},
		},
		Server: &configspb.Server{
			Grpc: &configspb.Server_GRPC{Addr: randomPort},
			Http: &configspb.Server_HTTP{Addr: randomPort},
		},
		Tcp:       &conf.Bootstrap_Net{Addr: randomPort},
		Websocket: &conf.Bootstrap_Net{Addr: randomPort},
		Kcp:       &conf.Bootstrap_Net{Addr: randomPort},
		Udp:       &conf.Bootstrap_Net{Addr: randomPort},
		Data: &configspb.Data{
			Redis: &configspb.Data_Redis{Addr: o.RedisAddr},
			Nats:  &configspb.Data_Nats{Url: o.NatsURL},
		},
	}
}
