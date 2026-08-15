// Package server 负责 gateway 服务的传输层组装与统一 handler：
// 五协议 Server（tcp/ws/kcp/udp/http）+ 会话绑定 + 挤下线 + 下行推送。
package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/transport"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// sessionTTL 是会话路由的默认租期（心跳续租周期）。
const sessionTTL = 30 * time.Second

// NewHTTPServer 构造 HTTP 服务端（健康检查）。
func NewHTTPServer(cfg *conf.Bootstrap) (transport.Server, error) {
	srv, err := atlashttp.NewServer(atlashttp.WithAddress(cfg.GetServer().GetHttp().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 HTTP 服务端失败: %w", err)
	}
	srv.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"gateway"}`))
	})
	return srv, nil
}

// newRedisClient 装配 redis 客户端（会话路由表后端）。
func newRedisClient(cfg *conf.Bootstrap) (*pkredis.Client, error) {
	opts := pkredis.Options{}
	if d := cfg.GetData(); d != nil && d.GetRedis() != nil {
		opts.Addr = d.GetRedis().GetAddr()
	}
	return pkredis.NewClient(opts)
}

// newNatsConn 装配 NATS 连接（推送订阅 + 控制通道）。
func newNatsConn(cfg *conf.Bootstrap) (*natsgo.Conn, error) {
	url := ""
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		url = d.GetNats().GetUrl()
	}
	return nats.Connect(nats.Options{URL: url, Name: "gateway"})
}

// newSessionManager 装配分布式会话管理器。
func newSessionManager(cfg *conf.Bootstrap, cli *pkredis.Client) *session.Manager {
	instanceID := ""
	if r := cfg.GetRuntime(); r != nil {
		instanceID = r.GetId()
	}
	return session.NewManager(session.NewRedisStore(cli), instanceID, sessionTTL)
}

// newActorClient 装配远程 actor 客户端（集群运行时：Locator=etcd、NATS 传输，
// 懒激活选节点经 game 服务发现）。
func newActorClient(cfg *conf.Bootstrap, ec *clientv3.Client) (*actorclient.Client, error) {
	var endpoints []string
	if r := cfg.GetRegistry(); r != nil && r.GetEtcd() != nil {
		endpoints = r.GetEtcd().GetEndpoints()
	}
	natsURL := ""
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		natsURL = d.GetNats().GetUrl()
	}
	nodeID := ""
	if r := cfg.GetRuntime(); r != nil {
		nodeID = r.GetId()
	}
	discovery, err := pkgregistry.NewEtcdDiscovery(ec, pkgregistry.Options{})
	if err != nil {
		return nil, fmt.Errorf("server: 构造服务发现失败: %w", err)
	}
	rt, err := pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        nodeID,
		ServiceName:   consts.ServiceGame, // 懒激活在 game 节点执行（PlayerActor 宿主）
		EtcdEndpoints: endpoints,
		NatsURL:       natsURL,
		Discovery:     discovery,
	})
	if err != nil {
		return nil, fmt.Errorf("server: 构造 actor 集群运行时失败: %w", err)
	}
	if err := pkgactor.RegisterPlayerReplica(rt); err != nil {
		return nil, fmt.Errorf("server: 注册 PlayerActor 懒激活副本失败: %w", err)
	}
	return actorclient.NewClient(rt), nil
}

// registerActorLifecycle 把 actor 集群运行时接入生命周期。
func registerActorLifecycle(lc fx.Lifecycle, actors *actorclient.Client) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return actors.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return actors.Shutdown(ctx) },
	})
}

// newGateway 装配统一 handler 并注册到各协议 Server。
func newGateway(
	cfg *conf.Bootstrap,
	sess *session.Manager,
	actors *actorclient.Client,
	nc *natsgo.Conn,
	tcpSrv *tcpt.Server, wsSrv *wst.Server, kcpSrv *kcpt.Server, udpSrv *udpt.Server,
) (*Gateway, error) {
	instanceID := ""
	if r := cfg.GetRuntime(); r != nil {
		instanceID = r.GetId()
	}
	g := NewGateway(instanceID, sess, actors, nc, tcpSrv, wsSrv, kcpSrv, udpSrv)
	if err := RegisterGatewayHandlers(tcpSrv, wsSrv, kcpSrv, udpSrv, g); err != nil {
		return nil, err
	}
	return g, nil
}

// Module 是 gateway 服务的传输层装配模块（五协议 + 会话 + 推送 + actor 客户端）。
var Module = fx.Module("server",
	fx.Provide(
		fx.Annotate(NewHTTPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(NewTCPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(NewWSServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(NewKCPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(NewUDPServer, fx.ResultTags(`group:"servers"`)),
		newRedisClient,
		newNatsConn,
		newSessionManager,
		newActorClient,
		newGateway,
	),
	fx.Invoke(registerRelay, registerActorLifecycle),
)
