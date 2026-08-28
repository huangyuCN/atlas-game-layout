package app

import (
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// sessionTTL 是会话路由的默认租期（心跳续租周期，与 game 会话租期对齐）。
const sessionTTL = 30 * time.Second

// NewRedisClient 装配 redis 客户端（会话路由表后端）。
func NewRedisClient(cfg *conf.Bootstrap) (*pkredis.Client, error) {
	opts := pkredis.Options{}
	if d := cfg.GetData(); d != nil && d.GetRedis() != nil {
		opts.Addr = d.GetRedis().GetAddr()
	}
	cli, err := pkredis.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("app: 构造 redis 客户端失败: %w", err)
	}
	return cli, nil
}

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

// newSessionManager 装配分布式会话管理器。
func newSessionManager(cfg *conf.Bootstrap, cli *pkredis.Client) *session.Manager {
	instanceID := ""
	if r := cfg.GetRuntime(); r != nil {
		instanceID = r.GetId()
	}
	return session.NewManager(session.NewRedisStore(cli), instanceID, sessionTTL)
}

// NewActorClient 装配远程 actor 客户端背后的集群运行时
// （Locator=etcd、NATS 传输；PlayerActor 懒激活在 game 节点执行，
// 本节点注册「只发不接」副本以支持发送侧判定）。
func NewActorClient(cfg *conf.Bootstrap, ec *clientv3.Client) (*actorclient.Client, error) {
	var endpoints []string
	if r := cfg.GetRegistry(); r != nil && r.GetEtcd() != nil {
		endpoints = r.GetEtcd().GetEndpoints()
	}
	nodeID := ""
	if r := cfg.GetRuntime(); r != nil {
		nodeID = r.GetId()
	}
	discovery, err := pkgregistry.NewEtcdDiscovery(ec, pkgregistry.Options{})
	if err != nil {
		return nil, fmt.Errorf("app: 构造服务发现失败: %w", err)
	}
	rt, err := pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        nodeID,
		ServiceName:   consts.ServiceGame, // 懒激活在 game 节点执行（PlayerActor 宿主）
		EtcdEndpoints: endpoints,
		NatsURL:       natsURLOf(cfg),
		Discovery:     discovery,
	})
	if err != nil {
		return nil, fmt.Errorf("app: 构造 actor 集群运行时失败: %w", err)
	}
	if err := pkgactor.RegisterPlayerReplica(rt); err != nil {
		return nil, fmt.Errorf("app: 注册 PlayerActor 懒激活副本失败: %w", err)
	}
	return actorclient.NewClient(rt), nil
}

// natsURLOf 提取 data.nats.url。
func natsURLOf(cfg *conf.Bootstrap) string {
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		return d.GetNats().GetUrl()
	}
	return ""
}
