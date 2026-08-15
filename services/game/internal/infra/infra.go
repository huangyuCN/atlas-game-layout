// Package infra 提供 game 服务的外部依赖装配：
// redis（会话 + 玩家快照缓存）、mongo（玩家持久化）、nats/etcd（actor 集群）。
package infra

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// NewRedisClient 装配 redis 客户端（会话令牌 + 玩家快照缓存）。
func NewRedisClient(cfg *conf.Bootstrap) (*pkredis.Client, error) {
	opts := pkredis.Options{}
	if d := cfg.GetData(); d != nil && d.GetRedis() != nil {
		opts.Addr = d.GetRedis().GetAddr()
	}
	return pkredis.NewClient(opts)
}

// NewNatsConn 装配 NATS 连接（actor 集群传输）。
func NewNatsConn(cfg *conf.Bootstrap) (*natsgo.Conn, error) {
	url := ""
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		url = d.GetNats().GetUrl()
	}
	return nats.Connect(nats.Options{URL: url, Name: "game"})
}

// NewMongoClient 装配 MongoDB 客户端（玩家持久化）。
func NewMongoClient(cfg *conf.Bootstrap) (*mongo.Client, error) {
	opts := mongo.Options{}
	if d := cfg.GetData(); d != nil && d.GetMongo() != nil {
		opts.URI = d.GetMongo().GetUri()
		opts.Database = d.GetMongo().GetDatabase()
	}
	return mongo.NewClient(context.Background(), opts)
}

// NewActorRuntime 装配 actor 集群运行时（Locator=etcd、传输=NATS、懒激活选 game 节点）。
func NewActorRuntime(cfg *conf.Bootstrap, ec *clientv3.Client) (*pkgactor.Runtime, error) {
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
		return nil, fmt.Errorf("infra: 构造服务发现失败: %w", err)
	}
	return pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        nodeID,
		ServiceName:   consts.ServiceGame,
		EtcdEndpoints: endpoints,
		NatsURL:       natsURL,
		Discovery:     discovery,
	})
}

// RegisterRuntimeLifecycle 把集群运行时接入 fx 生命周期。
func RegisterRuntimeLifecycle(lc fx.Lifecycle, rt *pkgactor.Runtime) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return rt.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return rt.Shutdown(ctx) },
	})
}

// 快照 TTL 与心跳等时间参数集中声明（biz 层引用）。
