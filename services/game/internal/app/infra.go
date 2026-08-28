package app

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
)

// NewRedisClient 装配 redis 客户端（会话令牌 + 玩家快照缓存）。
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

// NewNatsConn 装配 NATS 连接（actor 集群传输）。
func NewNatsConn(cfg *conf.Bootstrap) (*natsgo.Conn, error) {
	url := ""
	name := consts.ServiceGame
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

// NewMongoClient 装配 MongoDB 客户端（玩家持久化）。
// fx 无法注入裸 context.Context，这里以进程级上下文构造
// （仅索引初始化等启动期使用）。
func NewMongoClient(cfg *conf.Bootstrap) (*mongo.Client, error) {
	opts := mongo.Options{}
	if d := cfg.GetData(); d != nil && d.GetMongo() != nil {
		opts.URI = d.GetMongo().GetUri()
		opts.Database = d.GetMongo().GetDatabase()
	}
	cli, err := mongo.NewClient(context.Background(), opts)
	if err != nil {
		return nil, fmt.Errorf("app: 构造 mongo 客户端失败: %w", err)
	}
	return cli, nil
}

// NewActorRuntime 装配 actor 集群运行时
// （Locator=etcd、传输=NATS、懒激活按服务发现选 game 节点）。
func NewActorRuntime(cfg *conf.Bootstrap, ec *clientv3.Client) (*pkgactor.Runtime, error) {
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
		ServiceName:   consts.ServiceGame,
		EtcdEndpoints: endpoints,
		NatsURL:       natsURLOf(cfg),
		Discovery:     discovery,
	})
	if err != nil {
		return nil, fmt.Errorf("app: 构造 actor 运行时失败: %w", err)
	}
	return rt, nil
}

// natsURLOf 提取 data.nats.url。
func natsURLOf(cfg *conf.Bootstrap) string {
	if d := cfg.GetData(); d != nil && d.GetNats() != nil {
		return d.GetNats().GetUrl()
	}
	return ""
}
