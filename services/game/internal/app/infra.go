package app

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/registry"
	natsgo "github.com/nats-io/nats.go"
)

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
// 服务发现由装配层提供（fxkit.NewEtcdDiscovery），保证与注册端同一键前缀。
func NewActorRuntime(cfg *conf.Bootstrap, discovery registry.Discovery, meter metrics.Collector) (*pkgactor.Runtime, error) {
	var endpoints []string
	if r := cfg.GetRegistry(); r != nil && r.GetEtcd() != nil {
		endpoints = r.GetEtcd().GetEndpoints()
	}
	nodeID, ns := "", ""
	if r := cfg.GetRuntime(); r != nil {
		nodeID, ns = r.GetId(), pkgactor.NamespaceOf(r)
	}
	rt, err := pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        nodeID,
		Namespace:     ns,
		ServiceName:   consts.ServiceGame,
		EtcdEndpoints: endpoints,
		NatsURL:       natsURLOf(cfg),
		Tracer:        pkgactor.DefaultTracer(),
		Meter:         meter,
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
