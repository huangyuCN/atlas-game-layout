package app

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/repo"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// NewNatsConn 装配 NATS 连接（帧广播 + 结算事件）。
func NewNatsConn(cfg *conf.Bootstrap) (*natsgo.Conn, error) {
	url := ""
	name := consts.ServiceBattle
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

// NewMongoClient 装配 MongoDB 客户端（结算落库）。
// fx 无法注入裸 context.Context，这里以进程级上下文构造。
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

// newResultRepo 装配结算结果仓储。
func newResultRepo(cli *mongo.Client) repo.ResultRepo {
	return repo.NewMongoResultRepo(cli)
}

// NewActorRuntime 装配 actor 集群运行时（战斗 actor 宿主，
// Locator=etcd、传输=NATS、懒激活按服务发现选 battle 节点）。
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
		ServiceName:   consts.ServiceBattle,
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

// NewPubSub 装配帧广播 pubsub（底层为本节点本地运行时）。
func NewPubSub(rt *pkgactor.Runtime) (*pubsub.Registry, error) {
	reg, err := pubsub.New(rt.Raw().Local())
	if err != nil {
		return nil, fmt.Errorf("app: 构造 pubsub 失败: %w", err)
	}
	return reg, nil
}
