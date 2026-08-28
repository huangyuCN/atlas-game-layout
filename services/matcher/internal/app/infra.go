package app

import (
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/infra"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/matchmaker"
	natsgo "github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// NewRedisClient 装配 redis 客户端（票据映射 + 去重 + matchmaker 后端）。
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

// NewNatsConn 装配 NATS 连接（成局事件总线）。
func NewNatsConn(cfg *conf.Bootstrap) (*natsgo.Conn, error) {
	url := ""
	name := consts.ServiceMatcher
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

// NewActorRuntime 装配 actor 集群客户端（成局后开局调用 + 战斗 actor 懒激活副本）：
// ServiceName 指向 battle——副本注册使本节点的懒激活判定成立，实际拉起在 battle 节点（M7）。
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
	if err := pkgactor.RegisterBattleReplica(rt); err != nil {
		return nil, fmt.Errorf("app: 注册战斗 actor 懒激活副本失败: %w", err)
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

// NewMatchmakerRuntime 装配撮合运行时（redis 后端 + 等级相近规则）。
func NewMatchmakerRuntime(cli *pkredis.Client) (*matchredis.Runtime, error) {
	rt, err := infra.NewMatchmakerRuntime(cli)
	if err != nil {
		return nil, fmt.Errorf("app: 构造撮合运行时失败: %w", err)
	}
	return rt, nil
}

// serviceOf 暴露撮合运行时对外 API（grpc handler 依赖接口形态）。
func serviceOf(rt *matchredis.Runtime) matchmaker.Service { return rt.Service }

// newSink 装配成局观察方默认组合（nats 发布 + 开局调用）；
// 测试注入经 fx.Decorate 覆盖本提供器输出（见 assemble）。
func newSink(nc *natsgo.Conn, rt *pkgactor.Runtime) biz.MatchEventSink {
	return infra.NewSink(nc, rt)
}
