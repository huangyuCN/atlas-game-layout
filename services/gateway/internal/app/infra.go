package app

import (
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/registry"
	natsgo "github.com/nats-io/nats.go"
)

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

// sessionOptionsOf 把会话租期配置映射为管理器参数：空值透传零值
// （默认值由 session 包兜底，见 session.DefaultTTL），非法值启动期报错。
func sessionOptionsOf(cfg *conf.Bootstrap) (session.Options, error) {
	ttl, err := config.ParseDuration(cfg.GetSession().GetTtl())
	if err != nil {
		return session.Options{}, fmt.Errorf("app: session.ttl 无效: %w", err)
	}
	sweep, err := config.ParseDuration(cfg.GetSession().GetSweepInterval())
	if err != nil {
		return session.Options{}, fmt.Errorf("app: session.sweep_interval 无效: %w", err)
	}
	return session.Options{TTL: ttl, SweepInterval: sweep}, nil
}

// newSessionManager 装配分布式会话管理器（并注入指标采集器）。
func newSessionManager(cfg *conf.Bootstrap, cli *pkredis.Client, meter metrics.Collector) (*session.Manager, error) {
	opts, err := sessionOptionsOf(cfg)
	if err != nil {
		return nil, err
	}
	instanceID := ""
	if r := cfg.GetRuntime(); r != nil {
		instanceID = r.GetId()
	}
	m := session.NewManager(session.NewRedisStore(cli), instanceID, opts)
	m.SetMeter(meter)
	return m, nil
}

// NewActorClient 装配远程 actor 客户端背后的集群运行时
// （Locator=etcd、NATS 传输；PlayerActor 懒激活在 game 节点执行，
// 本节点注册「只发不接」副本以支持发送侧判定）。
func NewActorClient(cfg *conf.Bootstrap, discovery registry.Discovery, meter metrics.Collector) (*actorclient.Client, error) {
	var endpoints []string
	if r := cfg.GetRegistry(); r != nil && r.GetEtcd() != nil {
		endpoints = r.GetEtcd().GetEndpoints()
	}
	nodeID := ""
	if r := cfg.GetRuntime(); r != nil {
		nodeID = r.GetId()
	}
	rt, err := pkgactor.NewRuntime(pkgactor.Options{
		NodeID:        nodeID,
		ServiceName:   consts.ServiceGame, // 懒激活在 game 节点执行（PlayerActor 宿主）
		EtcdEndpoints: endpoints,
		NatsURL:       natsURLOf(cfg),
		Tracer:        pkgactor.DefaultTracer(),
		Meter:         meter,
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
