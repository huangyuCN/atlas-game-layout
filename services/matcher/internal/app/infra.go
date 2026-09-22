package app

import (
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/infra"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/matchmaker"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/registry"
	natsgo "github.com/nats-io/nats.go"
)

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
// 服务发现由装配层提供（fxkit.NewEtcdDiscovery），保证与注册端同一键前缀。
func NewActorRuntime(cfg *conf.Bootstrap, discovery registry.Discovery, meter metrics.Collector) (*pkgactor.Runtime, error) {
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
		ServiceName:   consts.ServiceBattle,
		EtcdEndpoints: endpoints,
		NatsURL:       natsURLOf(cfg),
		Tracer:        pkgactor.DefaultTracer(),
		Meter:         meter,
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

// rosterOf 把 nats 事件发布器绑定为名册变更发布接口（供 fx 按接口注入）。
func rosterOf(p *infra.NatsEventPublisher) biz.PartyRosterPublisher { return p }

// NewMatchmakerParty 装配整队名册引擎（redis Party，容量原子校验）。
func NewMatchmakerParty(svc matchmaker.Service, cli *pkredis.Client) matchmaker.Party {
	return infra.NewMatchmakerParty(svc, cli)
}

// newMatcherDeps 聚合撮合 handler 依赖（fx 装配）。
func newMatcherDeps(svc matchmaker.Service, party matchmaker.Party,
	mapper biz.PlayerTicketMapper, partyMapper biz.PartyQueueMapper,
	sink biz.MatchEventSink, deduper biz.MatchSettleDeduper, roster biz.PartyRosterPublisher) handler.MatcherDeps {
	return handler.MatcherDeps{
		Svc:         svc,
		Party:       party,
		Mapper:      mapper,
		PartyMapper: partyMapper,
		Sink:        sink,
		Deduper:     deduper,
		Roster:      roster,
	}
}
