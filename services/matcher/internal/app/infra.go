package app

import (
	"context"
	"fmt"

	battlev1rpc "github.com/huangyuCN/atlas-game-layout/api/battle/v1/rpc"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/infra"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/matchmaker"
	"github.com/huangyuCN/atlas/registry"
	natsgo "github.com/nats-io/nats.go"
	"google.golang.org/grpc"
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

// NewBattleConn 拨号 battle 的 **internal 面**（可信区）：成局后的开局调用走
// 类型化客户端（BattleService/Create），matcher 不再自建 actor 集群运行时，
// 也不需要「只发不接」的懒激活副本——目标 PID 的解析与懒激活在 battle 侧完成。
func NewBattleConn(discovery registry.Discovery, mws serverutil.ClientMiddlewares) *serverutil.LazyConn {
	return serverutil.NewLazyConn(func(ctx context.Context) (grpc.ClientConnInterface, error) {
		conn, err := serverutil.DialDomain(ctx, discovery, consts.ServiceBattle, serverutil.SchemeGRPC, mws)
		if err != nil {
			return nil, err
		}
		return conn, nil
	})
}

// NewBattleClient 基于 internal 面连接构造 BattleService 类型化客户端。
func NewBattleClient(conn *serverutil.LazyConn) battlev1rpc.BattleServiceClient {
	return battlev1rpc.NewBattleServiceClient(conn)
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
func newSink(pub *pkgnats.Publisher, cli battlev1rpc.BattleServiceClient) biz.MatchEventSink {
	return infra.NewSink(pub, cli)
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
