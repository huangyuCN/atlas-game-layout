// Package infra 提供 matcher 服务的外部依赖装配：
// redis（ticket 映射 + matchmaker 后端）、nats（事件总线）、actor 集群客户端（开局调用）。
package infra

import (
	"context"
	"fmt"
	"strings"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/matchfunc"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/matchmaker"
	natsgo "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

// 映射键约定与结算去重键。
const (
	mapperKeyPrefix = "atlas:match:player:"
	resultKeyPrefix = "atlas:match:result:"
	settleKeyPrefix = "atlas:match:settled:"
	mapperTTL       = 5 * time.Minute
)

// DefaultMaxLevelGap 是「等级相近」规则的最大等级差（规则描述与管理端点共用同一来源）。
const DefaultMaxLevelGap = 3

// RuleDescription 是 casual 规则集的可读描述（管理端点查询用）。
var RuleDescription = "等级相近 1v1（最大等级差 " + fmt.Sprint(DefaultMaxLevelGap) + "）"

// RedisPlayerTicketMapper 是玩家→ticket 映射的 redis 实现。
type RedisPlayerTicketMapper struct {
	cli *pkredis.Client
}

// NewRedisPlayerTicketMapper 构造映射存储。
func NewRedisPlayerTicketMapper(cli *pkredis.Client) *RedisPlayerTicketMapper {
	return &RedisPlayerTicketMapper{cli: cli}
}

// Get 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) Get(ctx context.Context, playerID string) (string, error) {
	v, err := m.cli.Raw().Get(ctx, mapperKeyPrefix+playerID).Result()
	if err == goredis.Nil {
		return "", nil
	}
	return v, err
}

// Set 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) Set(ctx context.Context, playerID, ticketID string, ttl time.Duration) error {
	return m.cli.Raw().Set(ctx, mapperKeyPrefix+playerID, ticketID, ttl).Err()
}

// Del 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) Del(ctx context.Context, playerID string) error {
	return m.cli.Raw().Del(ctx, mapperKeyPrefix+playerID).Err()
}

// SetMatch 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) SetMatch(ctx context.Context, playerID, matchID string, ttl time.Duration) error {
	return m.cli.Raw().Set(ctx, resultKeyPrefix+playerID, matchID, ttl).Err()
}

// GetMatch 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) GetMatch(ctx context.Context, playerID string) (string, error) {
	v, err := m.cli.Raw().Get(ctx, resultKeyPrefix+playerID).Result()
	if err == goredis.Nil {
		return "", nil
	}
	return v, err
}

// NatsEventPublisher 是成局/失败事件的 nats 发布器（biz.MatchEventPublisher 实现）。
type NatsEventPublisher struct {
	nc *natsgo.Conn
}

// NewNatsEventPublisher 构造事件发布器。
func NewNatsEventPublisher(nc *natsgo.Conn) *NatsEventPublisher {
	return &NatsEventPublisher{nc: nc}
}

// PublishStarted 实现 biz.MatchEventPublisher（主题 atlas.event.match.started）。
func (p *NatsEventPublisher) PublishStarted(ctx context.Context, battleID, matchID string, playerIDs []string) error {
	payload, err := protojson.Marshal(&matcherv1.MatchStartedEvent{
		MatchId:        matchID,
		BattleId:       battleID,
		PlayerIds:      playerIDs,
		BattleEndpoint: "", // M7 battle 服务接入后填充
	})
	if err != nil {
		return fmt.Errorf("infra: 成局事件编码失败: %w", err)
	}
	return nats.Publish(ctx, p.nc, consts.MatchStartedTopic(), payload)
}

// PublishFailed 实现 biz.MatchEventPublisher（主题 atlas.event.match.failed）。
func (p *NatsEventPublisher) PublishFailed(ctx context.Context, matchID string, playerIDs []string, reason string) error {
	payload, err := protojson.Marshal(&matcherv1.MatchFailedEvent{
		MatchId:   matchID,
		PlayerIds: playerIDs,
		Reason:    reason,
	})
	if err != nil {
		return fmt.Errorf("infra: 失败事件编码失败: %w", err)
	}
	return nats.Publish(ctx, p.nc, consts.MatchFailedTopic(), payload)
}

// BattleActorStarter 是 biz.BattleStarter 的实现：
// 经 actor 集群 Ask 懒激活战斗 actor（battle:<battleID>，M7 battle 服务承接）。
type BattleActorStarter struct {
	rt *pkgactor.Runtime
}

// NewBattleActorStarter 构造开局调用器。
func NewBattleActorStarter(rt *pkgactor.Runtime) *BattleActorStarter {
	return &BattleActorStarter{rt: rt}
}

// Start 实现 biz.BattleStarter：经 actor 集群 Ask 懒激活战斗 actor（信封开局）。
func (s *BattleActorStarter) Start(ctx context.Context, battleID, matchID string, playerIDs []string) error {
	pid, err := types.NewPID(consts.ActorTypeBattle, battleID)
	if err != nil {
		return fmt.Errorf("infra: 非法战斗 ID %q: %w", battleID, err)
	}
	reply := new(battlev1.CreateBattleReply)
	err = s.rt.AskProto(ctx, pid, &battlev1.BattleActorMsg{
		Kind: &battlev1.BattleActorMsg_Create{Create: &battlev1.CreateBattleRequest{
			MatchId:   matchID,
			PlayerIds: playerIDs,
		}},
	}, reply)
	if err != nil {
		return fmt.Errorf("infra: 开局调用失败: %w", err)
	}
	return nil
}

// NewMatchmakerRuntime 装配 matchmaker redis 运行时（等级相近规则集 + 本地 Placement）。
func NewMatchmakerRuntime(cli *pkredis.Client) (*matchredis.Runtime, error) {
	reg := matchmaker.Registry{
		biz.DefaultMatchmakerName: {
			Name:              biz.DefaultMatchmakerName,
			RequestTimeout:    2 * time.Minute,
			AcceptanceTimeout: 0, // 跳过确认阶段
			RequeuePolicy:     matchmaker.RequeueAll,
			MatchFunction:     matchfunc.LevelClose{MaxGap: DefaultMaxLevelGap},
			Placement:         LocalPlacement{},
			FetchSize:         10,
			TickInterval:      50 * time.Millisecond,
		},
	}
	return matchredis.NewRuntime(cli.Raw(), matchredis.Registry(reg))
}

// LocalPlacement 是占位 Placement：对局实例的分配由开局调用
// （BattleStarter 经 actor Ask 懒激活战斗 actor）承担，此处直接回执分配结果。
type LocalPlacement struct{}

// Place 实现 matchmaker.PlacementQueue。
func (LocalPlacement) Place(_ context.Context, m matchmaker.Match) (matchmaker.Assignment, error) {
	ticketIDs := make([]string, 0, len(m.Tickets))
	playerIDs := make([]string, 0, len(m.Tickets))
	for _, t := range m.Tickets {
		ticketIDs = append(ticketIDs, t.ID)
		playerIDs = append(playerIDs, t.PlayerIDs()...)
	}
	return matchmaker.Assignment{
		MatchID:        m.ID,
		TicketIDs:      ticketIDs,
		Teams:          m.Teams,
		ConnectionInfo: map[string]string{"player_ids": strings.Join(playerIDs, ",")},
	}, nil
}

// Sink 是成局观察方的默认组合（nats 发布 + actor 开局调用）。
// server 装配与可编程装配共用同一实现。
type Sink struct {
	publisher biz.MatchEventPublisher
	starter   biz.BattleStarter
}

// NewSink 构造默认成局观察方。
func NewSink(nc *natsgo.Conn, rt *pkgactor.Runtime) *Sink {
	return &Sink{
		publisher: NewNatsEventPublisher(nc),
		starter:   NewBattleActorStarter(rt),
	}
}

func (s *Sink) PublishStarted(ctx context.Context, battleID, matchID string, playerIDs []string) error {
	return s.publisher.PublishStarted(ctx, battleID, matchID, playerIDs)
}
func (s *Sink) PublishFailed(ctx context.Context, matchID string, playerIDs []string, reason string) error {
	return s.publisher.PublishFailed(ctx, matchID, playerIDs, reason)
}
func (s *Sink) Start(ctx context.Context, battleID, matchID string, playerIDs []string) error {
	return s.starter.Start(ctx, battleID, matchID, playerIDs)
}

// RedisSettleDeduper 是成局结算去重的 redis 实现（SETNX 幂等，跨实例安全）。
type RedisSettleDeduper struct {
	cli *pkredis.Client
}

// NewRedisSettleDeduper 构造结算去重器。
func NewRedisSettleDeduper(cli *pkredis.Client) *RedisSettleDeduper {
	return &RedisSettleDeduper{cli: cli}
}

// TrySettle 实现 biz.MatchSettleDeduper：首个 SETNX 成功者执行结算。
func (d *RedisSettleDeduper) TrySettle(ctx context.Context, matchID string, ttl time.Duration) (bool, error) {
	ok, err := d.cli.Raw().SetNX(ctx, settleKeyPrefix+matchID, "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("infra: 结算去重失败: %w", err)
	}
	return ok, nil
}

// 静态保证实现接口。
var (
	_ biz.PlayerTicketMapper    = (*RedisPlayerTicketMapper)(nil)
	_ biz.MatchEventPublisher   = (*NatsEventPublisher)(nil)
	_ biz.BattleStarter         = (*BattleActorStarter)(nil)
	_ biz.MatchEventSink        = (*Sink)(nil)
	_ biz.MatchSettleDeduper    = (*RedisSettleDeduper)(nil)
	_ matchmaker.PlacementQueue = LocalPlacement{}
)
