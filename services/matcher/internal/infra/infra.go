// Package infra 提供 matcher 服务的外部依赖装配：
// redis（ticket 映射 + matchmaker 后端）、nats（事件总线）、battle internal 面 gRPC 客户端（开局调用）。
package infra

import (
	"context"
	"fmt"
	"strings"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1rpc "github.com/huangyuCN/atlas-game-layout/api/battle/v1/rpc"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/matchfunc"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/matchmaker"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
)

// mapperTTL 是玩家→ticket 映射的租期（键名见 pkg/redis.Keys，前缀只在那一处拼）。
const mapperTTL = 5 * time.Minute

// DefaultMaxLevelGap 是「等级相近」规则的最大等级差（规则描述与管理端点共用同一来源）。
const DefaultMaxLevelGap = 3

// DefaultMaxSide 是灵活组队的单侧人数上限（与 biz.DefaultPartyCapacity 对齐）。
const DefaultMaxSide = 5

// RuleDescription 是 casual 规则集的可读描述（管理端点查询用）。
var RuleDescription = "等级相近灵活组队（1..N 人，最大等级差 " + fmt.Sprint(DefaultMaxLevelGap) +
	"，单侧上限 " + fmt.Sprint(DefaultMaxSide) + "）"

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
	v, err := m.cli.Raw().Get(ctx, m.cli.Keys().MatchPlayerTicket(playerID)).Result()
	if err == goredis.Nil {
		return "", nil
	}
	return v, err
}

// Set 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) Set(ctx context.Context, playerID, ticketID string, ttl time.Duration) error {
	return m.cli.Raw().Set(ctx, m.cli.Keys().MatchPlayerTicket(playerID), ticketID, ttl).Err()
}

// Del 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) Del(ctx context.Context, playerID string) error {
	return m.cli.Raw().Del(ctx, m.cli.Keys().MatchPlayerTicket(playerID)).Err()
}

// SetMatch 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) SetMatch(ctx context.Context, playerID, matchID string, ttl time.Duration) error {
	return m.cli.Raw().Set(ctx, m.cli.Keys().MatchResult(playerID), matchID, ttl).Err()
}

// GetMatch 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) GetMatch(ctx context.Context, playerID string) (string, error) {
	v, err := m.cli.Raw().Get(ctx, m.cli.Keys().MatchResult(playerID)).Result()
	if err == goredis.Nil {
		return "", nil
	}
	return v, err
}

// SetBattle 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) SetBattle(ctx context.Context, playerID, battleID string, ttl time.Duration) error {
	return m.cli.Raw().Set(ctx, m.cli.Keys().MatchBattle(playerID), battleID, ttl).Err()
}

// GetBattle 实现 biz.PlayerTicketMapper。
func (m *RedisPlayerTicketMapper) GetBattle(ctx context.Context, playerID string) (string, error) {
	v, err := m.cli.Raw().Get(ctx, m.cli.Keys().MatchBattle(playerID)).Result()
	if err == goredis.Nil {
		return "", nil
	}
	return v, err
}

// DelMatch 实现 biz.PlayerTicketMapper（入场失败时撤掉未成立的对局关联）。
func (m *RedisPlayerTicketMapper) DelMatch(ctx context.Context, playerID string) error {
	return m.cli.Raw().Del(ctx, m.cli.Keys().MatchResult(playerID)).Err()
}

// DelBattle 实现 biz.PlayerTicketMapper（入场失败时撤掉未成立的战斗关联）。
func (m *RedisPlayerTicketMapper) DelBattle(ctx context.Context, playerID string) error {
	return m.cli.Raw().Del(ctx, m.cli.Keys().MatchBattle(playerID)).Err()
}

// RedisPartyQueueMapper 是队伍→整队票映射的 redis 实现。
type RedisPartyQueueMapper struct {
	cli *pkredis.Client
}

// NewRedisPartyQueueMapper 构造队伍票映射存储。
func NewRedisPartyQueueMapper(cli *pkredis.Client) *RedisPartyQueueMapper {
	return &RedisPartyQueueMapper{cli: cli}
}

// GetPartyTicket 实现 biz.PartyQueueMapper。
func (m *RedisPartyQueueMapper) GetPartyTicket(ctx context.Context, partyID string) (string, error) {
	v, err := m.cli.Raw().Get(ctx, m.cli.Keys().MatchParty(partyID)).Result()
	if err == goredis.Nil {
		return "", nil
	}
	return v, err
}

// SetPartyTicket 实现 biz.PartyQueueMapper。
func (m *RedisPartyQueueMapper) SetPartyTicket(ctx context.Context, partyID, ticketID string, ttl time.Duration) error {
	return m.cli.Raw().Set(ctx, m.cli.Keys().MatchParty(partyID), ticketID, ttl).Err()
}

// DelPartyTicket 实现 biz.PartyQueueMapper。
func (m *RedisPartyQueueMapper) DelPartyTicket(ctx context.Context, partyID string) error {
	return m.cli.Raw().Del(ctx, m.cli.Keys().MatchParty(partyID)).Err()
}

// NatsEventPublisher 是成局/失败事件的 nats 发布器（biz.MatchEventPublisher 实现）。
type NatsEventPublisher struct {
	pub *pkgnats.Publisher
}

// NewNatsEventPublisher 构造事件发布器。
func NewNatsEventPublisher(pub *pkgnats.Publisher) *NatsEventPublisher {
	return &NatsEventPublisher{pub: pub}
}

// PublishStarted 实现 biz.MatchEventPublisher（主题 atlas.<ns>.event.match.started）：
// 事件带上 battle 回执的接入层面列表与逐人票据（matcher 只搬运，不重组票、不改地址）。
func (p *NatsEventPublisher) PublishStarted(ctx context.Context, battleID, matchID string, playerIDs []string, tickets *battlev1.IssueEntryTicketReply) error {
	payload, err := protojson.Marshal(&matcherv1.MatchStartedEvent{
		MatchId:         matchID,
		BattleId:        battleID,
		PlayerIds:       playerIDs,
		BattleEndpoints: tickets.GetEndpoints(),
		BattleTickets:   tickets.GetTickets(),
	})
	if err != nil {
		return fmt.Errorf("infra: 成局事件编码失败: %w", err)
	}
	return p.pub.Publish(ctx, p.pub.Topics().MatchStarted(), payload)
}

// PublishFailed 实现 biz.MatchEventPublisher（主题 atlas.<ns>.event.match.failed）。
// ticketID 是失败的票据 ID；未成局故 match_id 留空。
func (p *NatsEventPublisher) PublishFailed(ctx context.Context, ticketID string, playerIDs []string, reason matcherv1.MatchFailReason) error {
	payload, err := protojson.Marshal(&matcherv1.MatchFailedEvent{
		PlayerIds: playerIDs,
		Reason:    reason,
		TicketId:  ticketID,
	})
	if err != nil {
		return fmt.Errorf("infra: 失败事件编码失败: %w", err)
	}
	return p.pub.Publish(ctx, p.pub.Topics().MatchFailed(), payload)
}

// PublishRoster 实现 biz.PartyRosterPublisher（主题 atlas.<ns>.event.party.roster）。
func (p *NatsEventPublisher) PublishRoster(ctx context.Context, partyID, leaderID string, playerIDs []string, reason matcherv1.PartyRosterReason) error {
	payload, err := protojson.Marshal(&matcherv1.PartyRosterEvent{
		PartyId:   partyID,
		LeaderId:  leaderID,
		PlayerIds: playerIDs,
		Reason:    reason,
	})
	if err != nil {
		return fmt.Errorf("infra: 名册事件编码失败: %w", err)
	}
	return p.pub.Publish(ctx, p.pub.Topics().PartyRoster(), payload)
}

// BattleGRPCAdmitter 是 biz.BattleAdmitter 的实现：经 battle 的 internal 面 gRPC 客户端
// 发起**一次**入场调用（开局+出票合并）。目标 battle actor 由 battle 侧按请求里的
// battle_id 解析并懒激活——matcher 不持有 actor 集群运行时、也不持票据密钥。
type BattleGRPCAdmitter struct {
	cli battlev1rpc.BattleServiceClient
}

// NewBattleGRPCAdmitter 构造入场调用器。
func NewBattleGRPCAdmitter(cli battlev1rpc.BattleServiceClient) *BattleGRPCAdmitter {
	return &BattleGRPCAdmitter{cli: cli}
}

// Admit 实现 biz.BattleAdmitter：一次 RPC 内完成开局与出票（AdmitBattleReq.BattleId 即寻址字段）。
// 业务错误原样透传（远端 status 已在客户端拦截器还原为结构化错误，reason 可判定）。
func (a *BattleGRPCAdmitter) Admit(ctx context.Context, battleID, matchID string, playerIDs []string) (*battlev1.IssueEntryTicketReply, error) {
	reply, err := a.cli.Admit(ctx, &battlev1.AdmitBattleReq{
		MatchId:   matchID,
		PlayerIds: playerIDs,
		BattleId:  battleID, // 目标身份随请求携带：rpc 面按 uid_field 寻址
	})
	if err != nil {
		return nil, fmt.Errorf("infra: 入场调用失败（开局+出票）: %w", err)
	}
	return reply, nil
}

// DefaultRequestTimeout 是单张票的最长等待时间（缺省 30s）：到点即按既有语义给出
// 确定性失败回执（引擎 TicketTimedOut → matcher 发 MATCH_FAIL_REASON_TIMEOUT 失败事件）。
//
// 取 30s 的理由（诊断 §5 建议 4「匹配尾快速回执」）：本轮 12288 入场有 3608 人未成局，
// 旧值 2min 大于客户端耐心（142s 时 12288 人已全部 CancelMatch）——等于没有回执；
// 30s 让「超过阈值仍在池中」的玩家在放弃之前拿到终态，客户端据此自行重排。
// 复用既有回执语义是关键取舍：不新增事件类型、不加协议字段、不改客户端 SDK。
const DefaultRequestTimeout = 30 * time.Second

// NewMatchmakerRuntime 装配 matchmaker redis 运行时
// （等级相近灵活组队规则集 + 本地 Placement）。
// requestTimeout 是单张票的最长等待时间（≤0 取 DefaultRequestTimeout）。
func NewMatchmakerRuntime(cli *pkredis.Client, requestTimeout time.Duration) (*matchredis.Runtime, error) {
	if requestTimeout <= 0 {
		requestTimeout = DefaultRequestTimeout
	}
	reg := matchmaker.Registry{
		biz.DefaultMatchmakerName: {
			Name:              biz.DefaultMatchmakerName,
			RequestTimeout:    requestTimeout,
			AcceptanceTimeout: 0, // 跳过确认阶段
			RequeuePolicy:     matchmaker.RequeueAll,
			MatchFunction:     matchfunc.FlexTeams{MaxGap: DefaultMaxLevelGap, MaxSide: DefaultMaxSide},
			Placement:         LocalPlacement{},
			FetchSize:         10,
			TickInterval:      50 * time.Millisecond,
		},
	}
	return matchredis.NewRuntime(cli.Raw(), matchredis.Registry(reg),
		matchredis.WithPrefix(cli.Keys().MatchmakerPrefix()))
}

// NewMatchmakerParty 装配整队名册引擎（redis Party，容量原子校验）。
func NewMatchmakerParty(svc matchmaker.Service, cli *pkredis.Client) matchmaker.Party {
	return matchredis.NewParty(svc, cli.Raw(), matchredis.WithPartyCapacity(biz.DefaultPartyCapacity),
		matchredis.WithPrefix(cli.Keys().MatchmakerPrefix()))
}

// LocalPlacement 是占位 Placement：对局实例的分配由入场调用
// （Admit：battle 侧按 battle_id 懒激活战斗 actor）承担，此处直接回执分配结果。
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

// Sink 是成局观察方的默认组合（nats 发布 + battle 入场调用）。
// server 装配与可编程装配共用同一实现。
type Sink struct {
	publisher biz.MatchEventPublisher
	admitter  biz.BattleAdmitter
}

// NewSink 构造默认成局观察方（入场调用由 battle internal 面客户端承担）。
func NewSink(pub *pkgnats.Publisher, cli battlev1rpc.BattleServiceClient) *Sink {
	return &Sink{
		publisher: NewNatsEventPublisher(pub),
		admitter:  NewBattleGRPCAdmitter(cli),
	}
}

// PublishStarted 实现 biz.MatchEventSink：转发成局事件（含 battle 回执的票据）到内部发布器。
func (s *Sink) PublishStarted(ctx context.Context, battleID, matchID string, playerIDs []string, tickets *battlev1.IssueEntryTicketReply) error {
	return s.publisher.PublishStarted(ctx, battleID, matchID, playerIDs, tickets)
}

// PublishFailed 实现 biz.MatchEventSink：转发失败事件到内部发布器。
func (s *Sink) PublishFailed(ctx context.Context, ticketID string, playerIDs []string, reason matcherv1.MatchFailReason) error {
	return s.publisher.PublishFailed(ctx, ticketID, playerIDs, reason)
}

// Admit 实现 biz.BattleAdmitter：转发入场调用（开局+出票，一次 RPC）到 battle internal 面。
func (s *Sink) Admit(ctx context.Context, battleID, matchID string, playerIDs []string) (*battlev1.IssueEntryTicketReply, error) {
	return s.admitter.Admit(ctx, battleID, matchID, playerIDs)
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
	ok, err := d.cli.Raw().SetNX(ctx, d.cli.Keys().MatchSettled(matchID), "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("infra: 结算去重失败: %w", err)
	}
	return ok, nil
}

// 静态保证实现接口。
var (
	_ biz.PlayerTicketMapper    = (*RedisPlayerTicketMapper)(nil)
	_ biz.MatchEventPublisher   = (*NatsEventPublisher)(nil)
	_ biz.BattleAdmitter        = (*BattleGRPCAdmitter)(nil)
	_ biz.MatchEventSink        = (*Sink)(nil)
	_ biz.MatchSettleDeduper    = (*RedisSettleDeduper)(nil)
	_ matchmaker.PlacementQueue = LocalPlacement{}
)
