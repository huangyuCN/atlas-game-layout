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
	mmmetrics "github.com/huangyuCN/atlas/contrib/matchmaker/metrics"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/matchmaker"
	"github.com/huangyuCN/atlas/metrics"
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

// DefaultTickInterval 是池扫描周期的缺省值（10ms）：成局上限 = tick 频率 × 每 tick 成局数，
// 旧值 50ms 与「每轮只成 1 局」叠加即 20 局/秒 = 200 玩家/秒（实测 19.95 贴顶）。
// 规则集已改成一轮多局，故把周期收到 10ms 直接把上限抬 5 倍。
//
// 代价（诊断 §5 P0-2）：每 tick 的固定开销（抢锁/3 个扫描/池大小/认领 = ~4 次 Redis 往返）
// 从 20 次/秒升到 100 次/秒；matcher 全轮实测仅 0.067 核，CPU 可忽略，真正的成本在共享
// Redis 的 QPS 上——故本值可配（conf tick_interval_ms），按池深度与 Redis 余量调。
const DefaultTickInterval = 10 * time.Millisecond

// DefaultFetchSize 是每 tick 认领的候选上限（缺省 100 张）。
// 旧值 10 与「每轮只成 1 局」叠加时，每 tick 只认领 10 票 = 恰好 1 局（12288 实测
// matches_per_tick 恒 1.000，成局速率 43.6 局/秒 × 10 人 ≈ 445 玩家/秒）；放大到 100
// 让循环成型一轮能成 10 局（5v5），上限随之 ≈4,000 玩家/秒。
//
// 上界口径：单 tick 的票数据量与 Lua 扫描量都由它决定（认领 ≤FetchSize 张、
// 服务端 ≤4×FetchSize 次 GET），故本值可配（conf fetch_size）但夹在 MaxFetchSize 内。
const DefaultFetchSize = 100

// MaxFetchSize 是认领上限的安全边界：再大只会把单 tick 的内存与延迟线性推高，
// 而成局吞吐已受 Redis 往返与后续 Admit 环节约束（100 张 ≈ 10 局/轮已够 4,000 人/秒）。
const MaxFetchSize = 1000

// fetchSizeOrDefault 把配置的认领上限夹到 [1, MaxFetchSize]（≤0 取缺省）。
func fetchSizeOrDefault(n int) int {
	if n <= 0 {
		return DefaultFetchSize
	}
	if n > MaxFetchSize {
		return MaxFetchSize
	}
	return n
}

// MatchmakerRegistry 返回 casual 规则集的撮合配置
// （等待阈值、扫描周期与认领上限；时间 ≤0、fetchSize ≤0 取缺省）。
// 单独导出以便装配前就能断言「跑的是什么周期/批量」，不必启动 tick 循环去反推。
func MatchmakerRegistry(requestTimeout, tickInterval time.Duration, fetchSize int) matchmaker.Registry {
	if requestTimeout <= 0 {
		requestTimeout = DefaultRequestTimeout
	}
	if tickInterval <= 0 {
		tickInterval = DefaultTickInterval
	}
	return matchmaker.Registry{
		biz.DefaultMatchmakerName: {
			Name:              biz.DefaultMatchmakerName,
			RequestTimeout:    requestTimeout,
			AcceptanceTimeout: 0, // 跳过确认阶段
			RequeuePolicy:     matchmaker.RequeueAll,
			MatchFunction:     matchfunc.FlexTeams{MaxGap: DefaultMaxLevelGap, MaxSide: DefaultMaxSide},
			Placement:         LocalPlacement{},
			FetchSize:         fetchSizeOrDefault(fetchSize),
			TickInterval:      tickInterval,
		},
	}
}

// NewMatchmakerRuntime 装配 matchmaker redis 运行时
// （等级相近灵活组队规则集 + 本地 Placement）。
// requestTimeout 是单张票的最长等待时间，tickInterval 是池扫描周期，
// fetchSize 是每 tick 认领上限（均取缺省当 ≤0）。
// meter 用于导出 tick 级指标（matcher_tick_duration_seconds 等）；nil 时降级为 noop。
func NewMatchmakerRuntime(cli *pkredis.Client, requestTimeout, tickInterval time.Duration,
	fetchSize int, meter metrics.Collector) (*matchredis.Runtime, error) {
	reg := MatchmakerRegistry(requestTimeout, tickInterval, fetchSize)
	return matchredis.NewRuntime(cli.Raw(), matchredis.Registry(reg),
		matchredis.WithPrefix(cli.Keys().MatchmakerPrefix()),
		matchredis.WithMetrics(mmmetrics.New(meter)))
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

// BattleGRPCWarmer 是 biz.BattleWarmer 的实现：经 battle internal 面的 Create
// 触达 battle:{battle_id}，把懒激活的冷启动提前消费掉。
//
// 为什么用 Create 而不是 Admit：预热必须**无票务副作用**——Create 只登记 match_id
// 与参战名单（与随后 Admit 写入的是同一份数据，且幂等），不出票、不开入座窗口；
// Admit 仍是唯一入场入口。预热与入场用同一 battle_id/名单，命中同一 actor。
type BattleGRPCWarmer struct {
	cli battlev1rpc.BattleServiceClient
}

// NewBattleGRPCWarmer 构造预热端口。
func NewBattleGRPCWarmer(cli battlev1rpc.BattleServiceClient) *BattleGRPCWarmer {
	return &BattleGRPCWarmer{cli: cli}
}

// Warm 实现 biz.BattleWarmer：一次 Create 调用即触达目标 actor（懒激活在 battle 侧完成）。
func (w *BattleGRPCWarmer) Warm(ctx context.Context, battleID, matchID string, playerIDs []string) error {
	_, err := w.cli.Create(ctx, &battlev1.CreateBattleRequest{
		BattleId:  battleID,
		MatchId:   matchID,
		PlayerIds: playerIDs,
	})
	return err
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
