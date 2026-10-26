// Package biz 定义 matcher 服务的业务接口（根包只放接口，
// 实现位于 handler/，撮合规则位于 matchfunc/）。
package biz

import (
	"context"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
)

// MatcherService 是撮合服务接口（grpc 实现，方法名对齐生成接口）。
type MatcherService interface {
	// QueueMatch 入队：组装属性快照 ticket 并登记玩家→ticket 映射。
	QueueMatch(ctx context.Context, req *matcherv1.QueueMatchRequest) (*matcherv1.QueueMatchReply, error)
	// CancelMatch 取消匹配：按玩家定位 ticket 并取消。
	CancelMatch(ctx context.Context, req *matcherv1.CancelMatchRequest) (*matcherv1.CancelMatchReply, error)
	// QueryMatch 查询玩家当前匹配状态。
	QueryMatch(ctx context.Context, req *matcherv1.QueryMatchRequest) (*matcherv1.QueryMatchReply, error)
}

// BattleAdmitter 是入场合并接口：**一次**跨进程 RPC 内完成「开局（懒激活对局 actor +
// 登记参战名单）」与「出票」。
//
// 为什么合并（12288 入场崩塌的根因）：过去每局串行发两次跨进程 RPC（CreateBattle →
// IssueEntryTicket），万级并发下尾延迟叠加，任一超时即「既无票也无成局事件」，玩家只能空等；
// 合并后名单与票在同一次调用里原子产生，每局只付一次跨进程往返。
type BattleAdmitter interface {
	// Admit 开局+出票：回执含接入层地址与 {player_id → ticket} 名单。
	// 原子性：失败时名单与票都不产生（battle 侧签票失败不登记名单）；
	// 幂等：同一局重复调用不重复出票（回执与首次一致）。
	Admit(ctx context.Context, battleID, matchID string, playerIDs []string) (*battlev1.IssueEntryTicketReply, error)
}

// MatchEventPublisher 是成局/失败事件发布接口（nats 总线）。
type MatchEventPublisher interface {
	// PublishStarted 发布成局事件（battle 已开局且票据已签发）。
	// tickets 是 battle 的入场回执（接入层地址 + 逐人票据），原样进事件。
	PublishStarted(ctx context.Context, battleID, matchID string, playerIDs []string, tickets *battlev1.IssueEntryTicketReply) error
	// PublishFailed 发布失败事件（超时/取消/处置失败）。
	// ticketID 是失败的票据 ID（未成局故无对局 ID，事件 match_id 为空）。
	PublishFailed(ctx context.Context, ticketID string, playerIDs []string, reason matcherv1.MatchFailReason) error
}

// MatchEventSink 是成局观察方接口（事件发布 + 入场合并调用的组合，runtime 监听用）。
type MatchEventSink interface {
	MatchEventPublisher
	BattleAdmitter
}

// Admission 是成局处置策略：有界并发扇出 + 单局超时 + 等槽预算。
// 三者的零值由 handler 取缺省（见 DefaultAdmit* 常量）——**0 不得解释为无限并发**。
type Admission struct {
	// Concurrency 是同时进行的「开局+出票」上限（跨进程 RPC 的扇出上限，背压来源）。
	Concurrency int
	// Timeout 是单局「开局+出票」的预算（含 RPC 往返，超时即失败处置）。
	Timeout time.Duration
	// Wait 是拿不到并发槽时的等槽预算：超时按失败处置（回池 + 失败事件），不无限排队。
	Wait time.Duration
}

// 成局处置的缺省参数。
//
// 并发 200 的依据：单局成功路径是两次状态查询之外的**一次**单程 RPC（毫秒级），
// 4.3k 局在 200 并发下 ≈ 单局 p99 的 20 余倍即可排空，同时把 battle 侧激活/登记压力
// 限制在可承载范围内（12288 入场时曾因无界并发把 battle 队列压到 30s 超时）。
const (
	// DefaultAdmitConcurrency 是同时进行的成局处置数上限。
	DefaultAdmitConcurrency = 200
	// DefaultAdmitTimeout 是单局开局+出票的预算（含 battle 侧懒激活冷启动那一跳）。
	DefaultAdmitTimeout = 5 * time.Second
	// DefaultAdmitWait 是等并发槽的预算。
	DefaultAdmitWait = 5 * time.Second
)

// Warmup 是「成局后、入场前」预热 battle actor 的参数。
//
// 为什么默认打开：懒激活（首次触达 actor 时的创建 + OnStart 加载）的冷启动实测 p50≈2s，
// 落在入场那一跳上就直接吃掉「成局 → 客户端连帧」之间的全部余量。预热把这段冷启动
// 挪到入场之前、并在独立 goroutine 里跑——失败只记日志与打点，不阻塞、不改变本局处置。
type Warmup struct {
	// Disabled 为 true 时不预热（零值 false = 预热打开）。
	Disabled bool
	// Timeout 是单次预热预算；0 取 DefaultWarmupTimeout。
	Timeout time.Duration
}

// DefaultWarmupTimeout 是单次预热的缺省预算：与冷启动实测 p50（≈2s）同量级，
// 够触达激活即可；比它更长的等待没有收益（入场自己还有 5s 预算兜底）。
const DefaultWarmupTimeout = 2 * time.Second

// BattleWarmer 预热目标对局的 battle actor。
//
// 实现必须**幂等**且无票务副作用（只触发激活/登记，不出票、不开入座窗口）：
// 唯一入场入口仍是 Admit，预热只是提前把懒激活的冷启动消费掉。
type BattleWarmer interface {
	// Warm 触达 battle:{battleID} 并返回错误（调用方只记日志与打点，不据此改处置）。
	Warm(ctx context.Context, battleID, matchID string, playerIDs []string) error
}

// MatchSettleDeduper 是成局结算去重接口：
// matchmaker 对同局每张 ticket 各发一次 TicketCompleted，
// 发布事件与开局调用必须按 matchID 幂等（仅首个处理者执行）。
type MatchSettleDeduper interface {
	// TrySettle 尝试认领对局结算：返回 true 表示本调用方执行结算（首次）。
	TrySettle(ctx context.Context, matchID string, ttl time.Duration) (bool, error)
}

// DefaultMatchmakerName 是未指定规则集时的默认匹配器名。
const DefaultMatchmakerName = "casual"

// DefaultPartyCapacity 是队伍容量上限（队伍总人数含队长，1..N 灵活组队）。
const DefaultPartyCapacity = 5

// PartyQueueMapper 是队伍→整队票映射存储（取消/重复入队校验用）。
type PartyQueueMapper interface {
	// GetPartyTicket 读取队伍的整队票（不存在返回空串）。
	GetPartyTicket(ctx context.Context, partyID string) (string, error)
	// SetPartyTicket 登记队伍的整队票（TTL 过期自动清理）。
	SetPartyTicket(ctx context.Context, partyID, ticketID string, ttl time.Duration) error
	// DelPartyTicket 删除映射。
	DelPartyTicket(ctx context.Context, partyID string) error
}

// PartyRosterPublisher 是队伍名册变更事件发布接口（nats 总线）。
type PartyRosterPublisher interface {
	// PublishRoster 发布名册变更事件（建队/加入/离开/解散，gateway 推送全队）。
	PublishRoster(ctx context.Context, partyID, leaderID string, playerIDs []string, reason matcherv1.PartyRosterReason) error
}

// PlayerTicketMapper 是玩家→ticket 映射存储（取消/查询定位用）。
type PlayerTicketMapper interface {
	// Get 读取玩家当前 ticket（不存在返回空串）。
	Get(ctx context.Context, playerID string) (string, error)
	// Set 登记玩家 ticket（TTL 过期自动清理）。
	Set(ctx context.Context, playerID, ticketID string, ttl time.Duration) error
	// Del 删除玩家 ticket 映射。
	Del(ctx context.Context, playerID string) error
	// SetMatch 关联玩家已成的对局（查询回执用）。
	SetMatch(ctx context.Context, playerID, matchID string, ttl time.Duration) error
	// GetMatch 读取玩家对局关联（不存在返回空串）。
	GetMatch(ctx context.Context, playerID string) (string, error)
	// DelMatch 删除玩家对局关联（入场失败时撤掉未成立的关联）。
	DelMatch(ctx context.Context, playerID string) error
	// SetBattle 关联玩家已成局的战斗（开局推送丢失后重登恢复加入用）。
	SetBattle(ctx context.Context, playerID, battleID string, ttl time.Duration) error
	// GetBattle 读取玩家战斗关联（不存在返回空串）。
	GetBattle(ctx context.Context, playerID string) (string, error)
	// DelBattle 删除玩家战斗关联（入场失败时撤掉未成立的关联）。
	DelBattle(ctx context.Context, playerID string) error
}
