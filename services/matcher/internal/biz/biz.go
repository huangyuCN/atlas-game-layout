// Package biz 定义 matcher 服务的业务接口（根包只放接口，
// 实现位于 handler/，撮合规则位于 matchfunc/）。
package biz

import (
	"context"
	"time"

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

// BattleStarter 是对局开局接口：成局后懒激活战斗 actor（battle:<battleID>）。
// matcher 只发起调用与可观测（M7 battle 服务接入后由真实 actor 承接）。
type BattleStarter interface {
	// Start 开局：向战斗 actor 发起开局请求（失败不阻断成局事件发布）。
	Start(ctx context.Context, battleID, matchID string, playerIDs []string) error
}

// MatchEventPublisher 是成局/失败事件发布接口（nats 总线）。
type MatchEventPublisher interface {
	// PublishStarted 发布成局事件（battle 已创建/开局已发起）。
	PublishStarted(ctx context.Context, battleID, matchID string, playerIDs []string) error
	// PublishFailed 发布失败事件（超时/取消）。
	// ticketID 是失败的票据 ID（未成局故无对局 ID，事件 match_id 为空）。
	PublishFailed(ctx context.Context, ticketID string, playerIDs []string, reason string) error
}

// MatchEventSink 是成局观察方接口（事件发布与开局调用的组合，runtime 监听用）。
type MatchEventSink interface {
	MatchEventPublisher
	BattleStarter
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
}
