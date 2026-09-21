// Package handler 提供 biz 各接口的实现（撮合 grpc：入队/取消/查询）。
package handler

import (
	"context"
	"strings"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/idgen"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas/matchmaker"
	"go.opentelemetry.io/otel/attribute"
)

// ticketTTL 是玩家→ticket 映射与结算去重的租期（与 matchmaker 后端 ticket TTL 对齐）。
const ticketTTL = 5 * time.Minute

// MatcherDeps 聚合撮合 handler 的依赖（graph 装配与测试共用）；
// Party/PartyMapper/Roster 可为 nil：party 域方法降级返回内部错误（单测可只测单人域）。
type MatcherDeps struct {
	Svc         matchmaker.Service
	Party       matchmaker.Party
	Mapper      biz.PlayerTicketMapper
	PartyMapper biz.PartyQueueMapper
	Sink        biz.MatchEventSink
	Deduper     biz.MatchSettleDeduper
	Roster      biz.PartyRosterPublisher
}

// MatcherHandler 是 biz.MatcherService 与 party 域的 grpc 实现。
type MatcherHandler struct {
	matcherv1.UnimplementedMatcherServer
	svc         matchmaker.Service
	party       matchmaker.Party
	mapper      biz.PlayerTicketMapper
	partyMapper biz.PartyQueueMapper
	sink        biz.MatchEventSink
	deduper     biz.MatchSettleDeduper
	roster      biz.PartyRosterPublisher
}

// NewMatcherHandler 构造撮合 grpc 实现。
func NewMatcherHandler(d MatcherDeps) *MatcherHandler {
	return &MatcherHandler{
		svc:         d.Svc,
		party:       d.Party,
		mapper:      d.Mapper,
		partyMapper: d.PartyMapper,
		sink:        d.Sink,
		deduper:     d.Deduper,
		roster:      d.Roster,
	}
}

// QueueMatch 实现 biz.MatcherService：组装属性快照 ticket 入队并监听成局事件。
func (h *MatcherHandler) QueueMatch(ctx context.Context, req *matcherv1.QueueMatchRequest) (rep *matcherv1.QueueMatchReply, err error) {
	ctx, span := observability.StartSpan(ctx, "matcher.Matcher.QueueMatch",
		attribute.String("player.id", req.GetPlayerId()),
		attribute.String("ruleset", req.GetRuleset()))
	defer func() { observability.EndSpan(span, err) }()
	if req.GetPlayerId() == "" {
		return nil, errorv1.ErrInvalidParams("玩家不能为空")
	}
	// 已在匹配中：幂等拒绝（映射存在）。
	if tid, _ := h.mapper.Get(ctx, req.GetPlayerId()); tid != "" {
		return nil, errorv1.ErrAlreadyInMatch("已在匹配中")
	}
	name := req.GetRuleset()
	if name == "" {
		name = biz.DefaultMatchmakerName
	}
	// 规则集白名单：注册表当前仅 casual；扩规则时同步 infra.NewMatchmakerRuntime。
	if name != biz.DefaultMatchmakerName {
		return nil, errorv1.ErrInvalidParams("未知规则集: %s", name)
	}
	attrs := map[string]matchmaker.Attribute{
		"level": {Type: matchmaker.AttributeNumber, Number: float64(req.GetPlayer().GetLevel())},
	}
	ticketID, err := h.svc.StartMatchmaking(ctx, matchmaker.StartRequest{
		Matchmaker: name,
		Players: []matchmaker.Player{{
			ID:         req.GetPlayerId(),
			Attributes: attrs,
		}},
	})
	if err != nil {
		return nil, errorv1.ErrInternal("入队失败")
	}
	if err := h.mapper.Set(ctx, req.GetPlayerId(), ticketID, ticketTTL); err != nil {
		return nil, errorv1.ErrInternal("登记匹配映射失败")
	}
	// 后台监听 ticket 事件（成局发布/失败清理；单人票 partyID 为空）。
	go h.watchTicket(req.GetPlayerId(), ticketID, "")
	return &matcherv1.QueueMatchReply{TicketId: ticketID}, nil
}

// CancelMatch 实现 biz.MatcherService：按玩家定位 ticket 并取消。
func (h *MatcherHandler) CancelMatch(ctx context.Context, req *matcherv1.CancelMatchRequest) (rep *matcherv1.CancelMatchReply, err error) {
	ctx, span := observability.StartSpan(ctx, "matcher.Matcher.CancelMatch",
		attribute.String("player.id", req.GetPlayerId()))
	defer func() { observability.EndSpan(span, err) }()
	ticketID, err := h.mapper.Get(ctx, req.GetPlayerId())
	if err != nil {
		return nil, errorv1.ErrInternal("查询匹配映射失败")
	}
	if ticketID == "" {
		return &matcherv1.CancelMatchReply{Canceled: false}, nil
	}
	if err := h.svc.Cancel(ctx, ticketID); err != nil {
		return nil, errorv1.ErrInternal("取消匹配失败")
	}
	if err := h.mapper.Del(ctx, req.GetPlayerId()); err != nil {
		return nil, errorv1.ErrInternal("清理匹配映射失败")
	}
	return &matcherv1.CancelMatchReply{Canceled: true}, nil
}

// QueryMatch 实现 biz.MatcherService：映射 + 后端状态联合判定。
func (h *MatcherHandler) QueryMatch(ctx context.Context, req *matcherv1.QueryMatchRequest) (rep *matcherv1.QueryMatchReply, err error) {
	ctx, span := observability.StartSpan(ctx, "matcher.Matcher.QueryMatch",
		attribute.String("player.id", req.GetPlayerId()))
	defer func() { observability.EndSpan(span, err) }()
	ticketID, err := h.mapper.Get(ctx, req.GetPlayerId())
	if err != nil {
		return nil, errorv1.ErrInternal("查询匹配映射失败")
	}
	if ticketID == "" {
		return &matcherv1.QueryMatchReply{State: matcherv1.MatchState_MATCH_STATE_NONE}, nil
	}
	ticket, err := h.svc.Describe(ctx, ticketID)
	if err != nil {
		// 后端记录可能已过期：按无匹配处理。
		return &matcherv1.QueryMatchReply{State: matcherv1.MatchState_MATCH_STATE_NONE}, nil
	}
	state := mapTicketState(ticket.State)
	matchID, _ := h.mapper.GetMatch(ctx, req.GetPlayerId())
	battleID, _ := h.mapper.GetBattle(ctx, req.GetPlayerId())
	return &matcherv1.QueryMatchReply{
		State: state, TicketId: ticketID, MatchId: matchID, BattleId: battleID,
	}, nil
}

// watchTicket 监听 ticket 事件直至终态：
// 成局 → 发布事件 + 开局调用 + 写入全部参战玩家的对局关联；
// 失败/超时/取消 → 发布失败事件 + 清理映射。
// partyID 非空为整队票：失败事件的推送名单按队伍名册快照取（含已离队前的成员不可得，取剩余名册）。
func (h *MatcherHandler) watchTicket(playerID, ticketID, partyID string) {
	ctx, cancel := context.WithTimeout(context.Background(), ticketTTL)
	defer cancel()
	events, err := h.svc.Watch(ctx, ticketID)
	if err != nil {
		return
	}
	for ev := range events {
		switch ev.State {
		case matchmaker.TicketCompleted:
			h.onCompleted(ev)
			return
		case matchmaker.TicketFailed, matchmaker.TicketTimedOut, matchmaker.TicketCancelled:
			h.onFailed(playerID, ticketID, partyID, ev)
			return
		}
	}
}

// onCompleted 处理成局：按 matchID 幂等（同局每张 ticket 各触发一次，
// 仅首个处理者执行，见 biz.MatchSettleDeduper——去重前置避免双写关联）。
// 对局关联写入全部参战玩家（整队票含全队成员）。
func (h *MatcherHandler) onCompleted(ev matchmaker.TicketEvent) {
	if ev.Match == nil {
		return
	}
	ctx := context.Background()
	if h.deduper != nil {
		ok, err := h.deduper.TrySettle(ctx, ev.Match.ID, ticketTTL)
		if err != nil || !ok {
			return
		}
	}
	battleID := idgen.Battle()
	playerIDs := matchPlayerIDs(ev.Match)
	for _, pid := range playerIDs {
		_ = h.mapper.SetMatch(ctx, pid, ev.Match.ID, ticketTTL)
		// battle 关联：开局推送丢失后，玩家重登可经查询接口恢复加入。
		_ = h.mapper.SetBattle(ctx, pid, battleID, ticketTTL)
	}
	_ = h.sink.PublishStarted(ctx, battleID, ev.Match.ID, playerIDs)
	// 开局调用（可观测）：失败不阻断事件发布。
	_ = h.sink.Start(ctx, battleID, ev.Match.ID, playerIDs)
}

// onFailed 处理失败终态：发布失败事件（整队票按名册快照推送）并清理映射。
func (h *MatcherHandler) onFailed(playerID, ticketID, partyID string, ev matchmaker.TicketEvent) {
	playerIDs := []string{playerID}
	if partyID != "" {
		if info, err := h.party.Describe(context.Background(), partyID); err == nil && len(info.Players) > 0 {
			playerIDs = rosterPlayerIDs(info.Players)
		}
	}
	_ = h.sink.PublishFailed(context.Background(), ticketID, playerIDs, matchFailReason(ev))
	_ = h.mapper.Del(context.Background(), playerID)
}

// matchFailReason 把引擎终态/处置来源映射为协议失败原因枚举
// （引擎 Reason 为内部自由文本，仅在服务端日志保留）。
func matchFailReason(ev matchmaker.TicketEvent) matcherv1.MatchFailReason {
	switch {
	case ev.State == matchmaker.TicketTimedOut:
		return matcherv1.MatchFailReason_MATCH_FAIL_REASON_TIMEOUT
	case ev.State == matchmaker.TicketCancelled:
		return matcherv1.MatchFailReason_MATCH_FAIL_REASON_CANCELLED
	case strings.HasPrefix(ev.Reason, "acceptance_failed"):
		return matcherv1.MatchFailReason_MATCH_FAIL_REASON_ACCEPTANCE_FAILED
	case strings.HasPrefix(ev.Reason, "placement_failed"):
		return matcherv1.MatchFailReason_MATCH_FAIL_REASON_PLACEMENT_FAILED
	default:
		return matcherv1.MatchFailReason_MATCH_FAIL_REASON_FAILED
	}
}

// rosterPlayerIDs 汇总名册的玩家 ID 列表（保序）。
func rosterPlayerIDs(players []matchmaker.Player) []string {
	out := make([]string, 0, len(players))
	for _, p := range players {
		out = append(out, p.ID)
	}
	return out
}

// mapTicketState 把 matchmaker 状态映射为协议枚举。
func mapTicketState(s matchmaker.TicketState) matcherv1.MatchState {
	switch s {
	case matchmaker.TicketCompleted:
		return matcherv1.MatchState_MATCH_STATE_MATCHED
	case matchmaker.TicketFailed, matchmaker.TicketCancelled, matchmaker.TicketTimedOut:
		return matcherv1.MatchState_MATCH_STATE_FAILED
	default:
		return matcherv1.MatchState_MATCH_STATE_WAITING
	}
}

// matchPlayerIDs 收集对局全部玩家 ID。
func matchPlayerIDs(m *matchmaker.Match) []string {
	ids := make([]string, 0, len(m.Tickets))
	for _, t := range m.Tickets {
		ids = append(ids, t.PlayerIDs()...)
	}
	return ids
}

// 静态保证实现接口。
var _ biz.MatcherService = (*MatcherHandler)(nil)
