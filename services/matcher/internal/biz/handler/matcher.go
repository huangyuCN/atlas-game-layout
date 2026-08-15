// Package handler 提供 biz 各接口的实现（撮合 grpc：入队/取消/查询）。
package handler

import (
	"context"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/idgen"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas/matchmaker"
)

// ticketTTL 是玩家→ticket 映射与结算去重的租期（与 matchmaker 后端 ticket TTL 对齐）。
const ticketTTL = 5 * time.Minute

// MatcherHandler 是 biz.MatcherService 的实现（入队/取消/查询）。
type MatcherHandler struct {
	matcherv1.UnimplementedMatcherServer
	svc     matchmaker.Service
	mapper  biz.PlayerTicketMapper
	sink    biz.MatchEventSink
	deduper biz.MatchSettleDeduper
}

// NewMatcherHandler 构造撮合 grpc 实现。
func NewMatcherHandler(svc matchmaker.Service, mapper biz.PlayerTicketMapper, sink biz.MatchEventSink, deduper biz.MatchSettleDeduper) *MatcherHandler {
	return &MatcherHandler{svc: svc, mapper: mapper, sink: sink, deduper: deduper}
}

// QueueMatch 实现 biz.MatcherService：组装属性快照 ticket 入队并监听成局事件。
func (h *MatcherHandler) QueueMatch(ctx context.Context, req *matcherv1.QueueMatchRequest) (*matcherv1.QueueMatchReply, error) {
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
	// 后台监听 ticket 事件（成局发布/失败清理）。
	go h.watchTicket(req.GetPlayerId(), ticketID)
	return &matcherv1.QueueMatchReply{TicketId: ticketID}, nil
}

// CancelMatch 实现 biz.MatcherService：按玩家定位 ticket 并取消。
func (h *MatcherHandler) CancelMatch(ctx context.Context, req *matcherv1.CancelMatchRequest) (*matcherv1.CancelMatchReply, error) {
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
func (h *MatcherHandler) QueryMatch(ctx context.Context, req *matcherv1.QueryMatchRequest) (*matcherv1.QueryMatchReply, error) {
	ticketID, err := h.mapper.Get(ctx, req.GetPlayerId())
	if err != nil {
		return nil, errorv1.ErrInternal("查询匹配映射失败")
	}
	if ticketID == "" {
		return &matcherv1.QueryMatchReply{State: "none"}, nil
	}
	ticket, err := h.svc.Describe(ctx, ticketID)
	if err != nil {
		// 后端记录可能已过期：按无匹配处理。
		return &matcherv1.QueryMatchReply{State: "none"}, nil
	}
	state := mapTicketState(ticket.State)
	matchID, _ := h.mapper.GetMatch(ctx, req.GetPlayerId())
	return &matcherv1.QueryMatchReply{State: state, TicketId: ticketID, MatchId: matchID}, nil
}

// watchTicket 监听 ticket 事件直至终态：
// 成局 → 发布事件 + 开局调用 + 写入 match 关联；
// 失败/超时/取消 → 发布失败事件 + 清理映射。
func (h *MatcherHandler) watchTicket(playerID, ticketID string) {
	ctx, cancel := context.WithTimeout(context.Background(), ticketTTL)
	defer cancel()
	events, err := h.svc.Watch(ctx, ticketID)
	if err != nil {
		return
	}
	for ev := range events {
		switch ev.State {
		case matchmaker.TicketCompleted:
			h.onCompleted(playerID, ev)
			return
		case matchmaker.TicketFailed, matchmaker.TicketTimedOut, matchmaker.TicketCancelled:
			h.onFailed(playerID, ticketID, ev)
			return
		}
	}
}

// onCompleted 处理成局：按 matchID 幂等（同局每张 ticket 各触发一次，
// 仅首个处理者执行发布与开局，见 biz.MatchSettleDeduper）。
func (h *MatcherHandler) onCompleted(playerID string, ev matchmaker.TicketEvent) {
	if ev.Match == nil {
		return
	}
	ctx := context.Background()
	// 无论结算是否由本实例执行，都关联玩家与对局（查询回执用）。
	_ = h.mapper.SetMatch(ctx, playerID, ev.Match.ID, ticketTTL)
	// 结算去重：同局其它 ticket 的 TicketCompleted 直接跳过。
	if h.deduper != nil {
		ok, err := h.deduper.TrySettle(ctx, ev.Match.ID, ticketTTL)
		if err != nil || !ok {
			return
		}
	}
	battleID := idgen.Battle()
	playerIDs := matchPlayerIDs(ev.Match)
	_ = h.sink.PublishStarted(ctx, battleID, ev.Match.ID, playerIDs)
	// 开局调用（可观测）：失败不阻断事件发布（M7 battle 服务接入后自然成功）。
	_ = h.sink.Start(ctx, battleID, ev.Match.ID, playerIDs)
}

// onFailed 处理失败终态：发布失败事件并清理映射。
func (h *MatcherHandler) onFailed(playerID, ticketID string, ev matchmaker.TicketEvent) {
	reason := string(ev.State)
	if ev.Reason != "" {
		reason = ev.Reason
	}
	_ = h.sink.PublishFailed(context.Background(), ticketID, []string{playerID}, reason)
	_ = h.mapper.Del(context.Background(), playerID)
}

// mapTicketState 把 matchmaker 状态映射为协议状态。
func mapTicketState(s matchmaker.TicketState) string {
	switch s {
	case matchmaker.TicketCompleted:
		return "matched"
	case matchmaker.TicketFailed, matchmaker.TicketCancelled, matchmaker.TicketTimedOut:
		return "failed"
	default:
		return "waiting"
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
