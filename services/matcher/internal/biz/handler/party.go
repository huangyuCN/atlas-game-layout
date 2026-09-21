// Package handler 灵活组队域的 grpc 实现：
// 建队/加入/离开/名册查询/整队入队。名册权威在撮合域（引擎 redis Party），
// 属性由各成员自己的 PlayerActor 权威填充；成员变更发布名册事件（gateway 推送全队）。
package handler

import (
	"context"
	"errors"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	getlog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/matchmaker"
	"go.opentelemetry.io/otel/attribute"
)

// partyAttrs 由等级构造匹配属性（属性名 "level"，与单人入队共用约定）。
func partyAttrs(level int32) map[string]matchmaker.Attribute {
	return map[string]matchmaker.Attribute{
		"level": {Type: matchmaker.AttributeNumber, Number: float64(level)},
	}
}

// publishRoster 发布名册变更事件（roster 未装配时跳过；失败仅忽略不阻断业务）。
func (h *MatcherHandler) publishRoster(ctx context.Context, partyID, leaderID string, playerIDs []string, reason matcherv1.PartyRosterReason) {
	if h.roster == nil {
		return
	}
	if err := h.roster.PublishRoster(ctx, partyID, leaderID, playerIDs, reason); err != nil {
		getlog.GetLogger().Error("matcher: 名册事件发布失败", "party", partyID, "err", err)
	} else {
		getlog.GetLogger().Info("matcher: 名册事件已发布", "party", partyID, "reason", reason.String())
	}
}

// CreateParty 实现 matcherv1.MatcherServer：队长建队。
func (h *MatcherHandler) CreateParty(ctx context.Context, req *matcherv1.CreatePartyRequest) (rep *matcherv1.CreatePartyReply, err error) {
	ctx, span := observability.StartSpan(ctx, "matcher.Matcher.CreateParty",
		attribute.String("player.id", req.GetPlayerId()))
	defer func() { observability.EndSpan(span, err) }()
	if h.party == nil {
		return nil, errorv1.ErrInternal("组队服务不可用")
	}
	if req.GetPlayerId() == "" || req.GetPlayer() == nil {
		return nil, errorv1.ErrInvalidParams("玩家与属性不能为空")
	}
	leader := matchmaker.Player{ID: req.GetPlayerId(), Attributes: partyAttrs(req.GetPlayer().GetLevel())}
	partyID, err := h.party.Create(ctx, leader, nil)
	if err != nil {
		return nil, errorv1.ErrInternal("建队失败")
	}
	h.publishRoster(ctx, partyID, req.GetPlayerId(), []string{req.GetPlayerId()}, matcherv1.PartyRosterReason_PARTY_ROSTER_REASON_CREATED)
	return &matcherv1.CreatePartyReply{PartyId: partyID}, nil
}

// JoinParty 实现 matcherv1.MatcherServer：按 party_id 加入队伍（容量原子校验）。
func (h *MatcherHandler) JoinParty(ctx context.Context, req *matcherv1.JoinPartyRequest) (rep *matcherv1.JoinPartyReply, err error) {
	ctx, span := observability.StartSpan(ctx, "matcher.Matcher.JoinParty",
		attribute.String("player.id", req.GetPlayerId()),
		attribute.String("party.id", req.GetPartyId()))
	defer func() { observability.EndSpan(span, err) }()
	if h.party == nil {
		return nil, errorv1.ErrInternal("组队服务不可用")
	}
	if req.GetPlayerId() == "" || req.GetPlayer() == nil {
		return nil, errorv1.ErrInvalidParams("玩家与属性不能为空")
	}
	player := matchmaker.Player{ID: req.GetPlayerId(), Attributes: partyAttrs(req.GetPlayer().GetLevel())}
	if err := h.party.Join(ctx, req.GetPartyId(), player); err != nil {
		return nil, partyError(err, "加入队伍失败")
	}
	info, err := h.party.Describe(ctx, req.GetPartyId())
	if err == nil {
		h.publishRoster(ctx, info.PartyID, info.LeaderID, rosterPlayerIDs(info.Players), matcherv1.PartyRosterReason_PARTY_ROSTER_REASON_JOIN)
	}
	return &matcherv1.JoinPartyReply{}, nil
}

// LeaveParty 实现 matcherv1.MatcherServer：离开队伍。
// 若整队正在匹配中：先取消整队票（终态事件异步推送剩余成员），再移出名册。
func (h *MatcherHandler) LeaveParty(ctx context.Context, req *matcherv1.LeavePartyRequest) (rep *matcherv1.LeavePartyReply, err error) {
	ctx, span := observability.StartSpan(ctx, "matcher.Matcher.LeaveParty",
		attribute.String("player.id", req.GetPlayerId()),
		attribute.String("party.id", req.GetPartyId()))
	defer func() { observability.EndSpan(span, err) }()
	if h.party == nil || h.partyMapper == nil {
		return nil, errorv1.ErrInternal("组队服务不可用")
	}
	if ticketID, _ := h.partyMapper.GetPartyTicket(ctx, req.GetPartyId()); ticketID != "" {
		_ = h.svc.Cancel(ctx, ticketID)
		_ = h.partyMapper.DelPartyTicket(ctx, req.GetPartyId())
	}
	if err := h.party.Leave(ctx, req.GetPartyId(), req.GetPlayerId()); err != nil {
		return nil, partyError(err, "离开队伍失败")
	}
	info, derr := h.party.Describe(ctx, req.GetPartyId())
	if derr != nil {
		// 名册已不存在：队伍解散。
		h.publishRoster(ctx, req.GetPartyId(), req.GetPlayerId(), nil, matcherv1.PartyRosterReason_PARTY_ROSTER_REASON_DISSOLVE)
		return &matcherv1.LeavePartyReply{}, nil
	}
	h.publishRoster(ctx, info.PartyID, info.LeaderID, rosterPlayerIDs(info.Players), matcherv1.PartyRosterReason_PARTY_ROSTER_REASON_LEAVE)
	return &matcherv1.LeavePartyReply{}, nil
}

// DescribeParty 实现 matcherv1.MatcherServer：名册快照（轮询兜底）。
func (h *MatcherHandler) DescribeParty(ctx context.Context, req *matcherv1.DescribePartyRequest) (rep *matcherv1.PartyInfo, err error) {
	ctx, span := observability.StartSpan(ctx, "matcher.Matcher.DescribeParty",
		attribute.String("party.id", req.GetPartyId()))
	defer func() { observability.EndSpan(span, err) }()
	if h.party == nil {
		return nil, errorv1.ErrInternal("组队服务不可用")
	}
	info, err := h.party.Describe(ctx, req.GetPartyId())
	if err != nil {
		return nil, partyError(err, "查询队伍失败")
	}
	members := make([]*matcherv1.PartyMember, 0, len(info.Players))
	for _, p := range info.Players {
		level := int32(0)
		if a, ok := p.Attributes["level"]; ok && a.Type == matchmaker.AttributeNumber {
			level = int32(a.Number)
		}
		members = append(members, &matcherv1.PartyMember{PlayerId: p.ID, Level: level})
	}
	return &matcherv1.PartyInfo{
		PartyId:  info.PartyID,
		LeaderId: info.LeaderID,
		Members:  members,
	}, nil
}

// QueueParty 实现 matcherv1.MatcherServer：队长发整队入队（1..N 人都可入队）。
func (h *MatcherHandler) QueueParty(ctx context.Context, req *matcherv1.QueuePartyRequest) (rep *matcherv1.QueuePartyReply, err error) {
	ctx, span := observability.StartSpan(ctx, "matcher.Matcher.QueueParty",
		attribute.String("party.id", req.GetPartyId()),
		attribute.String("ruleset", req.GetRuleset()))
	defer func() { observability.EndSpan(span, err) }()
	if h.party == nil || h.partyMapper == nil {
		return nil, errorv1.ErrInternal("组队服务不可用")
	}
	// 重复整队入队拒绝（队伍已有活动票）。
	if ticketID, _ := h.partyMapper.GetPartyTicket(ctx, req.GetPartyId()); ticketID != "" {
		return nil, errorv1.ErrAlreadyInMatch("队伍已在匹配中")
	}
	name := req.GetRuleset()
	if name == "" {
		name = biz.DefaultMatchmakerName
	}
	// 规则集白名单：与单人入队共用（注册表当前仅 casual；扩规则时同步 infra）。
	if name != biz.DefaultMatchmakerName {
		return nil, errorv1.ErrInvalidParams("未知规则集: %s", name)
	}
	ticketID, err := h.party.EnqueueAsParty(ctx, req.GetPartyId(), name)
	if err != nil {
		return nil, partyError(err, "整队入队失败")
	}
	if err := h.partyMapper.SetPartyTicket(ctx, req.GetPartyId(), ticketID, ticketTTL); err != nil {
		return nil, errorv1.ErrInternal("登记队伍票映射失败")
	}
	// 后台监听整队票终态（成局/失败推送全队）。
	leaderID := ""
	if info, derr := h.party.Describe(ctx, req.GetPartyId()); derr == nil {
		leaderID = info.LeaderID
	}
	go h.watchTicket(leaderID, ticketID, req.GetPartyId())
	return &matcherv1.QueuePartyReply{TicketId: ticketID}, nil
}

// partyError 把引擎哨兵错误映射为结构化业务错误（默认内部错误）。
func partyError(err error, fallbackMsg string) error {
	switch {
	case errors.Is(err, matchmaker.ErrPartyNotFound):
		return errorv1.ErrPartyNotFound("队伍不存在或已解散")
	case errors.Is(err, matchmaker.ErrPartyFull):
		return errorv1.ErrPartyFull("队伍已满")
	case errors.Is(err, matchmaker.ErrPlayerNotInParty):
		return errorv1.ErrNotInParty("玩家不在队伍中")
	default:
		return errorv1.ErrInternal("%s", fallbackMsg)
	}
}
