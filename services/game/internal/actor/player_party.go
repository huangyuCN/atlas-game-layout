// Package actor 组队域业务方法：CreateParty/JoinParty/LeaveParty/GetParty/QueueParty
// （实现 PlayerServiceActorServer 接口的组队部分）。名册权威在撮合域（引擎 redis Party）；
// PlayerActor 只持「我所在的 partyID」在线态（纯在线会话语义：下线即离队，
// 不持久化到聚合根，避免把撮合域状态引入玩家数据模型）；
// 属性从聚合根权威填充（客户端不可伪造）。
package actor

import (
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/usecase"
	"github.com/huangyuCN/atlas/contrib/actor/core"
)

// partySnapshot 由名册回执组装客户端快照（info 为 nil 时回执空快照）。
func (p *PlayerActor) partySnapshot(info *matcherv1.PartyInfo) *gamev1.PartyReply {
	out := &gamev1.PartyReply{}
	if info == nil {
		return out
	}
	out.PartyId = info.GetPartyId()
	out.LeaderId = info.GetLeaderId()
	for _, m := range info.GetMembers() {
		out.Members = append(out.Members, &commonv1.PlayerSummary{
			PlayerId: m.GetPlayerId(),
			Level:    m.GetLevel(),
		})
	}
	return out
}

// CreateParty 实现 gamev1.PlayerServiceActorServer：建队（本玩家为队长，Ask）。
func (p *PlayerActor) CreateParty(ctx core.ActorContext, _ *gamev1.CreatePartyReq) (*gamev1.PartyReply, error) {
	if p.partyUnavailable() {
		return nil, errorv1.ErrInternal("匹配服务不可用")
	}
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("建队失败：玩家不在线")
	}
	if p.partyID != "" {
		return nil, errorv1.ErrAlreadyInParty("已在队伍中")
	}
	partyID, err := p.match.Create(ctx.Context(), p.pid.UID(), p.player.Level)
	if err != nil {
		return nil, err
	}
	p.partyID = partyID
	// 建队即队长为唯一成员：直接组装快照（免一次 Describe 往返）。
	return &gamev1.PartyReply{
		PartyId:  partyID,
		LeaderId: p.pid.UID(),
		Members:  []*commonv1.PlayerSummary{usecase.PlayerSummary(p.player)},
	}, nil
}

// JoinParty 实现 gamev1.PlayerServiceActorServer：按 party_id 加入（Ask，容量原子校验）。
func (p *PlayerActor) JoinParty(ctx core.ActorContext, req *gamev1.JoinPartyReq) (*gamev1.PartyReply, error) {
	if p.partyUnavailable() {
		return nil, errorv1.ErrInternal("匹配服务不可用")
	}
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("加入队伍失败：玩家不在线")
	}
	if p.partyID != "" {
		return nil, errorv1.ErrAlreadyInParty("已在队伍中")
	}
	if err := p.match.Join(ctx.Context(), req.GetPartyId(), p.pid.UID(), p.player.Level); err != nil {
		return nil, err
	}
	p.partyID = req.GetPartyId()
	info, err := p.match.Describe(ctx.Context(), p.partyID)
	if err != nil {
		return nil, err
	}
	return p.partySnapshot(info), nil
}

// LeaveParty 实现 gamev1.PlayerServiceActorServer：离开队伍（Ask，幂等）。
// 不在队直接回执空快照；队长离开顺延、空队解散由撮合域保证。
func (p *PlayerActor) LeaveParty(ctx core.ActorContext, _ *gamev1.LeavePartyReq) (*gamev1.PartyReply, error) {
	if p.partyUnavailable() {
		return nil, errorv1.ErrInternal("匹配服务不可用")
	}
	if p.partyID == "" {
		return p.partySnapshot(nil), nil
	}
	if err := p.match.Leave(ctx.Context(), p.partyID, p.pid.UID()); err != nil {
		return nil, err
	}
	p.partyID = ""
	return p.partySnapshot(nil), nil
}

// GetParty 实现 gamev1.PlayerServiceActorServer：名册快照（Ask，轮询兜底）。
func (p *PlayerActor) GetParty(ctx core.ActorContext, _ *gamev1.GetPartyReq) (*gamev1.PartyReply, error) {
	if p.partyUnavailable() {
		return nil, errorv1.ErrInternal("匹配服务不可用")
	}
	if p.partyID == "" {
		return p.partySnapshot(nil), nil
	}
	info, err := p.match.Describe(ctx.Context(), p.partyID)
	if err != nil {
		return nil, err
	}
	return p.partySnapshot(info), nil
}

// QueueParty 实现 gamev1.PlayerServiceActorServer：队长发整队入队（Ask）。
// 1..N 人都可入队（不要求满员），等对面凑齐等量人数。
func (p *PlayerActor) QueueParty(ctx core.ActorContext, req *gamev1.QueuePartyReq) (*gamev1.PartyQueueReply, error) {
	if p.partyUnavailable() {
		return nil, errorv1.ErrInternal("匹配服务不可用")
	}
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("整队入队失败：玩家不在线")
	}
	if p.partyID == "" {
		return nil, errorv1.ErrNotInParty("整队入队失败：未组队")
	}
	ticketID, err := p.match.Queue(ctx.Context(), p.partyID, req.GetRuleset())
	if err != nil {
		return nil, err
	}
	return &gamev1.PartyQueueReply{TicketId: ticketID}, nil
}

// partyUnavailable 判断撮合客户端是否未装配（nil 降级：组队/匹配方法返回内部错误）。
func (p *PlayerActor) partyUnavailable() bool {
	return p.match == nil
}
