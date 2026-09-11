// Package actor 匹配域业务方法：EnterMatchQueue/CancelMatch/GetMatchStatus
// （实现 PlayerActorServer 接口的匹配部分）。匹配属性从聚合根权威填充
// （客户端不可伪造）；未登录错误上抛；业务错误经集群 error 通道往返。
package actor

import (
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas/contrib/actor/core"
)

// EnterMatchQueue 实现 gamev1.PlayerActorServer：入队匹配（Ask）。
// level 取聚合根权威数据；ruleset 空串由 matcher 取默认；
// 已在匹配中等业务错误原样透传；match 客户端未装配（降级）时内部错误。
func (p *PlayerActor) EnterMatchQueue(ctx core.ActorContext, req *gamev1.EnterMatchQueueActorReq) (*gamev1.EnterMatchQueueActorReply, error) {
	if p.match == nil {
		return nil, errorv1.ErrInternal("匹配服务不可用")
	}
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("入队匹配失败：玩家不在线")
	}
	if err := p.match.Enter(ctx.Context(), p.pid.UID(), p.player.Level, req.GetRuleset()); err != nil {
		return nil, err
	}
	return &gamev1.EnterMatchQueueActorReply{}, nil
}

// CancelMatch 实现 gamev1.PlayerActorServer：取消匹配（Ask，幂等）。
func (p *PlayerActor) CancelMatch(ctx core.ActorContext, _ *gamev1.CancelMatchActorReq) (*gamev1.CancelMatchActorReply, error) {
	if p.match == nil {
		return nil, errorv1.ErrInternal("匹配服务不可用")
	}
	canceled, err := p.match.Cancel(ctx.Context(), p.pid.UID())
	if err != nil {
		return nil, err
	}
	return &gamev1.CancelMatchActorReply{Canceled: canceled}, nil
}

// GetMatchStatus 实现 gamev1.PlayerActorServer：查询匹配状态（Ask，轮询兜底）。
func (p *PlayerActor) GetMatchStatus(ctx core.ActorContext, _ *gamev1.GetMatchStatusActorReq) (*gamev1.MatchStatusActorReply, error) {
	if p.match == nil {
		return nil, errorv1.ErrInternal("匹配服务不可用")
	}
	state, ticketID, matchID, err := p.match.Status(ctx.Context(), p.pid.UID())
	if err != nil {
		return nil, err
	}
	return &gamev1.MatchStatusActorReply{State: state, TicketId: ticketID, MatchId: matchID}, nil
}
