// Package actor 背包/查询域业务方法：GrantItem/GetBackpack/GetPlayer（实现 PlayerServiceServer 接口
// 的背包部分）。依赖内存聚合根（登录后持有）；未登录错误上抛（结构化 error）。
package actor

import (
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/usecase"
	"github.com/huangyuCN/atlas/contrib/actor/core"
)

// GrantItem 实现 gamev1.PlayerServiceServer：聚合根 undo 写（未登录拒绝）。
func (p *PlayerActor) GrantItem(_ core.ActorContext, req *gamev1.GrantItemReq) (*gamev1.GrantItemReply, error) {
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("发放道具失败：玩家不在线")
	}
	p.player.GrantItem(req.GetItemId(), req.GetCount())
	return &gamev1.GrantItemReply{}, nil
}

// GetBackpack 实现 gamev1.PlayerServiceServer：背包查询（聚合根内存快照）。
func (p *PlayerActor) GetBackpack(_ core.ActorContext, _ *gamev1.GetBackpackReq) (*gamev1.BackpackReply, error) {
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("查询背包失败：玩家不在线")
	}
	return &gamev1.BackpackReply{Items: usecase.BackpackItems(p.player)}, nil
}

// GetPlayer 实现 gamev1.PlayerServiceServer：玩家摘要查询（聚合根内存快照）。
func (p *PlayerActor) GetPlayer(_ core.ActorContext, _ *gamev1.GetPlayerReq) (*gamev1.PlayerReply, error) {
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("查询玩家失败：玩家不在线")
	}
	return &gamev1.PlayerReply{Player: usecase.PlayerSummary(p.player)}, nil
}

// GetPlayerData 实现 gamev1.PlayerServiceServer：玩家数据全量快照（摘要 + 背包，
// 登录后客户端调用一次完成全量对齐；聚合根内存快照，未登录拒绝）。
func (p *PlayerActor) GetPlayerData(_ core.ActorContext, _ *gamev1.GetPlayerDataReq) (*gamev1.PlayerDataReply, error) {
	if p.player == nil {
		return nil, errorv1.ErrPlayerNotOnline("数据同步失败：玩家不在线")
	}
	return &gamev1.PlayerDataReply{
		Player: usecase.PlayerSummary(p.player),
		Items:  usecase.BackpackItems(p.player),
	}, nil
}
