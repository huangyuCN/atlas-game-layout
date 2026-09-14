// Package server 玩家数据同步域 handler：GetPlayerData（GatewayPlayer 协议实现）。
// 登录轻回执的补充面：客户端登录成功后调用一次，聚合 actor 内存权威态
// （摘要 + 背包）完成全量对齐；服务端数据变更的主动推送由后续演进提供。
package server

import (
	"context"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
)

// GetPlayerData 玩家数据同步：会话校验 → actor 聚合查询（摘要 + 背包内存快照）。
func (g *Gateway) GetPlayerData(ctx context.Context, req *gatewayv1.PlayerDataRequest) (*gatewayv1.PlayerDataReply, error) {
	pid, err := g.playerCall(ctx, req.GetPlayerId(), req.GetToken(), "玩家数据同步")
	if err != nil {
		return nil, err
	}
	prep, err := g.players.GetPlayer(ctx, pid, &gamev1.GetPlayerActorReq{})
	if err != nil {
		return nil, err // PLAYER_NOT_ONLINE 等业务错误透传（产生点即语义）
	}
	brep, err := g.players.GetBackpack(ctx, pid, &gamev1.GetBackpackActorReq{})
	if err != nil {
		return nil, err
	}
	return &gatewayv1.PlayerDataReply{
		Player: prep.GetPlayer(),
		Items:  brep.GetItems(),
	}, nil
}
