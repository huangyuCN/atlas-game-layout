package handler

import (
	"context"
	"errors"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
)

// GameHandler 是 biz.GameService 的实现：
// 查询与背包写操作全部经 PlayerStateAccess 转发 PlayerActor
// （聚合根单写者收敛，配合 cow undo 语义）。
type GameHandler struct {
	gamev1.UnimplementedPlayerServer
	players biz.PlayerStateAccess
}

// NewGameHandler 构造玩家业务 grpc 实现。
func NewGameHandler(players biz.PlayerStateAccess) *GameHandler {
	return &GameHandler{players: players}
}

// GetPlayer 实现 biz.GameService（聚合根内存快照）。
func (h *GameHandler) GetPlayer(ctx context.Context, req *gamev1.GetPlayerRequest) (*gamev1.GetPlayerReply, error) {
	summary, err := h.players.GetPlayer(ctx, req.GetPlayerId())
	if errors.Is(err, repo.ErrPlayerNotFound) {
		return nil, errorv1.ErrPlayerNotFound("玩家不存在")
	}
	if err != nil {
		return nil, err // 实现层（actor 客户端）已将回执 reason 映射为结构化错误
	}
	return &gamev1.GetPlayerReply{Player: summary}, nil
}

// GetBackpack 实现 biz.GameService（聚合根内存快照）。
func (h *GameHandler) GetBackpack(ctx context.Context, req *gamev1.GetBackpackRequest) (*gamev1.GetBackpackReply, error) {
	items, err := h.players.GetBackpack(ctx, req.GetPlayerId())
	if errors.Is(err, repo.ErrPlayerNotFound) {
		return nil, errorv1.ErrPlayerNotFound("玩家不存在")
	}
	if err != nil {
		return nil, err
	}
	return &gamev1.GetBackpackReply{Items: items}, nil
}

// GrantItem 实现 biz.GameService（聚合根 undo 写）。
func (h *GameHandler) GrantItem(ctx context.Context, req *gamev1.GrantItemRequest) (*gamev1.GrantItemReply, error) {
	if req.GetPlayerId() == "" || req.GetItemId() == 0 || req.GetCount() == 0 {
		return nil, errorv1.ErrInvalidParams("玩家/道具/数量不能为空")
	}
	if err := h.players.GrantItem(ctx, req.GetPlayerId(), req.GetItemId(), req.GetCount(), req.GetReason()); err != nil {
		return nil, err
	}
	return &gamev1.GrantItemReply{}, nil
}

// 静态保证 GameHandler 实现 biz.GameService。
var _ biz.GameService = (*GameHandler)(nil)
