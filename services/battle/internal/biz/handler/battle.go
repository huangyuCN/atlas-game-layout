// Package handler 提供 battle 服务 grpc 接口的实现（开局/查询，经 actor 转发）。
package handler

import (
	"context"
	"fmt"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// BattleHandler 是战斗管理 grpc 实现：
// 开局/查询经生成的 client stub 转发到战斗 actor（懒激活）。
type BattleHandler struct {
	battlev1.UnimplementedBattleServer
	cli *battlev1.BattleActorClient
}

// NewBattleHandler 构造战斗管理实现。
func NewBattleHandler(rt *pkgactor.Runtime) *BattleHandler {
	return &BattleHandler{cli: battlev1.NewBattleActorClient(rt)}
}

// battlePID 以战斗 ID 构造战斗 actor PID（懒激活 + 跨节点透明）。
func battlePID(battleID string) (types.PID, error) {
	pid, err := types.NewPID(consts.ActorTypeBattle, battleID)
	if err != nil {
		return types.PID{}, fmt.Errorf("handler: 非法战斗 ID %q: %w", battleID, err)
	}
	return pid, nil
}

// CreateBattle 实现 grpc：开局（matcher 成局后也可直接 actor 调用）。
func (h *BattleHandler) CreateBattle(ctx context.Context, req *battlev1.CreateBattleRequest) (*battlev1.CreateBattleReply, error) {
	pid, err := battlePID(req.GetMatchId())
	if err != nil {
		return nil, err
	}
	reply, err := h.cli.Create(ctx, pid, req)
	if err != nil {
		return nil, fmt.Errorf("handler: 战斗请求失败: %w", err)
	}
	return reply, nil
}

// GetBattle 实现 grpc：状态查询。
func (h *BattleHandler) GetBattle(ctx context.Context, req *battlev1.GetBattleRequest) (*battlev1.GetBattleReply, error) {
	pid, err := battlePID(req.GetBattleId())
	if err != nil {
		return nil, err
	}
	st, err := h.cli.GetState(ctx, pid, &battlev1.GetStateReq{})
	if err != nil {
		return nil, fmt.Errorf("handler: 战斗请求失败: %w", err)
	}
	return &battlev1.GetBattleReply{State: st.GetState()}, nil
}
