// Package handler 提供 battle 服务 grpc 接口的实现（开局/查询，经 actor 转发）。
package handler

import (
	"context"
	"fmt"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"google.golang.org/protobuf/proto"
)

// BattleHandler 是战斗管理 grpc 实现：
// 开局/查询经 actor 集群转发到战斗 actor（懒激活）。
type BattleHandler struct {
	battlev1.UnimplementedBattleServer
	rt *pkgactor.Runtime
}

// NewBattleHandler 构造战斗管理实现。
func NewBattleHandler(rt *pkgactor.Runtime) *BattleHandler {
	return &BattleHandler{rt: rt}
}

// CreateBattle 实现 grpc：开局（懒激活战斗 actor，matcher 成局后也可直接 actor 调用）。
func (h *BattleHandler) CreateBattle(ctx context.Context, req *battlev1.CreateBattleRequest) (*battlev1.CreateBattleReply, error) {
	out := new(battlev1.CreateBattleReply)
	if err := h.ask(ctx, req.GetMatchId(), &battlev1.BattleActorMsg{
		Kind: &battlev1.BattleActorMsg_Create{Create: req},
	}, out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetBattle 实现 grpc：状态查询。
func (h *BattleHandler) GetBattle(ctx context.Context, req *battlev1.GetBattleRequest) (*battlev1.GetBattleReply, error) {
	st := new(battlev1.GetStateReply)
	if err := h.ask(ctx, req.GetBattleId(), &battlev1.BattleActorMsg{
		Kind: &battlev1.BattleActorMsg_GetState{GetState: &battlev1.GetStateReq{}},
	}, st); err != nil {
		return nil, err
	}
	return &battlev1.GetBattleReply{State: st.GetState()}, nil
}

// ask 以战斗 actor PID 发起 proto 请求（懒激活 + 跨节点透明）。
func (h *BattleHandler) ask(ctx context.Context, battleID string, req, out proto.Message) error {
	pid, err := types.NewPID(consts.ActorTypeBattle, battleID)
	if err != nil {
		return fmt.Errorf("handler: 非法战斗 ID %q: %w", battleID, err)
	}
	if err := h.rt.AskProto(ctx, pid, req, out); err != nil {
		return fmt.Errorf("handler: 战斗请求失败: %w", err)
	}
	return nil
}
