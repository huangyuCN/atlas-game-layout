package actor

import (
	"context"
	"fmt"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/usecase"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"google.golang.org/protobuf/proto"
)

// PlayerClient 是 biz.PlayerStateAccess 的实现：
// 经集群运行时 Ask PlayerActor（写收敛到聚合根单写者，cow 前提）。
type PlayerClient struct {
	rt *pkgactor.Runtime
}

// NewPlayerClient 构造玩家状态访问客户端。
func NewPlayerClient(rt *pkgactor.Runtime) *PlayerClient {
	return &PlayerClient{rt: rt}
}

// GrantItem 实现 biz.PlayerStateAccess（聚合根 undo 写）。
func (c *PlayerClient) GrantItem(ctx context.Context, playerID string, itemID, count uint32, reason string) error {
	reply := new(gamev1.GrantItemActorReply)
	if err := c.ask(ctx, playerID, &gamev1.PlayerActorMsg{
		Kind: &gamev1.PlayerActorMsg_GrantItem{GrantItem: &gamev1.GrantItemActorReq{ItemId: itemID, Count: count, Reason: reason}},
	}, reply); err != nil {
		return errorv1.ErrInternal("发放道具失败")
	}
	if !reply.GetOk() {
		return usecase.ActorReplyError(reply.GetErrorReason())
	}
	return nil
}

// GetBackpack 实现 biz.PlayerStateAccess（聚合根内存快照）。
func (c *PlayerClient) GetBackpack(ctx context.Context, playerID string) ([]*gamev1.BackpackItem, error) {
	reply := new(gamev1.GetBackpackActorReply)
	if err := c.ask(ctx, playerID, &gamev1.PlayerActorMsg{
		Kind: &gamev1.PlayerActorMsg_GetBackpack{GetBackpack: &gamev1.GetBackpackActorReq{}},
	}, reply); err != nil {
		return nil, errorv1.ErrInternal("查询背包失败")
	}
	if !reply.GetOk() {
		return nil, usecase.ActorReplyError(errorv1.ReasonPlayerNotOnline())
	}
	return reply.GetItems(), nil
}

// GetPlayer 实现 biz.PlayerStateAccess（聚合根内存快照）。
func (c *PlayerClient) GetPlayer(ctx context.Context, playerID string) (*commonv1.PlayerSummary, error) {
	reply := new(gamev1.GetPlayerActorReply)
	if err := c.ask(ctx, playerID, &gamev1.PlayerActorMsg{
		Kind: &gamev1.PlayerActorMsg_GetPlayer{GetPlayer: &gamev1.GetPlayerActorReq{}},
	}, reply); err != nil {
		return nil, errorv1.ErrInternal("查询玩家失败")
	}
	if !reply.GetOk() {
		return nil, usecase.ActorReplyError(errorv1.ReasonPlayerNotOnline())
	}
	return reply.GetPlayer(), nil
}

// ask 以玩家 PID 发起 proto 请求（懒激活 + 跨节点透明）。
func (c *PlayerClient) ask(ctx context.Context, playerID string, req, out proto.Message) error {
	pid, err := types.NewPID(consts.ActorTypePlayer, playerID)
	if err != nil {
		return fmt.Errorf("actor: 非法玩家 ID %q: %w", playerID, err)
	}
	return c.rt.AskProto(ctx, pid, req, out)
}

// 静态保证 PlayerClient 实现 biz.PlayerStateAccess。
var _ biz.PlayerStateAccess = (*PlayerClient)(nil)
