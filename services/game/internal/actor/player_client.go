package actor

import (
	"context"
	"fmt"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// PlayerClient 是 biz.PlayerStateAccess 的实现：
// 经生成的 client stub 调用 PlayerActor（写收敛到聚合根单写者，cow 前提）。
// 业务错误直接以 Go error 返回（code/reason 经集群 error 通道往返保留），
// 调用方按 atlas errors.Reason 判定还原语义。
type PlayerClient struct {
	cli *gamev1.PlayerActorClient
}

// NewPlayerClient 构造玩家状态访问客户端。
func NewPlayerClient(rt *pkgactor.Runtime) *PlayerClient {
	return &PlayerClient{cli: gamev1.NewPlayerActorClient(rt)}
}

// GrantItem 实现 biz.PlayerStateAccess（聚合根 undo 写）。
func (c *PlayerClient) GrantItem(ctx context.Context, playerID string, itemID, count uint32, reason string) error {
	pid, err := c.playerPID(playerID)
	if err != nil {
		return err
	}
	req := &gamev1.GrantItemActorReq{ItemId: itemID, Count: count, Reason: reason}
	if _, err := c.cli.GrantItem(ctx, pid, req); err != nil {
		return err
	}
	return nil
}

// GetBackpack 实现 biz.PlayerStateAccess（聚合根内存快照）。
func (c *PlayerClient) GetBackpack(ctx context.Context, playerID string) ([]*gamev1.BackpackItem, error) {
	pid, err := c.playerPID(playerID)
	if err != nil {
		return nil, err
	}
	reply, err := c.cli.GetBackpack(ctx, pid, &gamev1.GetBackpackActorReq{})
	if err != nil {
		return nil, err
	}
	return reply.GetItems(), nil
}

// GetPlayer 实现 biz.PlayerStateAccess（聚合根内存快照）。
func (c *PlayerClient) GetPlayer(ctx context.Context, playerID string) (*commonv1.PlayerSummary, error) {
	pid, err := c.playerPID(playerID)
	if err != nil {
		return nil, err
	}
	reply, err := c.cli.GetPlayer(ctx, pid, &gamev1.GetPlayerActorReq{})
	if err != nil {
		return nil, err
	}
	return reply.GetPlayer(), nil
}

// playerPID 以玩家 ID 构造 PlayerActor PID（懒激活 + 跨节点透明）。
func (c *PlayerClient) playerPID(playerID string) (types.PID, error) {
	pid, err := types.NewPID(consts.ActorTypePlayer, playerID)
	if err != nil {
		return types.PID{}, fmt.Errorf("actor: 非法玩家 ID %q: %w", playerID, err)
	}
	return pid, nil
}

// 静态保证 PlayerClient 实现 biz.PlayerStateAccess。
var _ biz.PlayerStateAccess = (*PlayerClient)(nil)
