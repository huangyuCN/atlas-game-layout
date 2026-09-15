// Package biz 定义 game 服务的业务接口（根包只放接口，
// 实现位于 handler/，跨接口共享逻辑位于 usecase/）。
package biz

import (
	"context"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
)

// PlayerService 是玩家注册/登录业务接口（PlayerActor 经此接入业务逻辑）。
// 会话裁决已单点收敛到 Gateway（会话管理器）：game 侧只管玩家数据，不持 token 副本。
type PlayerService interface {
	// Register 注册：账号不存在则创建玩家（两段式：只建数据不建会话）。
	Register(ctx context.Context, req *gamev1.RegisterReq) (*gamev1.RegisterReply, error)
	// Login 登录：校验玩家数据与口令并回执摘要；会话建立与令牌裁决在 Gateway。
	Login(ctx context.Context, req *gamev1.LoginReq) (*gamev1.LoginReply, error)
}

// GameService 是玩家查询与背包业务接口（grpc/http 服务实现）。
type GameService interface {
	GetPlayer(ctx context.Context, req *gamev1.GetPlayerRequest) (*gamev1.GetPlayerReply, error)
	GetBackpack(ctx context.Context, req *gamev1.GetBackpackRequest) (*gamev1.GetBackpackReply, error)
	GrantItem(ctx context.Context, req *gamev1.GrantItemRequest) (*gamev1.GrantItemReply, error)
}

// PlayerStateAccess 是玩家状态访问接口：
// 实现经 actor 转发收敛到 PlayerActor（聚合根单写者，cow 前提）。
type PlayerStateAccess interface {
	// GrantItem 发放道具（聚合根 undo 写）。
	GrantItem(ctx context.Context, playerID string, itemID, count uint32, reason string) error
	// GetBackpack 读取背包快照。
	GetBackpack(ctx context.Context, playerID string) ([]*gamev1.BackpackItem, error)
	// GetPlayer 读取玩家摘要。
	GetPlayer(ctx context.Context, playerID string) (*commonv1.PlayerSummary, error)
}

// PlayerServiceOptions 是玩家业务服务的装配参数（handler 构造用）。
type PlayerServiceOptions struct {
	// NewPlayerID 是玩家 ID 生成器（nil 用 idgen 默认实现）。
	NewPlayerID func() string
}

// MatchmakerClient 是撮合域客户端接口（PlayerActor 的匹配/组队域用，
// infra 侧 gRPC 实现，经服务发现寻址 matcher 服务）。
type MatchmakerClient interface {
	// ---- 单人匹配 ----
	// Enter 入队：level 是聚合根权威属性（服务端填充，客户端不可伪造）。
	Enter(ctx context.Context, playerID string, level int32, ruleset string) error
	// Cancel 取消匹配：回执 canceled=false 表示本就未在队（幂等不视为错误）。
	Cancel(ctx context.Context, playerID string) (bool, error)
	// Status 查询匹配状态（matched 态回执 battle_id 供重登恢复）。
	Status(ctx context.Context, playerID string) (*matcherv1.QueryMatchReply, error)
	// ---- 灵活组队（1..N 人，名册权威在撮合域）----
	// Create 队长建队（属性权威）。
	Create(ctx context.Context, playerID string, level int32) (string, error)
	// Join 加入队伍（容量原子校验）。
	Join(ctx context.Context, partyID, playerID string, level int32) error
	// Leave 离开队伍。
	Leave(ctx context.Context, partyID, playerID string) error
	// Describe 名册快照。
	Describe(ctx context.Context, partyID string) (*matcherv1.PartyInfo, error)
	// Queue 队长发整队入队。
	Queue(ctx context.Context, partyID, ruleset string) (string, error)
}
