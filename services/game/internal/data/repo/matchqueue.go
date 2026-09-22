// Package repo 的 matcher 服务 gRPC 客户端（本文件）：实现 biz.MatchmakerClient，
// 经 etcd 服务发现寻址（discovery:///atlas.matcher），game 节点无须静态配置 matcher 地址。
package repo

import (
	"context"
	"fmt"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas/registry"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
)

// discoveryTarget 是 matcher 服务的发现寻址目标（discovery scheme + 服务名）。
const discoveryTarget = "discovery:///" + consts.ServiceMatcher

// MatchQueueGRPC 是 biz.MatchmakerClient 的 gRPC 实现。
type MatchQueueGRPC struct {
	cli matcherv1.MatcherClient
}

// NewMatchQueueGRPC 构造 matcher gRPC 客户端（服务发现寻址）；
// 发现器与客户端中间件由装配层提供——后者注入 traceparent，使 game→matcher 落在同一条链路。
func NewMatchQueueGRPC(ctx context.Context, discovery registry.Discovery,
	mws serverutil.ClientMiddlewares) (*MatchQueueGRPC, error) {
	conn, err := atlasgrpc.DialInsecure(ctx,
		atlasgrpc.WithDiscovery(discovery),
		atlasgrpc.WithEndpoint(discoveryTarget),
		atlasgrpc.WithMiddleware(mws...),
	)
	if err != nil {
		return nil, fmt.Errorf("repo: 连接 matcher 服务失败: %w", err)
	}
	return &MatchQueueGRPC{cli: matcherv1.NewMatcherClient(conn)}, nil
}

// 静态保证实现接口。
var _ biz.MatchmakerClient = (*MatchQueueGRPC)(nil)

// Enter 实现 biz.MatchmakerClient：入队（level 为聚合根权威属性）。
func (m *MatchQueueGRPC) Enter(ctx context.Context, playerID string, level int32, ruleset string) error {
	_, err := m.cli.QueueMatch(ctx, &matcherv1.QueueMatchRequest{
		PlayerId: playerID,
		Player:   &commonv1.PlayerSummary{PlayerId: playerID, Level: level},
		Ruleset:  ruleset,
	})
	return err
}

// Cancel 实现 biz.MatchmakerClient：取消匹配（未在队回执 canceled=false 不视为错误）。
func (m *MatchQueueGRPC) Cancel(ctx context.Context, playerID string) (bool, error) {
	rep, err := m.cli.CancelMatch(ctx, &matcherv1.CancelMatchRequest{PlayerId: playerID})
	if err != nil {
		return false, err
	}
	return rep.GetCanceled(), nil
}

// Status 实现 biz.MatchmakerClient：查询匹配状态（matched 态含 battle_id）。
func (m *MatchQueueGRPC) Status(ctx context.Context, playerID string) (*matcherv1.QueryMatchReply, error) {
	return m.cli.QueryMatch(ctx, &matcherv1.QueryMatchRequest{PlayerId: playerID})
}

// ---- 灵活组队（1..N 人）----

// Create 实现 biz.MatchmakerClient：队长建队（属性权威）。
func (m *MatchQueueGRPC) Create(ctx context.Context, playerID string, level int32) (string, error) {
	rep, err := m.cli.CreateParty(ctx, &matcherv1.CreatePartyRequest{
		PlayerId: playerID,
		Player:   &commonv1.PlayerSummary{PlayerId: playerID, Level: level},
	})
	if err != nil {
		return "", err
	}
	return rep.GetPartyId(), nil
}

// Join 实现 biz.MatchmakerClient：加入队伍（容量原子校验）。
func (m *MatchQueueGRPC) Join(ctx context.Context, partyID, playerID string, level int32) error {
	_, err := m.cli.JoinParty(ctx, &matcherv1.JoinPartyRequest{
		PartyId:  partyID,
		PlayerId: playerID,
		Player:   &commonv1.PlayerSummary{PlayerId: playerID, Level: level},
	})
	return err
}

// Leave 实现 biz.MatchmakerClient：离开队伍。
func (m *MatchQueueGRPC) Leave(ctx context.Context, partyID, playerID string) error {
	_, err := m.cli.LeaveParty(ctx, &matcherv1.LeavePartyRequest{PartyId: partyID, PlayerId: playerID})
	return err
}

// Describe 实现 biz.MatchmakerClient：名册快照。
func (m *MatchQueueGRPC) Describe(ctx context.Context, partyID string) (*matcherv1.PartyInfo, error) {
	return m.cli.DescribeParty(ctx, &matcherv1.DescribePartyRequest{PartyId: partyID})
}

// Queue 实现 biz.MatchmakerClient：队长发整队入队。
func (m *MatchQueueGRPC) Queue(ctx context.Context, partyID, ruleset string) (string, error) {
	rep, err := m.cli.QueueParty(ctx, &matcherv1.QueuePartyRequest{PartyId: partyID, Ruleset: ruleset})
	if err != nil {
		return "", err
	}
	return rep.GetTicketId(), nil
}
