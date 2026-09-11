// Package repo 的 matcher 服务 gRPC 客户端（本文件）：实现 biz.MatchQueueClient，
// 经 etcd 服务发现寻址（discovery:///atlas.matcher），game 节点无须静态配置 matcher 地址。
package repo

import (
	"context"
	"fmt"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// discoveryTarget 是 matcher 服务的发现寻址目标（discovery scheme + 服务名）。
const discoveryTarget = "discovery:///" + consts.ServiceMatcher

// MatchQueueGRPC 是 biz.MatchQueueClient 的 gRPC 实现。
type MatchQueueGRPC struct {
	cli matcherv1.MatcherClient
}

// NewMatchQueueGRPC 构造 matcher gRPC 客户端（服务发现寻址）。
func NewMatchQueueGRPC(ctx context.Context, ec *clientv3.Client) (*MatchQueueGRPC, error) {
	discovery, err := registry.NewEtcdDiscovery(ec, registry.Options{})
	if err != nil {
		return nil, fmt.Errorf("repo: 构造服务发现失败: %w", err)
	}
	conn, err := atlasgrpc.DialInsecure(ctx,
		atlasgrpc.WithDiscovery(discovery),
		atlasgrpc.WithEndpoint(discoveryTarget),
	)
	if err != nil {
		return nil, fmt.Errorf("repo: 连接 matcher 服务失败: %w", err)
	}
	return &MatchQueueGRPC{cli: matcherv1.NewMatcherClient(conn)}, nil
}

// Enter 实现 biz.MatchQueueClient：入队（level 为聚合根权威属性）。
func (m *MatchQueueGRPC) Enter(ctx context.Context, playerID string, level int32, ruleset string) error {
	_, err := m.cli.QueueMatch(ctx, &matcherv1.QueueMatchRequest{
		PlayerId: playerID,
		Player:   &commonv1.PlayerSummary{PlayerId: playerID, Level: level},
		Ruleset:  ruleset,
	})
	return err
}

// Cancel 实现 biz.MatchQueueClient：取消匹配（未在队回执 canceled=false 不视为错误）。
func (m *MatchQueueGRPC) Cancel(ctx context.Context, playerID string) (bool, error) {
	rep, err := m.cli.CancelMatch(ctx, &matcherv1.CancelMatchRequest{PlayerId: playerID})
	if err != nil {
		return false, err
	}
	return rep.GetCanceled(), nil
}

// Status 实现 biz.MatchQueueClient：查询匹配状态。
func (m *MatchQueueGRPC) Status(ctx context.Context, playerID string) (matcherv1.MatchState, string, string, error) {
	rep, err := m.cli.QueryMatch(ctx, &matcherv1.QueryMatchRequest{PlayerId: playerID})
	if err != nil {
		return matcherv1.MatchState_MATCH_STATE_UNSPECIFIED, "", "", err
	}
	return rep.GetState(), rep.GetTicketId(), rep.GetMatchId(), nil
}

// 静态保证实现接口。
var _ biz.MatchQueueClient = (*MatchQueueGRPC)(nil)
