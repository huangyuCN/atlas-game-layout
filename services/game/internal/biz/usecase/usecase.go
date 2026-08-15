// Package usecase 提供跨接口共享的业务小逻辑（多个 handler 可复用）。
package usecase

import (
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// PlayerSummary 组装玩家公开摘要（注册/登录/查询共用）。
func PlayerSummary(p *models.Player) *commonv1.PlayerSummary {
	return &commonv1.PlayerSummary{
		PlayerId: p.PlayerID,
		Nickname: p.Nickname,
		Level:    p.Level,
	}
}

// BackpackItems 把聚合根背包转换为协议条目（查询共用）。
func BackpackItems(p *models.Player) []*gamev1.BackpackItem {
	items := make([]*gamev1.BackpackItem, 0, len(p.Items))
	for _, it := range p.Items {
		items = append(items, &gamev1.BackpackItem{ItemId: it.ItemID, Count: it.Count})
	}
	return items
}

// ActorReplyError 把 PlayerActor 回执的 error_reason 映射为结构化错误
// （grpc 转发与 gateway 共用同一映射约定）。
func ActorReplyError(reason string) error {
	switch reason {
	case errorv1.ReasonPlayerNotFound():
		return errorv1.ErrPlayerNotFound("玩家不存在")
	case errorv1.ReasonInvalidParams():
		return errorv1.ErrInvalidParams("参数非法")
	case errorv1.ReasonPlayerNotOnline():
		return errorv1.ErrPlayerNotOnline("玩家不在线")
	default:
		return errorv1.ErrInternal("业务处理失败: %s", reason)
	}
}

// ReasonOf 提取错误的 reason（回执 error_reason 字段，失败回执共用）。
func ReasonOf(err error) string {
	if se := atlaserrors.FromError(err); se != nil {
		return se.Reason
	}
	return "UNKNOWN"
}
