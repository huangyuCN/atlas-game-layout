// Package usecase 提供跨接口共享的业务小逻辑（多个 handler 可复用）。
package usecase

import (
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
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
