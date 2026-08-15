// Package repo 提供玩家数据的缓存与持久化操作：
// 在线内存聚合根（PlayerActor 持有）→ 定时 redis 快照 → 下线 mongo 持久化；
// 上线加载走「redis 优先、mongo 兜底」三级链路。
package repo

import (
	"context"
	"errors"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
)

// ErrPlayerNotFound 表示玩家数据不存在。
var ErrPlayerNotFound = errors.New("repo: 玩家不存在")

// PlayerRepo 是玩家数据仓储接口（biz 层依赖；实现见 PlayerStore）。
type PlayerRepo interface {
	// LoadPlayer 加载玩家：redis 快照优先，未命中回源 mongo。
	LoadPlayer(ctx context.Context, playerID string) (*models.Player, error)
	// FindByAccount 按账号查询（注册判重用，仅 mongo）。
	FindByAccount(ctx context.Context, account string) (*models.Player, error)
	// CreatePlayer 注册建档（mongo 插入）。
	CreatePlayer(ctx context.Context, player *models.Player) error
	// SaveSnapshot 写 redis 快照（在线定时刷盘）。
	SaveSnapshot(ctx context.Context, player *models.Player, ttl time.Duration) error
	// SavePlayer 持久化到 mongo（下线落库）。
	SavePlayer(ctx context.Context, player *models.Player) error
}

// SessionStore 是玩家在线会话令牌存储抽象。
type SessionStore interface {
	// Get 读取玩家当前会话令牌（不存在返回空串）。
	Get(ctx context.Context, playerID string) (string, error)
	// Set 写入玩家会话令牌（TTL 过期自动清理）。
	Set(ctx context.Context, playerID, token string, ttl time.Duration) error
	// Del 删除玩家会话令牌（登出/挤下线清理）。
	Del(ctx context.Context, playerID string) error
}
