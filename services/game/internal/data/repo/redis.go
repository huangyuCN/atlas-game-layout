package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	goredis "github.com/redis/go-redis/v9"
)

// 缓存键约定。
const (
	// playerCachePrefix 是玩家快照缓存键前缀：atlas:player:<playerID>。
	playerCachePrefix = "atlas:player:"
)

// RedisPlayerCache 是玩家聚合根的 redis 快照缓存。
type RedisPlayerCache struct {
	cli *pkredis.Client
}

// NewRedisPlayerCache 构造玩家快照缓存。
func NewRedisPlayerCache(cli *pkredis.Client) *RedisPlayerCache {
	return &RedisPlayerCache{cli: cli}
}

// Get 读取玩家快照；不存在返回 ErrPlayerNotFound。
func (c *RedisPlayerCache) Get(ctx context.Context, playerID string) (*models.Player, error) {
	b, err := c.cli.Raw().Get(ctx, playerCachePrefix+playerID).Bytes()
	if err == goredis.Nil {
		return nil, ErrPlayerNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repo: 读取玩家缓存失败: %w", err)
	}
	var p models.Player
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("repo: 玩家缓存解码失败: %w", err)
	}
	return &p, nil
}

// Set 写玩家快照（定时刷盘，TTL 过期自动清理）。
func (c *RedisPlayerCache) Set(ctx context.Context, p *models.Player, ttl time.Duration) error {
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("repo: 玩家缓存编码失败: %w", err)
	}
	return c.cli.Raw().Set(ctx, playerCachePrefix+p.PlayerID, string(b), ttl).Err()
}
