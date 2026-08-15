package repo

import (
	"context"
	"errors"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
)

// PlayerStore 是玩家仓储的组合实现：
// 读走「redis 快照优先、mongo 兜底」三级缓存链路；
// 写在线的快照（redis）与下线持久化（mongo）分离。
type PlayerStore struct {
	cache   *RedisPlayerCache
	persist *MongoPlayerRepo
}

// NewPlayerStore 构造组合仓储。
func NewPlayerStore(cache *RedisPlayerCache, persist *MongoPlayerRepo) *PlayerStore {
	return &PlayerStore{cache: cache, persist: persist}
}

// LoadPlayer 实现 PlayerRepo：redis 快照优先，未命中回源 mongo。
func (s *PlayerStore) LoadPlayer(ctx context.Context, playerID string) (*models.Player, error) {
	p, err := s.cache.Get(ctx, playerID)
	if err == nil {
		return p, nil
	}
	if !errors.Is(err, ErrPlayerNotFound) {
		return nil, err
	}
	return s.persist.FindByID(ctx, playerID)
}

// FindByAccount 实现 PlayerRepo（注册判重，仅 mongo）。
func (s *PlayerStore) FindByAccount(ctx context.Context, account string) (*models.Player, error) {
	return s.persist.FindByAccount(ctx, account)
}

// CreatePlayer 实现 PlayerRepo（注册建档，mongo 插入）。
func (s *PlayerStore) CreatePlayer(ctx context.Context, p *models.Player) error {
	return s.persist.Create(ctx, p)
}

// SaveSnapshot 实现 PlayerRepo（在线定时 redis 快照）。
func (s *PlayerStore) SaveSnapshot(ctx context.Context, p *models.Player, ttl time.Duration) error {
	return s.cache.Set(ctx, p, ttl)
}

// SavePlayer 实现 PlayerRepo（下线 mongo 落库）。
func (s *PlayerStore) SavePlayer(ctx context.Context, p *models.Player) error {
	return s.persist.Upsert(ctx, p)
}
