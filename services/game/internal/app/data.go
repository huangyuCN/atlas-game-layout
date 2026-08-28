package app

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
)

// newMongoPlayerRepo 装配 mongo 玩家持久化仓储（启动期建 account 唯一索引）。
// repo.NewMongoPlayerRepo 需要外部 ctx，这里以进程级上下文补齐（fx 无法注入裸 ctx）。
func newMongoPlayerRepo(cli *mongo.Client) (*repo.MongoPlayerRepo, error) {
	persist, err := repo.NewMongoPlayerRepo(context.Background(), cli)
	if err != nil {
		return nil, fmt.Errorf("app: 装配玩家仓储失败: %w", err)
	}
	return persist, nil
}

// newPlayerStore 装配组合仓储：redis 快照缓存 + mongo 持久化
// （实现 biz 侧的 repo.PlayerRepo 三级加载语义）。
func newPlayerStore(cache *repo.RedisPlayerCache, persist *repo.MongoPlayerRepo) *repo.PlayerStore {
	return repo.NewPlayerStore(cache, persist)
}
