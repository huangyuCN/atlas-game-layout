package app

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// newMatchQueueClient 装配 matcher gRPC 客户端（服务发现寻址）。
// 连接惰性建立（进程级上下文无 deadline），matcher 未就绪不阻塞 game 启动。
func newMatchQueueClient(ec *clientv3.Client) (*repo.MatchQueueGRPC, error) {
	return repo.NewMatchQueueGRPC(context.Background(), ec)
}

// newPlayerStore 装配组合仓储：redis 快照缓存 + mongo 持久化
// （实现 biz 侧的 repo.PlayerRepo 三级加载语义）。
func newPlayerStore(cache *repo.RedisPlayerCache, persist *repo.MongoPlayerRepo) *repo.PlayerStore {
	return repo.NewPlayerStore(cache, persist)
}
