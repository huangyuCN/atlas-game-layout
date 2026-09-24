package repo

// 本文件是仓储层的通用查询辅助：把「查单条文档 + 区分不存在与故障」收敛到一处，
// 各仓储只提供集合、过滤条件与各自的哨兵错误（避免逐仓储复制同构的 findOne）。

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
)

// findOneDoc 按过滤条件读取单条文档到 T；不存在返回 notFound，其他错误包装返回。
// what 是错误信息里的对象描述（如「玩家」「审计记录」）。
func findOneDoc[T any](ctx context.Context, coll *mongodriver.Collection, filter bson.M,
	what string, notFound error) (*T, error) {
	var out T
	err := coll.FindOne(ctx, filter).Decode(&out)
	if errors.Is(err, mongodriver.ErrNoDocuments) {
		return nil, notFound
	}
	if err != nil {
		return nil, fmt.Errorf("repo: 查询%s失败: %w", what, err)
	}
	return &out, nil
}
