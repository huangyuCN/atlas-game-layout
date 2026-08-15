// Package repo 提供战斗结算结果的持久化操作（mongo）。
package repo

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/models"
	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// battleCollection 是战斗结果集合名。
const battleCollection = "battle_results"

// ResultRepo 是战斗结算结果的仓储接口。
type ResultRepo interface {
	// Save 落库结算结果（覆盖写，幂等）。
	Save(ctx context.Context, r *models.BattleResult) error
}

// MongoResultRepo 是结算结果的 mongo 实现。
type MongoResultRepo struct {
	coll *mongodriver.Collection
}

// NewMongoResultRepo 构造 mongo 结算仓储（_id 即 battleID，天然唯一索引，无需建索引）。
func NewMongoResultRepo(cli *mongo.Client) *MongoResultRepo {
	return &MongoResultRepo{coll: cli.Collection(battleCollection)}
}

// Save 实现 ResultRepo（按 battle_id 覆盖，幂等）。
func (r *MongoResultRepo) Save(ctx context.Context, res *models.BattleResult) error {
	_, err := r.coll.ReplaceOne(ctx, bson.M{"_id": res.BattleID}, res, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("repo: 保存结算结果失败: %w", err)
	}
	return nil
}
