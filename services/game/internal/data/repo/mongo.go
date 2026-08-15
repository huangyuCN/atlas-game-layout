package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// playerCollection 是玩家集合名。
const playerCollection = "players"

// MongoPlayerRepo 是玩家持久化的 mongo 实现。
type MongoPlayerRepo struct {
	coll *mongodriver.Collection
}

// NewMongoPlayerRepo 构造 mongo 持久化（含索引初始化）。
func NewMongoPlayerRepo(ctx context.Context, cli *mongo.Client) (*MongoPlayerRepo, error) {
	if err := cli.EnsureIndexes(ctx, playerCollection, []mongo.Index{
		{Keys: map[string]int{"account": 1}, Unique: true, Name: "uniq_account"},
	}); err != nil {
		return nil, err
	}
	return &MongoPlayerRepo{coll: cli.Collection(playerCollection)}, nil
}

// FindByID 按玩家 ID 查询。
func (r *MongoPlayerRepo) FindByID(ctx context.Context, playerID string) (*models.Player, error) {
	return r.findOne(ctx, bson.M{"_id": playerID})
}

// FindByAccount 按账号查询。
func (r *MongoPlayerRepo) FindByAccount(ctx context.Context, account string) (*models.Player, error) {
	return r.findOne(ctx, bson.M{"account": account})
}

// Create 注册建档（重复插入由账号唯一索引兜底）。
func (r *MongoPlayerRepo) Create(ctx context.Context, p *models.Player) error {
	if _, err := r.coll.InsertOne(ctx, p); err != nil {
		return fmt.Errorf("repo: 创建玩家失败: %w", err)
	}
	return nil
}

// Upsert 按玩家 ID 覆盖持久化（下线落库；不存在则插入）。
func (r *MongoPlayerRepo) Upsert(ctx context.Context, p *models.Player) error {
	if _, err := r.coll.ReplaceOne(ctx, bson.M{"_id": p.PlayerID}, p, optionsReplace()); err != nil {
		return fmt.Errorf("repo: 保存玩家失败: %w", err)
	}
	return nil
}

// findOne 按过滤条件读取单个玩家文档；不存在返回 ErrPlayerNotFound。
func (r *MongoPlayerRepo) findOne(ctx context.Context, filter bson.M) (*models.Player, error) {
	var p models.Player
	err := r.coll.FindOne(ctx, filter).Decode(&p)
	if errors.Is(err, mongodriver.ErrNoDocuments) {
		return nil, ErrPlayerNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repo: 查询玩家失败: %w", err)
	}
	return &p, nil
}

// optionsReplace 构造 upsert 替换选项。
func optionsReplace() *options.ReplaceOptions {
	return options.Replace().SetUpsert(true)
}
