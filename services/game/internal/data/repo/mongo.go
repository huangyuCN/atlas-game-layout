package repo

import (
	"context"
	"errors"
	"fmt"

	atlaslog "github.com/huangyuCN/atlas/log"

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

// 存档大小预警阈值：mongo 单文档上限 16MiB，接近上限（15MiB）时告警
// （背包无限膨胀等业务错误应在预警阶段暴露，而非写入失败后才被知道）。
const (
	maxDocBytes  = 16 << 20
	warnDocBytes = 15 << 20
)

// excludedUpsertKeys 是聚合根→mongo 更新写（Upsert）剔除的字段：
//   - "_id" 不允许出现在 $set 里（mongo 禁止修改 _id，靠过滤条件携带）；
//   - 认证字段只在注册建档（Create）时写入；此后内存聚合根可能来自 redis
//     快照（无认证字段），整文档替换会把 mongo 凭据覆盖为空——更新写一律
//     不携带，凭据变更走显式专用路径。
//
// 剔除基于 bson 文档键（schema 无关）：Player 新增字段自动进 $set，无需维护转换对。
var excludedUpsertKeys = map[string]bool{"_id": true, "salt": true, "password": true}

// upsertSetDoc 把聚合根编码为 $set 更新文档：整文档 marshal → 剔除
// excludedUpsertKeys。full 为全量 BSON（含认证字段，供文档大小预警复用，
// 免二次 marshal）。
func upsertSetDoc(p *models.Player) (set bson.M, full []byte, err error) {
	full, err = bson.Marshal(p)
	if err != nil {
		return nil, nil, fmt.Errorf("repo: 玩家 BSON 编码失败: %w", err)
	}
	if err := bson.Unmarshal(full, &set); err != nil {
		return nil, nil, fmt.Errorf("repo: 玩家文档解码失败: %w", err)
	}
	for k := range excludedUpsertKeys {
		delete(set, k)
	}
	return set, full, nil
}

// warnDocSize 存档大小预警：超过预警阈值打 Error（可告警），不阻断写入
// （>16MiB 时 mongo 驱动会自行返回写入错误）。
func warnDocSize(playerID string, full []byte) {
	if len(full) >= warnDocBytes {
		atlaslog.Errorf("repo: 玩家存档过大: player=%s size=%d（预警阈值 %d / mongo 上限 %d）",
			playerID, len(full), warnDocBytes, maxDocBytes)
	}
}

// Upsert 按玩家 ID 更新持久化（下线落库 / 降级直写 / 登录补写共用）。
// $set 更新（剔除认证字段）：不存在则插入（upsert），已存在则仅覆盖数据
// 字段——mongo 里的凭据永远只由 Create 写入。
func (r *MongoPlayerRepo) Upsert(ctx context.Context, p *models.Player) error {
	set, full, err := upsertSetDoc(p)
	if err != nil {
		return err
	}
	warnDocSize(p.PlayerID, full)
	if _, err := r.coll.UpdateOne(ctx, bson.M{"_id": p.PlayerID}, bson.M{"$set": set}, optionsUpsert()); err != nil {
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

// optionsUpsert 构造 $set 更新的 upsert 选项。
func optionsUpsert() *options.UpdateOptions {
	return options.Update().SetUpsert(true)
}
