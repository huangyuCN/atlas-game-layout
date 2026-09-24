package mongo

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
)

// itMongoURI 返回集成测试连接串：默认走集成服务器约定的 mongo 端口 27018
// （与 test/e2e 的 itMongoURI 一致），可用 ATLAS_IT_MONGO_URI 覆盖。
func itMongoURI() string {
	if uri := os.Getenv("ATLAS_IT_MONGO_URI"); uri != "" {
		return uri
	}
	return "mongodb://127.0.0.1:27018"
}

// TestBuildIndexModels 校验 Index → mongo.IndexModel 的选项映射（不依赖真实 mongo）：
// Sparse 必须透传到 SetSparse（幂等键唯一索引若非稀疏，dry-run 记录会在第二次插入时被误判为冲突）；
// 复合索引键必须是有序文档（多键 map 会被 mongo 拒绝：multi-key map passed in for ordered parameter keys）。
func TestBuildIndexModels(t *testing.T) {
	compound := bson.D{{Key: "target_type", Value: 1}, {Key: "target_id", Value: 1}, {Key: "created_at", Value: -1}}
	models := buildIndexModels([]Index{
		{Keys: bson.D{{Key: "idempotency_key", Value: 1}}, Unique: true, Sparse: true, Name: "uniq_idempotency_key"},
		{Keys: compound, Name: "idx_target_created"},
	})
	if len(models) != 2 {
		t.Fatalf("buildIndexModels 返回 %d 个模型, 期望 2", len(models))
	}
	uniq := models[0].Options
	if uniq.Unique == nil || !*uniq.Unique {
		t.Fatal("唯一索引的 Unique 未透传")
	}
	if uniq.Sparse == nil || !*uniq.Sparse {
		t.Fatal("稀疏索引的 Sparse 未透传（dry-run 行会与唯一索引冲突）")
	}
	if uniq.Name == nil || *uniq.Name != "uniq_idempotency_key" {
		t.Fatalf("索引名 = %v, 期望 uniq_idempotency_key", uniq.Name)
	}
	keys, ok := models[1].Keys.(bson.D)
	if !ok {
		t.Fatalf("复合索引键 = %T, 期望 bson.D（多键 map 会被 mongo 拒绝）", models[1].Keys)
	}
	if len(keys) != 3 || keys[0].Key != "target_type" || keys[1].Key != "target_id" ||
		keys[2].Key != "created_at" || keys[2].Value != -1 {
		t.Fatalf("复合索引键 = %v, 期望按声明顺序保留三键", keys)
	}
	plain := models[1].Options
	if plain.Unique != nil && *plain.Unique {
		t.Fatal("普通索引不得被设为唯一")
	}
	if plain.Sparse != nil && *plain.Sparse {
		t.Fatal("普通索引不得被设为稀疏")
	}
	if plain.Name == nil || *plain.Name != "idx_target_created" {
		t.Fatalf("索引名 = %v, 期望 idx_target_created", plain.Name)
	}
}

// TestEnsureIndexesSparseIntegration 在真实 mongo 上校验稀疏唯一索引语义：
// 索引属性 unique+sparse 落地；无键（dry-run）文档可多条共存；同键第二次插入被拒。
func TestEnsureIndexesSparseIntegration(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(ctx, Options{URI: itMongoURI(), Database: "atlas_mongo_it", ConnectTimeout: 2 * time.Second})
	if err != nil {
		t.Skipf("mongo 不可用: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	const coll = "sparse_idx_it"
	if err := c.Collection(coll).Drop(ctx); err != nil {
		t.Fatalf("清理集合: %v", err)
	}
	t.Cleanup(func() { _ = c.Collection(coll).Drop(context.Background()) })
	if err := c.EnsureIndexes(ctx, coll, []Index{
		{Keys: bson.D{{Key: "idempotency_key", Value: 1}}, Unique: true, Sparse: true, Name: "uniq_idempotency_key"},
		// 复合索引一并建：多键 map 会被 mongo 拒绝，这里守护有序键的透传。
		{Keys: bson.D{{Key: "target_type", Value: 1}, {Key: "target_id", Value: 1}, {Key: "created_at", Value: -1}}, Name: "idx_target_created"},
	}); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	assertSparseUniqueIndex(t, ctx, c, coll, "uniq_idempotency_key")

	// dry-run 记录不写幂等键：两条共存（稀疏索引不索引缺失字段）。
	if _, err := c.Collection(coll).InsertMany(ctx, []any{
		map[string]any{"_id": "a-1", "result": "SKIPPED_DRY_RUN"},
		map[string]any{"_id": "a-2", "result": "SKIPPED_DRY_RUN"},
	}); err != nil {
		t.Fatalf("两条无幂等键记录应共存（稀疏索引）: %v", err)
	}
	// 同键第二次插入必须被唯一索引拒绝（仓储按此错误判定幂等命中）。
	if _, err := c.Collection(coll).InsertOne(ctx, map[string]any{"_id": "a-3", "idempotency_key": "k-1"}); err != nil {
		t.Fatalf("首次插入带键记录: %v", err)
	}
	if _, err := c.Collection(coll).InsertOne(ctx, map[string]any{"_id": "a-4", "idempotency_key": "k-1"}); err == nil {
		t.Fatal("同键第二次插入应被唯一索引拒绝")
	} else if !mongodriver.IsDuplicateKeyError(err) {
		t.Fatalf("错误应可被识别为重复键: %v", err)
	}
}

// assertSparseUniqueIndex 断言指定索引在真实集合上确实为 unique + sparse。
func assertSparseUniqueIndex(t *testing.T, ctx context.Context, c *Client, coll, name string) {
	t.Helper()
	cur, err := c.Collection(coll).Indexes().List(ctx)
	if err != nil {
		t.Fatalf("列出索引: %v", err)
	}
	var specs []struct {
		Name   string `bson:"name"`
		Unique bool   `bson:"unique"`
		Sparse bool   `bson:"sparse"`
	}
	if err := cur.All(ctx, &specs); err != nil {
		t.Fatalf("解码索引清单: %v", err)
	}
	for _, s := range specs {
		if s.Name != name {
			continue
		}
		if !s.Unique || !s.Sparse {
			t.Fatalf("索引 %s 属性 = unique:%v sparse:%v, 期望都为 true", name, s.Unique, s.Sparse)
		}
		return
	}
	t.Fatalf("索引 %s 不存在（现有: %+v）", name, specs)
}

// TestNewClientEmptyOpts 验证空配置报错。
func TestNewClientEmptyOpts(t *testing.T) {
	ctx := context.Background()
	if _, err := NewClient(ctx, Options{}); err == nil {
		t.Fatal("NewClient() 期望空 uri 错误")
	}
	if _, err := NewClient(ctx, Options{URI: "mongodb://127.0.0.1:27017"}); err == nil {
		t.Fatal("NewClient() 期望空 database 错误")
	}
}

// TestNewClientUnavailable 验证无 mongo 时连接报错（本地无服务环境）。
func TestNewClientUnavailable(t *testing.T) {
	ctx := context.Background()
	_, err := NewClient(ctx, Options{
		URI:            "mongodb://127.0.0.1:27017",
		Database:       "test",
		ConnectTimeout: 300 * time.Millisecond,
	})
	if err == nil {
		t.Skip("本机存在 mongo，跳过不可用用例")
	}
	// 报错即符合预期（无 mongo 环境）。
}

// TestEnsureIndexesEmpty 验证空索引列表为空操作。
func TestEnsureIndexesEmpty(t *testing.T) {
	c := &Client{database: "db"}
	if err := c.EnsureIndexes(context.Background(), "coll", nil); err != nil {
		t.Fatalf("EnsureIndexes(nil) 错误 = %v", err)
	}
}

// TestMongoIntegration 真实 mongo 集成测试（默认 127.0.0.1:27018，无服务时跳过）。
func TestMongoIntegration(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(ctx, Options{
		URI:            itMongoURI(),
		Database:       "atlas_m2_test",
		ConnectTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("mongo 不可用: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	// 索引初始化。
	if err := c.EnsureIndexes(ctx, "players", []Index{
		{Keys: bson.D{{Key: "player_id", Value: 1}}, Unique: true, Name: "uniq_player"},
	}); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	// 写入与读取。
	col := c.Collection("players")
	if _, err := col.InsertOne(ctx, map[string]any{"player_id": "p-1", "lv": 10}); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}
	var got struct {
		PlayerID string `bson:"player_id"`
		Lv       int    `bson:"lv"`
	}
	if err := col.FindOne(ctx, map[string]any{"player_id": "p-1"}).Decode(&got); err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if got.PlayerID != "p-1" || got.Lv != 10 {
		t.Fatalf("读取结果不符合预期: %+v", got)
	}
	// 清理测试库。
	_ = c.Database().Drop(ctx)
}
