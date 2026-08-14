package mongo

import (
	"context"
	"testing"
	"time"
)

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

// TestMongoIntegration 真实 mongo 集成测试（127.0.0.1:27017，无服务时跳过）。
func TestMongoIntegration(t *testing.T) {
	ctx := context.Background()
	c, err := NewClient(ctx, Options{
		URI:            "mongodb://127.0.0.1:27017",
		Database:       "atlas_m2_test",
		ConnectTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("mongo 不可用: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	// 索引初始化。
	if err := c.EnsureIndexes(ctx, "players", []Index{
		{Keys: map[string]int{"player_id": 1}, Unique: true, Name: "uniq_player"},
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
