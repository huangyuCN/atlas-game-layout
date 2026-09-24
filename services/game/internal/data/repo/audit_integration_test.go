// audit_integration_test.go 是审计仓储的 mongo 集成测试：
// 默认连 127.0.0.1:27018（集成服务器约定，与 test/e2e 一致），不可达即跳过；
// 在 10.10.9.36 上执行可覆盖真实稀疏唯一索引与游标分页。
// 库名沿用 e2e 约定 game_it；集合 admin_audits 为管理面专用，不与业务集合互踩。
package repo

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
)

// auditITMongoURI 返回集成测试连接串：可用 ATLAS_IT_MONGO_URI 覆盖。
func auditITMongoURI() string {
	if uri := os.Getenv("ATLAS_IT_MONGO_URI"); uri != "" {
		return uri
	}
	return "mongodb://127.0.0.1:27018"
}

// TestMongoAuditRepoIntegration 在真实 mongo 上跑审计仓储接口契约（与内存替身同一份用例）：
// 同键第二次插入被唯一索引拒绝并归一为 ErrAuditDuplicateKey、dry-run 无键不冲突、
// 游标翻页不重不漏；并断言幂等键索引确实是 unique + sparse。
func TestMongoAuditRepoIntegration(t *testing.T) {
	ctx := context.Background()
	cli, err := mongo.NewClient(ctx, mongo.Options{
		URI:            auditITMongoURI(),
		Database:       "game_it",
		ConnectTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("mongo 不可用: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close(context.Background()) })
	if err := cli.Collection(auditCollection).Drop(ctx); err != nil {
		t.Fatalf("清理审计集合: %v", err)
	}
	t.Cleanup(func() { _ = cli.Collection(auditCollection).Drop(context.Background()) })

	r, err := NewMongoAuditRepo(ctx, cli)
	if err != nil {
		t.Fatalf("NewMongoAuditRepo: %v", err)
	}
	runAuditRepoContract(t, r)
	assertAuditIndexSparse(t, ctx, cli)
}

// assertAuditIndexSparse 断言幂等键索引在真实集合上为 unique + sparse
// （dry-run 不写键，非稀疏则多条无键记录会互相冲突）。
func assertAuditIndexSparse(t *testing.T, ctx context.Context, cli *mongo.Client) {
	t.Helper()
	cur, err := cli.Collection(auditCollection).Indexes().List(ctx)
	if err != nil {
		t.Fatalf("列出审计索引: %v", err)
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
		if s.Name != "uniq_idempotency_key" {
			continue
		}
		if !s.Unique || !s.Sparse {
			t.Fatalf("索引 uniq_idempotency_key 属性 = unique:%v sparse:%v, 期望都为 true", s.Unique, s.Sparse)
		}
		return
	}
	t.Fatalf("索引 uniq_idempotency_key 不存在（现有: %+v）", specs)
}
