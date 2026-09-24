// audit_test.go 是管理面审计仓储的 TDD 场景测试：
// 纯函数（重复键识别/错误归一/游标编解码/查询组装）不需要 mongo；
// 接口契约（幂等命中、dry-run 不占键、游标翻页不重不漏）由内存替身与真实 mongo 共用同一份用例。
package repo

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
)

// TestIsDuplicateKey 校验唯一索引冲突识别（幂等命中的判定基础）：
// mongo 写入异常（错误码 11000）直接或包装返回都要命中，其他错误不得误判。
func TestIsDuplicateKey(t *testing.T) {
	dup := mongodriver.WriteException{WriteErrors: []mongodriver.WriteError{{Code: 11000, Message: "E11000 duplicate key"}}}
	if !isDuplicateKey(dup) {
		t.Fatal("11000 写入异常应判定为重复键")
	}
	if !isDuplicateKey(fmt.Errorf("repo: 写入审计记录失败: %w", dup)) {
		t.Fatal("包装后的重复键错误应仍可判定（errors.As 穿透）")
	}
	if isDuplicateKey(errors.New("网络抖动")) {
		t.Fatal("普通错误不得判定为重复键")
	}
	if isDuplicateKey(nil) {
		t.Fatal("nil 不得判定为重复键")
	}
}

// TestInsertErrorMapping 校验插入错误归一：唯一索引冲突 → ErrAuditDuplicateKey
// （调用方据此走幂等命中，而不是返回 500）。
func TestInsertErrorMapping(t *testing.T) {
	err := insertError(mongodriver.WriteException{WriteErrors: []mongodriver.WriteError{{Code: 11000}}})
	if !errors.Is(err, ErrAuditDuplicateKey) {
		t.Fatalf("insertError(重复键) = %v, 期望 ErrAuditDuplicateKey", err)
	}
	other := insertError(errors.New("磁盘故障"))
	if errors.Is(other, ErrAuditDuplicateKey) {
		t.Fatal("其他错误不得归一为 ErrAuditDuplicateKey")
	}
	if other == nil {
		t.Fatal("其他错误必须包装返回，不得吞掉")
	}
}

// TestAuditCursorRoundTrip 校验游标编解码：往返一致、token 不透明、非法 token 报错。
func TestAuditCursorRoundTrip(t *testing.T) {
	c := auditCursor{CreatedAt: 1727000000123, ID: "a-1"}
	token := encodeAuditCursor(c)
	if token == "" || strings.Contains(token, "a-1") || strings.Contains(token, "1727000000123") {
		t.Fatalf("游标 token 必须是不透明编码, 实际 %q", token)
	}
	got, err := decodeAuditCursor(token)
	if err != nil {
		t.Fatalf("decodeAuditCursor: %v", err)
	}
	if got != c {
		t.Fatalf("游标往返 = %+v, 期望 %+v", got, c)
	}
	bad := []string{
		"!!!not-base64",
		base64.RawURLEncoding.EncodeToString([]byte("no-separator")),
		base64.RawURLEncoding.EncodeToString([]byte("abc:a-1")),
		base64.RawURLEncoding.EncodeToString([]byte("123:")),
	}
	for _, token := range bad {
		if _, err := decodeAuditCursor(token); !errors.Is(err, ErrAuditInvalidCursor) {
			t.Fatalf("decodeAuditCursor(%q) = %v, 期望 ErrAuditInvalidCursor", token, err)
		}
	}
}

// TestAuditListQuery 校验列表查询组装：过滤条件、页大小归一与上限、
// 游标过滤必须是「created_at 严格小于」或「同毫秒用 _id 兜底」两分支（不重不漏的键）。
func TestAuditListQuery(t *testing.T) {
	filter, size, err := auditListQuery(AuditListOptions{TargetID: "p-1", Operator: "gm"})
	if err != nil {
		t.Fatalf("auditListQuery: %v", err)
	}
	if size != defaultAuditPageSize {
		t.Fatalf("首页页大小 = %d, 期望默认 %d", size, defaultAuditPageSize)
	}
	if filter["target_id"] != "p-1" || filter["operator"] != "gm" {
		t.Fatalf("过滤条件 = %v, 期望 target_id/operator", filter)
	}
	if _, ok := filter["$or"]; ok {
		t.Fatal("首页（空 token）不得带游标过滤")
	}

	filter, size, err = auditListQuery(AuditListOptions{
		PageSize:  5,
		PageToken: encodeAuditCursor(auditCursor{CreatedAt: 100, ID: "a-9"}),
	})
	if err != nil {
		t.Fatalf("带游标查询: %v", err)
	}
	if size != 5 {
		t.Fatalf("页大小 = %d, 期望 5", size)
	}
	branches, ok := filter["$or"].([]bson.M)
	if !ok || len(branches) != 2 {
		t.Fatalf("游标过滤 = %#v, 期望两个 $or 分支", filter["$or"])
	}
	if got := fmt.Sprint(branches[0]["created_at"]); got != "map[$lt:100]" {
		t.Fatalf("第一分支 = %v, 期望 created_at 严格小于游标时间", got)
	}
	if branches[1]["created_at"] != int64(100) || fmt.Sprint(branches[1]["_id"]) != "map[$lt:a-9]" {
		t.Fatalf("第二分支 = %v, 期望同毫秒用 _id 兜底", branches[1])
	}

	if _, _, err := auditListQuery(AuditListOptions{PageSize: maxAuditPageSize + 1}); !errors.Is(err, ErrAuditPageSize) {
		t.Fatalf("超上限页大小 = %v, 期望 ErrAuditPageSize（不静默截断）", err)
	}
	if _, _, err := auditListQuery(AuditListOptions{PageToken: "!!!"}); !errors.Is(err, ErrAuditInvalidCursor) {
		t.Fatalf("非法游标 = %v, 期望 ErrAuditInvalidCursor", err)
	}
}

// TestAuditRepoContractMemory 用内存替身跑接口契约（真实 mongo 由集成测试跑同一份用例）。
func TestAuditRepoContractMemory(t *testing.T) {
	runAuditRepoContract(t, newMemAuditRepo())
}

// runAuditRepoContract 是 AuditRepo 的接口契约用例（内存替身与 MongoAuditRepo 共用）：
// 同键第二次插入冲突可识别、dry-run 无键不冲突、幂等命中读回首单、游标翻页不重不漏。
func runAuditRepoContract(t *testing.T, r AuditRepo) {
	t.Helper()
	ctx := context.Background()
	contractIdempotency(t, r, ctx)
	contractPagination(t, r, ctx)
	if _, err := r.List(ctx, AuditListOptions{PageToken: "!!!"}); !errors.Is(err, ErrAuditInvalidCursor) {
		t.Fatalf("非法游标 = %v, 期望 ErrAuditInvalidCursor", err)
	}
	if _, err := r.List(ctx, AuditListOptions{PageSize: maxAuditPageSize + 1}); !errors.Is(err, ErrAuditPageSize) {
		t.Fatalf("超上限页大小 = %v, 期望 ErrAuditPageSize", err)
	}
}

// contractIdempotency 覆盖幂等语义：dry-run 不占键、同键第二次插入报冲突、命中读回首单并可收尾。
func contractIdempotency(t *testing.T, r AuditRepo, ctx context.Context) {
	t.Helper()
	// dry-run 记录不写幂等键：多条共存（唯一索引必须稀疏，否则第二条就冲突）。
	for i := 0; i < 2; i++ {
		rec := &models.AuditRecord{
			AuditID: fmt.Sprintf("a-dry-%d", i), Operator: "gm", DryRun: true,
			Result: models.AuditSkippedDryRun, CreatedAt: int64(100 + i),
		}
		if err := r.Insert(ctx, rec); err != nil {
			t.Fatalf("dry-run 记录 %d 插入（无幂等键不得冲突）: %v", i, err)
		}
	}
	// 同键第二次插入 → ErrAuditDuplicateKey（幂等命中判定，不是 500）。
	first := &models.AuditRecord{
		AuditID: "a-1", Operator: "gm", IdempotencyKey: "k-1", ParamsHash: "h-1",
		Result: models.AuditPending, CreatedAt: 300,
	}
	if err := r.Insert(ctx, first); err != nil {
		t.Fatalf("首次带键插入: %v", err)
	}
	dup := &models.AuditRecord{AuditID: "a-2", Operator: "gm", IdempotencyKey: "k-1", Result: models.AuditPending, CreatedAt: 400}
	if err := r.Insert(ctx, dup); !errors.Is(err, ErrAuditDuplicateKey) {
		t.Fatalf("同键第二次插入 = %v, 期望 ErrAuditDuplicateKey", err)
	}
	got, err := r.FindByIdempotencyKey(ctx, "k-1")
	if err != nil {
		t.Fatalf("幂等命中读回: %v", err)
	}
	if got.AuditID != "a-1" || got.ParamsHash != "h-1" {
		t.Fatalf("幂等命中读回 = %+v, 期望首次记录 a-1/h-1", got)
	}
	if err := r.Finalize(ctx, "a-1", AuditFinalize{Result: models.AuditSuccess, FinishedAt: 500}); err != nil {
		t.Fatalf("收尾: %v", err)
	}
	if got, err = r.FindByIdempotencyKey(ctx, "k-1"); err != nil || got.Result != models.AuditSuccess || got.FinishedAt != 500 {
		t.Fatalf("收尾后读回 = %+v(err=%v), 期望 SUCCESS/finished_at=500", got, err)
	}
	if err := r.Finalize(ctx, "a-missing", AuditFinalize{Result: models.AuditFailed, FinishedAt: 1}); !errors.Is(err, ErrAuditNotFound) {
		t.Fatalf("收尾不存在记录 = %v, 期望 ErrAuditNotFound", err)
	}
	if _, err := r.FindByIdempotencyKey(ctx, ""); !errors.Is(err, ErrAuditNotFound) {
		t.Fatalf("空幂等键查询 = %v, 期望 ErrAuditNotFound（dry-run 不占键）", err)
	}
}

// contractPagination 覆盖游标翻页：含同毫秒记录（_id 兜底），页大小 2 逐页取完，
// 断言并集不重不漏、末页 token 为空（按 operator 过滤隔离出本次样本）。
func contractPagination(t *testing.T, r AuditRepo, ctx context.Context) {
	t.Helper()
	const operator = "gm-page"
	created := []int64{1000, 1000, 1000, 2000, 2000, 3000, 3000}
	for i, at := range created {
		rec := &models.AuditRecord{
			AuditID: fmt.Sprintf("p-%d", i), Operator: operator, Result: models.AuditSuccess, CreatedAt: at,
		}
		if err := r.Insert(ctx, rec); err != nil {
			t.Fatalf("插入分页样本 %d: %v", i, err)
		}
	}
	seen := map[string]int{}
	token, pages := "", 0
	for {
		got, err := r.List(ctx, AuditListOptions{Operator: operator, PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("第 %d 页: %v", pages+1, err)
		}
		pages++
		for _, rec := range got.Records {
			seen[rec.AuditID]++
		}
		if got.NextPageToken == "" {
			break
		}
		if pages > len(created) {
			t.Fatal("翻页未在预期页数内结束（游标不前进）")
		}
		token = got.NextPageToken
	}
	if len(seen) != len(created) {
		t.Fatalf("翻页覆盖 %d 条, 期望 %d（有遗漏）: %v", len(seen), len(created), seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("记录 %s 出现 %d 次（有重复）: %v", id, n, seen)
		}
	}
}

// memAuditRepo 是 AuditRepo 的内存替身：语义与 MongoAuditRepo 对齐——
// 幂等键唯一（空键不占位）、列表按 created_at 降序 + _id 兜底、游标复用生产编解码。
type memAuditRepo struct {
	mu   sync.Mutex
	recs map[string]*models.AuditRecord // 审计号 → 记录
	keys map[string]string              // 幂等键 → 审计号
}

// newMemAuditRepo 构造内存审计仓储替身。
func newMemAuditRepo() *memAuditRepo {
	return &memAuditRepo{recs: map[string]*models.AuditRecord{}, keys: map[string]string{}}
}

// Insert 写入记录；幂等键重复返回 ErrAuditDuplicateKey。
func (m *memAuditRepo) Insert(_ context.Context, rec *models.AuditRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rec.IdempotencyKey != "" {
		if _, ok := m.keys[rec.IdempotencyKey]; ok {
			return ErrAuditDuplicateKey
		}
		m.keys[rec.IdempotencyKey] = rec.AuditID
	}
	cp := *rec
	m.recs[rec.AuditID] = &cp
	return nil
}

// Finalize 收尾记录；不存在返回 ErrAuditNotFound。
func (m *memAuditRepo) Finalize(_ context.Context, auditID string, in AuditFinalize) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.recs[auditID]
	if !ok {
		return ErrAuditNotFound
	}
	rec.Result, rec.Reason, rec.Message, rec.FinishedAt = in.Result, in.Reason, in.Message, in.FinishedAt
	return nil
}

// FindByIdempotencyKey 按幂等键读取首次记录；空键或不存在返回 ErrAuditNotFound。
func (m *memAuditRepo) FindByIdempotencyKey(_ context.Context, key string) (*models.AuditRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.keys[key]
	if key == "" || !ok {
		return nil, ErrAuditNotFound
	}
	cp := *m.recs[id]
	return &cp, nil
}

// List 内存分页：复用生产页大小归一与游标编解码，排序/游标语义与 mongo 实现一致。
func (m *memAuditRepo) List(_ context.Context, opts AuditListOptions) (*AuditPage, error) {
	size, err := normalizeAuditPageSize(opts.PageSize)
	if err != nil {
		return nil, err
	}
	var cursor auditCursor
	hasCursor := opts.PageToken != ""
	if hasCursor {
		if cursor, err = decodeAuditCursor(opts.PageToken); err != nil {
			return nil, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	all := make([]*models.AuditRecord, 0, len(m.recs))
	for _, rec := range m.recs {
		if opts.TargetID != "" && rec.TargetID != opts.TargetID {
			continue
		}
		if opts.Operator != "" && rec.Operator != opts.Operator {
			continue
		}
		if hasCursor && !afterAuditCursor(rec, cursor) {
			continue
		}
		all = append(all, rec)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt != all[j].CreatedAt {
			return all[i].CreatedAt > all[j].CreatedAt
		}
		return all[i].AuditID > all[j].AuditID
	})
	page := &AuditPage{Records: all}
	if uint32(len(all)) > size {
		page.Records = all[:size]
		last := page.Records[size-1]
		page.NextPageToken = encodeAuditCursor(auditCursor{CreatedAt: last.CreatedAt, ID: last.AuditID})
	}
	return page, nil
}

// afterAuditCursor 判定记录是否严格位于游标之后（降序排序键的严格后继）。
func afterAuditCursor(rec *models.AuditRecord, c auditCursor) bool {
	if rec.CreatedAt != c.CreatedAt {
		return rec.CreatedAt < c.CreatedAt
	}
	return rec.AuditID < c.ID
}

// EnsureIndexes 内存替身无索引可建（真实实现见 MongoAuditRepo）。
func (m *memAuditRepo) EnsureIndexes(context.Context) error { return nil }
