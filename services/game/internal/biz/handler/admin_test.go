package handler

// admin_test.go 是管理面 handler 的 TDD 场景测试：用内存审计仓储替身（模拟唯一稀疏索引）
// 与计数假状态访问，钉死「校验 → 审计占位 → 幂等判定 → 执行 → 收尾」的裁决顺序与零副作用边界。

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas/metrics"
)

// fakeAuditRepo 是 repo.AuditRepo 的内存替身：模拟唯一**稀疏**索引（同键第二次插入冲突、
// 空键不占键）、收尾写终态、按幂等键读首次记录，并记录调用次数供「零副作用/零审计」断言。
type fakeAuditRepo struct {
	mu        sync.Mutex
	byID      map[string]*models.AuditRecord
	byKey     map[string]string // 幂等键 → 审计号（唯一索引的替身）
	insertErr error             // 注入的占位失败
	finalErr  error             // 注入的收尾失败
	inserts   int
	finalizes int
	lastList  repo.AuditListOptions
}

// newFakeAuditRepo 构造空的内存审计仓储。
func newFakeAuditRepo() *fakeAuditRepo {
	return &fakeAuditRepo{byID: map[string]*models.AuditRecord{}, byKey: map[string]string{}}
}

// Insert 写入审计占位；幂等键重复返回 repo.ErrAuditDuplicateKey（幂等命中，不是 500）。
func (r *fakeAuditRepo) Insert(_ context.Context, rec *models.AuditRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inserts++
	if r.insertErr != nil {
		return r.insertErr
	}
	if rec.IdempotencyKey != "" {
		if _, ok := r.byKey[rec.IdempotencyKey]; ok {
			return repo.ErrAuditDuplicateKey
		}
		r.byKey[rec.IdempotencyKey] = rec.AuditID
	}
	r.byID[rec.AuditID] = cloneAudit(rec)
	return nil
}

// Finalize 收尾占位记录（写终态 + reason/message + finished_at）。
func (r *fakeAuditRepo) Finalize(_ context.Context, auditID string, in repo.AuditFinalize) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finalizes++
	if r.finalErr != nil {
		return r.finalErr
	}
	rec, ok := r.byID[auditID]
	if !ok {
		return repo.ErrAuditNotFound
	}
	rec.Result, rec.Reason, rec.Message, rec.FinishedAt = in.Result, in.Reason, in.Message, in.FinishedAt
	return nil
}

// FindByIdempotencyKey 按幂等键读首次记录；不存在返回 repo.ErrAuditNotFound。
func (r *fakeAuditRepo) FindByIdempotencyKey(_ context.Context, key string) (*models.AuditRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, ok := r.byKey[key]
	if !ok {
		return nil, repo.ErrAuditNotFound
	}
	return cloneAudit(r.byID[id]), nil
}

// List 按 target/operator 过滤返回记录（分页语义由仓储单测覆盖，此处只记录过滤条件）。
func (r *fakeAuditRepo) List(_ context.Context, opts repo.AuditListOptions) (*repo.AuditPage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastList = opts
	page := &repo.AuditPage{}
	for _, rec := range r.byID {
		if opts.TargetID != "" && rec.TargetID != opts.TargetID {
			continue
		}
		if opts.Operator != "" && rec.Operator != opts.Operator {
			continue
		}
		page.Records = append(page.Records, cloneAudit(rec))
	}
	return page, nil
}

// EnsureIndexes 是空实现（内存替身没有索引）。
func (r *fakeAuditRepo) EnsureIndexes(context.Context) error { return nil }

// record 取指定审计号的落库记录（不存在即失败）。
func (r *fakeAuditRepo) record(t *testing.T, auditID string) *models.AuditRecord {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.byID[auditID]
	if !ok {
		t.Fatalf("审计记录 %s 不存在", auditID)
	}
	return rec
}

// onlyRecord 取唯一一条落库记录（不唯一即失败）。
func (r *fakeAuditRepo) onlyRecord(t *testing.T) *models.AuditRecord {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.byID) != 1 {
		t.Fatalf("审计记录数 = %d，期望 1", len(r.byID))
	}
	for _, rec := range r.byID {
		return rec
	}
	return nil
}

// cloneAudit 复制审计记录（替身内部状态与调用方视图隔离）。
func cloneAudit(rec *models.AuditRecord) *models.AuditRecord {
	out := *rec
	return &out
}

// fakeStateAccess 是 biz.PlayerStateAccess 的假实现：记录调用与参数、回放注入错误，
// 供管理面断言「零调用 / 只执行一次 / 转发参数原样」。
type fakeStateAccess struct {
	err            error
	missingPlayer  bool // true = 玩家不存在（GetPlayer 返回 nil, nil）
	grantCalls     int
	getPlayerCalls int
	grantPlayer    string
	grantItem      uint32
	grantCount     uint32
	grantReason    string
	backpack       []*gamev1.BackpackItem
	summary        *commonv1.PlayerSummary
}

// GrantItem 记录发放调用并回放注入错误。
func (f *fakeStateAccess) GrantItem(_ context.Context, playerID string, itemID, count uint32, reason string) error {
	f.grantCalls++
	f.grantPlayer, f.grantItem, f.grantCount, f.grantReason = playerID, itemID, count, reason
	return f.err
}

// GetBackpack 回放固定背包。
func (f *fakeStateAccess) GetBackpack(context.Context, string) ([]*gamev1.BackpackItem, error) {
	return f.backpack, f.err
}

// GetPlayer 回放固定玩家摘要（未设置时按 playerID 合成）；missingPlayer=true 时按不存在回放。
func (f *fakeStateAccess) GetPlayer(_ context.Context, playerID string) (*commonv1.PlayerSummary, error) {
	f.getPlayerCalls++
	if f.err != nil {
		return nil, f.err
	}
	if f.missingPlayer {
		return nil, nil
	}
	if f.summary != nil {
		return f.summary, nil
	}
	return &commonv1.PlayerSummary{PlayerId: playerID}, nil
}

// fakeMeter 是 metrics.Collector 的计数替身：只累计 counter，其余能力返回空实现，
// 供断言「审计收尾失败打点一次」。
type fakeMeter struct {
	mu       sync.Mutex
	counters map[string]float64
}

// newFakeMeter 构造空的计数替身。
func newFakeMeter() *fakeMeter { return &fakeMeter{counters: map[string]float64{}} }

// Counter 返回按名累加到替身的计数句柄。
func (m *fakeMeter) Counter(name string, _ ...string) metrics.Counter {
	return meterCounter{meter: m, name: name}
}

// Histogram 返回空实现（管理面只用 counter）。
func (m *fakeMeter) Histogram(string, ...string) metrics.Histogram {
	return metrics.Noop().Histogram("")
}

// Gauge 返回空实现（管理面只用 counter）。
func (m *fakeMeter) Gauge(string, ...string) metrics.Gauge { return metrics.Noop().Gauge("") }

// total 取指定 counter 的累计值（未打过点即 0）。
func (m *fakeMeter) total(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

// meterCounter 是按名累加到 fakeMeter 的最小 counter 实现。
type meterCounter struct {
	meter *fakeMeter
	name  string
}

// Add 累加增量。
func (c meterCounter) Add(delta float64) {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	c.meter.counters[c.name] += delta
}

// adminFixture 是管理面单测夹具：内存审计仓储 + 计数假状态访问 + 计数指标 + 确定性审计号/时钟。
type adminFixture struct {
	h     *AdminHandler
	audit *fakeAuditRepo
	state *fakeStateAccess
	meter *fakeMeter
}

// newAdminFixture 构造夹具；maxGrant = 0 时由 handler 回退默认上限。
func newAdminFixture(maxGrant uint32) *adminFixture {
	f := &adminFixture{audit: newFakeAuditRepo(), state: &fakeStateAccess{}, meter: newFakeMeter()}
	seq := 0
	f.h = NewAdminHandler(f.audit, f.state, biz.AdminOptions{
		MaxGrantCount: maxGrant,
		Meter:         f.meter,
		NewAuditID:    func() string { seq++; return fmt.Sprintf("aud-%d", seq) },
		Now:           func() time.Time { return time.UnixMilli(1727000000000 + int64(seq)) },
	})
	return f
}

// grantReq 构造发道具请求（item 固定 1001，用例按需覆写上下文与数量）。
func grantReq(ac *admingamev1.AdminContext, playerID string, count uint32) *admingamev1.GrantItemRequest {
	return &admingamev1.GrantItemRequest{Context: ac, PlayerId: playerID, ItemId: 1001, Count: count}
}

// writeCtx 构造管理面写操作信封。
func writeCtx(operator, key string) *admingamev1.AdminContext {
	return &admingamev1.AdminContext{Operator: operator, IdempotencyKey: key, Reason: "工单-1"}
}

// assertNoSideEffect 断言既未调用底层也未写审计（拒单路径的硬要求）。
func assertNoSideEffect(t *testing.T, f *adminFixture) {
	t.Helper()
	if f.state.grantCalls != 0 {
		t.Fatalf("拒单路径不得调用底层发放，实际 %d 次", f.state.grantCalls)
	}
	if f.audit.inserts != 0 {
		t.Fatalf("拒单路径不得写审计，实际 %d 次", f.audit.inserts)
	}
}

// TestAdminGrantItemRejectsMissingOperator 缺 operator → AdminOperatorMissing（先于幂等键校验）。
func TestAdminGrantItemRejectsMissingOperator(t *testing.T) {
	f := newAdminFixture(100)
	_, err := f.h.GrantItem(context.Background(),
		grantReq(&admingamev1.AdminContext{IdempotencyKey: "k-1"}, "p-1", 1))
	if !errorv1.IsAdminOperatorMissing(err) {
		t.Fatalf("缺 operator 错误 = %v，期望 AdminOperatorMissing", err)
	}
	assertNoSideEffect(t, f)
}

// TestAdminGrantItemRejectsMissingIdempotencyKey 缺幂等键 → AdminIdempotencyKeyMissing（写操作必填）。
func TestAdminGrantItemRejectsMissingIdempotencyKey(t *testing.T) {
	f := newAdminFixture(100)
	_, err := f.h.GrantItem(context.Background(),
		grantReq(&admingamev1.AdminContext{Operator: "gm-1"}, "p-1", 1))
	if !errorv1.IsAdminIdempotencyKeyMissing(err) {
		t.Fatalf("缺幂等键错误 = %v，期望 AdminIdempotencyKeyMissing", err)
	}
	assertNoSideEffect(t, f)
}

// TestAdminGrantItemDryRunSkipsSideEffect dry-run：做存在性检查（只读）但不调用底层发放、不改档，
// 审计收尾 SKIPPED_DRY_RUN 且**不占用幂等键**（稀疏唯一索引，随后真发放可用同键）；
// 玩家存在时回执 violations 为空。
func TestAdminGrantItemDryRunSkipsSideEffect(t *testing.T) {
	f := newAdminFixture(100)
	ac := writeCtx("gm-1", "k-dry")
	ac.DryRun = true
	rep, err := f.h.GrantItem(context.Background(), grantReq(ac, "p-1", 5))
	if err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if rep.GetApplied() || rep.GetReplayed() || rep.GetAuditId() == "" {
		t.Fatalf("dry-run 回执不符（applied=false/replayed=false/audit_id 非空）: %+v", rep)
	}
	if len(rep.GetViolations()) != 0 {
		t.Fatalf("玩家存在时 dry-run violations 应为空，实际 %v", rep.GetViolations())
	}
	if f.state.getPlayerCalls != 1 {
		t.Fatalf("dry-run 应做一次存在性检查，实际 %d 次", f.state.getPlayerCalls)
	}
	if f.state.grantCalls != 0 {
		t.Fatalf("dry-run 不得调用底层，实际 %d 次", f.state.grantCalls)
	}
	rec := f.audit.record(t, rep.GetAuditId())
	if rec.Result != models.AuditSkippedDryRun || !rec.DryRun {
		t.Fatalf("dry-run 审计 = %+v，期望 SKIPPED_DRY_RUN 且 dry_run=true", rec)
	}
	if rec.IdempotencyKey != "" {
		t.Fatalf("dry-run 不得占用幂等键，实际 %q", rec.IdempotencyKey)
	}
	if f.audit.finalizes != 1 {
		t.Fatalf("dry-run 应收尾一次，实际 %d 次", f.audit.finalizes)
	}
}

// TestAdminGrantItemDryRunReportsMissingPlayer dry-run 的存在性检查：玩家不存在时把原因写入
// 回执 violations，仍零副作用（不调用底层发放），审计收尾仍是 SKIPPED_DRY_RUN。
func TestAdminGrantItemDryRunReportsMissingPlayer(t *testing.T) {
	f := newAdminFixture(100)
	f.state.missingPlayer = true
	ac := writeCtx("gm-1", "k-dry-missing")
	ac.DryRun = true
	rep, err := f.h.GrantItem(context.Background(), grantReq(ac, "p-404", 5))
	if err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if len(rep.GetViolations()) == 0 {
		t.Fatal("玩家不存在时 dry-run violations 不得为空（存在性检查缺失）")
	}
	if rep.GetApplied() || rep.GetReplayed() {
		t.Fatalf("dry-run 回执不得声称已改档/重放: %+v", rep)
	}
	if f.state.grantCalls != 0 {
		t.Fatalf("dry-run 不得调用底层发放，实际 %d 次", f.state.grantCalls)
	}
	rec := f.audit.record(t, rep.GetAuditId())
	if rec.Result != models.AuditSkippedDryRun || !rec.DryRun {
		t.Fatalf("dry-run 审计 = %+v，期望 SKIPPED_DRY_RUN 且 dry_run=true", rec)
	}
	if rec.IdempotencyKey != "" {
		t.Fatalf("dry-run 不得占用幂等键，实际 %q", rec.IdempotencyKey)
	}
}

// TestAdminGrantItemIdempotentReplay 同键同参重复：返回首次结果（replayed=true）、底层只执行一次。
func TestAdminGrantItemIdempotentReplay(t *testing.T) {
	f := newAdminFixture(100)
	req := grantReq(writeCtx("gm-1", "k-1"), "p-1", 5)
	first, err := f.h.GrantItem(context.Background(), req)
	if err != nil {
		t.Fatalf("首次发放: %v", err)
	}
	second, err := f.h.GrantItem(context.Background(), req)
	if err != nil {
		t.Fatalf("同键重复发放应返回首次结果，实际 %v", err)
	}
	if !second.GetReplayed() || !second.GetApplied() || second.GetAuditId() != first.GetAuditId() {
		t.Fatalf("重放回执不符: first=%+v second=%+v", first, second)
	}
	if f.state.grantCalls != 1 {
		t.Fatalf("同键重复不得重复执行，实际调用 %d 次", f.state.grantCalls)
	}
	// 审计只落一条：第二次是同键幂等命中（唯一索引冲突），不再占位。
	f.audit.onlyRecord(t)
}

// TestAdminGrantItemRejectsKeyReuse 同键改参：报 AdminIdempotencyKeyReused 且不执行第二次。
func TestAdminGrantItemRejectsKeyReuse(t *testing.T) {
	f := newAdminFixture(100)
	if _, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-1"), "p-1", 5)); err != nil {
		t.Fatalf("首次发放: %v", err)
	}
	_, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-1"), "p-1", 6))
	if !errorv1.IsAdminIdempotencyKeyReused(err) {
		t.Fatalf("同键改参错误 = %v，期望 AdminIdempotencyKeyReused", err)
	}
	if f.state.grantCalls != 1 {
		t.Fatalf("同键改参不得执行第二次，实际调用 %d 次", f.state.grantCalls)
	}
}

// TestAdminGrantItemAuditsFailure 业务失败：审计收尾 FAILED + reason/message/finished_at 落库。
func TestAdminGrantItemAuditsFailure(t *testing.T) {
	f := newAdminFixture(100)
	f.state.err = errorv1.ErrPlayerNotFound("玩家不存在")
	_, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-1"), "p-404", 5))
	if !errorv1.IsPlayerNotFound(err) {
		t.Fatalf("业务失败应原样返回，实际 %v", err)
	}
	rec := f.audit.onlyRecord(t)
	if rec.Result != models.AuditFailed {
		t.Fatalf("失败审计 result = %q，期望 FAILED", rec.Result)
	}
	if rec.Reason != errorv1.ReasonPlayerNotFound() || rec.Message == "" {
		t.Fatalf("失败审计应落 reason/message，实际 reason=%q message=%q", rec.Reason, rec.Message)
	}
	if rec.FinishedAt == 0 {
		t.Fatal("失败审计应写 finished_at")
	}
}

// TestAdminGrantItemRejectsWhenAuditInsertFails 审计占位失败 → 拒单且不执行副作用
// （不写审计就不改档是硬要求）。
func TestAdminGrantItemRejectsWhenAuditInsertFails(t *testing.T) {
	f := newAdminFixture(100)
	f.audit.insertErr = errors.New("mongo 不可用")
	_, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-1"), "p-1", 5))
	if !errorv1.IsInternal(err) {
		t.Fatalf("审计占位失败应拒单（Internal），实际 %v", err)
	}
	if f.state.grantCalls != 0 {
		t.Fatalf("审计占位失败不得执行副作用，实际调用 %d 次", f.state.grantCalls)
	}
}

// TestAdminAuditFinalizeFailureCountsMetric 审计收尾失败：记录保持 PENDING（不反向变业务失败），
// 并打点 admin_audit_finalize_failed_total（供运维发现需人工核查的残留 PENDING）。
func TestAdminAuditFinalizeFailureCountsMetric(t *testing.T) {
	f := newAdminFixture(100)
	f.audit.finalErr = errors.New("mongo 不可用")
	rep, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-1"), "p-1", 5))
	if err != nil {
		t.Fatalf("收尾失败不得反向变成业务失败，实际 %v", err)
	}
	if !rep.GetApplied() {
		t.Fatalf("副作用已发生，回执应为 applied=true: %+v", rep)
	}
	if got := f.meter.total(MetricAdminAuditFinalizeFailed); got != 1 {
		t.Fatalf("%s = %v，期望 1", MetricAdminAuditFinalizeFailed, got)
	}
	if rec := f.audit.record(t, rep.GetAuditId()); rec.Result != models.AuditPending || rec.FinishedAt != 0 {
		t.Fatalf("收尾失败应保留 PENDING 且 finished_at=0，实际 %+v", rec)
	}
}

// TestAdminQueryAuditsMapsFilters 验证管理面 operator_filter/target_id 映射到仓储过滤条件。
func TestAdminQueryAuditsMapsFilters(t *testing.T) {
	f := newAdminFixture(100)
	if _, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-1"), "p-1", 5)); err != nil {
		t.Fatalf("发放: %v", err)
	}
	rep, err := f.h.QueryAudits(context.Background(), &admingamev1.QueryAuditsRequest{
		Context:        &admingamev1.AdminContext{Operator: "gm-1"},
		TargetId:       "p-1",
		OperatorFilter: "gm-1",
		PageSize:       10,
	})
	if err != nil {
		t.Fatalf("QueryAudits: %v", err)
	}
	if f.audit.lastList.Operator != "gm-1" || f.audit.lastList.TargetID != "p-1" || f.audit.lastList.PageSize != 10 {
		t.Fatalf("过滤条件映射不符: %+v", f.audit.lastList)
	}
	if len(rep.GetEntries()) != 1 {
		t.Fatalf("审计条目数 = %d，期望 1", len(rep.GetEntries()))
	}
	entry := rep.GetEntries()[0]
	if entry.GetResult() != admingamev1.AuditResult_AUDIT_RESULT_SUCCESS || entry.GetAction() != admingamev1.AdminService_GrantItem_FullMethodName {
		t.Fatalf("审计投影不符: %+v", entry)
	}
}
