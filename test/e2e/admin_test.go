package e2e

// admin_test.go 是 game 管理面（admin.game.v1.AdminService）的端到端用例：
// 发放闭环与审计可查、同键幂等重放、单次上限（R12），以及「管理面只在 internal 面可达」的安全边界。
// 说明：进程内装配不初始化链路导出（noop provider），用例装内存导出器让 span 有效，
// 从而验证审计 trace_id 确实来自链路上下文（而不是恒为空）。

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/pkg/spanstest"
)

// adminItemID 是用例发放的道具 ID。
const adminItemID uint32 = 1001

// adminEnv 是管理面 e2e 夹具：管理面客户端 + 已登录玩家 + 请求上下文。
// 发放与查询都经域面收敛到 PlayerActor，要求聚合根在线，故先经网关注册登录。
type adminEnv struct {
	admin    admingamev1.AdminServiceClient
	playerID string
	ctx      context.Context
}

// newAdminEnv 起 game/gateway，注册登录一名玩家，并拨号 internal 面的管理面。
func newAdminEnv(t *testing.T) *adminEnv {
	t.Helper()
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	// 审计 trace_id 取自 span 上下文：进程内装配是 noop provider，装内存导出器才有有效 span。
	_, restore := spanstest.Install()
	t.Cleanup(restore)

	game := newGame(t)
	gw := newGateway(t, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	sess, _ := dialBizSession(t, gw.TCPURL)
	loginFlow(t, ctx, sess)
	return &adminEnv{admin: dialAdmin(t, game.GRPCURL), playerID: sess.PlayerID(), ctx: ctx}
}

// itemCount 统计背包读模型里指定道具的数量（管理面与域面读模型共用本统计）。
func itemCount(items []*admingamev1.BackpackItem, itemID uint32) uint32 {
	var n uint32
	for _, it := range items {
		if it.GetItemId() == itemID {
			n += it.GetCount()
		}
	}
	return n
}

// grantOnce 发放一次道具并返回回执（失败即终止用例）。
func grantOnce(t *testing.T, env *adminEnv, key string, count uint32) *admingamev1.GrantItemReply {
	t.Helper()
	rep, err := env.admin.GrantItem(env.ctx, &admingamev1.GrantItemRequest{
		Context: adminCtx("e2e-admin", key, false), PlayerId: env.playerID, ItemId: adminItemID, Count: count,
	})
	if err != nil {
		t.Fatalf("GrantItem(count=%d): %v", count, err)
	}
	return rep
}

// backpackCount 查管理面背包并返回指定道具的数量。
func backpackCount(t *testing.T, env *adminEnv, itemID uint32) uint32 {
	t.Helper()
	rep, err := env.admin.QueryBackpack(env.ctx, &admingamev1.QueryBackpackRequest{
		Context: adminCtx("e2e-admin", "", false), PlayerId: env.playerID,
	})
	if err != nil {
		t.Fatalf("QueryBackpack: %v", err)
	}
	return itemCount(rep.GetItems(), itemID)
}

// auditEntries 按目标查一页审计（页大小取仓储上限 200）。
func auditEntries(t *testing.T, env *adminEnv, targetID string) []*admingamev1.AuditEntry {
	t.Helper()
	rep, err := env.admin.QueryAudits(env.ctx, &admingamev1.QueryAuditsRequest{
		Context: adminCtx("e2e-admin", "", false), TargetId: targetID, PageSize: 200,
	})
	if err != nil {
		t.Fatalf("QueryAudits: %v", err)
	}
	return rep.GetEntries()
}

// findAudit 在审计投影里按审计号取记录（不存在即失败）。
func findAudit(t *testing.T, entries []*admingamev1.AuditEntry, auditID string) *admingamev1.AuditEntry {
	t.Helper()
	for _, e := range entries {
		if e.GetAuditId() == auditID {
			return e
		}
	}
	t.Fatalf("审计记录 %s 不在投影里（共 %d 条）", auditID, len(entries))
	return nil
}

// mongoAudit 直查 admin_audits 集合（验证 handler 真落库，而不是只信管理面投影）。
func mongoAudit(t *testing.T, auditID string) bson.M {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err := pkgmongo.NewClient(ctx, pkgmongo.Options{URI: itMongoURI, Database: itMongoDB})
	if err != nil {
		t.Fatalf("mongo 连接: %v", err)
	}
	defer func() { _ = cli.Close(ctx) }()
	var doc bson.M
	if err := cli.Collection("admin_audits").FindOne(ctx, bson.M{"_id": auditID}).Decode(&doc); err != nil {
		t.Fatalf("直查审计记录 %s: %v", auditID, err)
	}
	return doc
}

// TestE2EAdminGrant 验证发放闭环：applied=true/replayed=false/audit_id 非空、背包数量正确，
// 且审计两路可查（管理面投影 + mongo 直查），trace_id 来自链路上下文。
func TestE2EAdminGrant(t *testing.T) {
	env := newAdminEnv(t)
	key := "grant-" + uuid.NewString()
	rep := grantOnce(t, env, key, 5)
	if !rep.GetApplied() || rep.GetReplayed() || rep.GetAuditId() == "" {
		t.Fatalf("发放回执不符（applied=true/replayed=false/audit_id 非空）: %+v", rep)
	}
	if got := backpackCount(t, env, adminItemID); got != 5 {
		t.Fatalf("背包数量 = %d, 期望 5", got)
	}

	entry := findAudit(t, auditEntries(t, env, env.playerID), rep.GetAuditId())
	if entry.GetOperator() != "e2e-admin" || entry.GetResult() != admingamev1.AuditResult_AUDIT_RESULT_SUCCESS ||
		entry.GetAction() != admingamev1.AdminService_GrantItem_FullMethodName ||
		entry.GetTargetId() != env.playerID || entry.GetCreatedAt() == 0 {
		t.Fatalf("审计投影字段不符: %+v", entry)
	}
	if entry.GetTraceId() == "" {
		t.Fatal("审计 trace_id 为空：链路不可追（span 上下文未落库）")
	}

	doc := mongoAudit(t, rep.GetAuditId())
	if doc["idempotency_key"] != key || doc["params_hash"] == "" || doc["finished_at"] == int64(0) {
		t.Fatalf("审计落库字段不符: %+v", doc)
	}
}

// TestE2EAdminIdempotentReplay 验证同键同参幂等重放：第二次 replayed=true、背包不叠加、
// 该键只留一条 SUCCESS 审计。
func TestE2EAdminIdempotentReplay(t *testing.T) {
	env := newAdminEnv(t)
	key := "replay-" + uuid.NewString()
	first := grantOnce(t, env, key, 5)
	second := grantOnce(t, env, key, 5)
	if !second.GetReplayed() || !second.GetApplied() || second.GetAuditId() != first.GetAuditId() {
		t.Fatalf("幂等重放回执不符: first=%+v second=%+v", first, second)
	}
	if got := backpackCount(t, env, adminItemID); got != 5 {
		t.Fatalf("幂等重放不得叠加背包，实际 %d", got)
	}
	var successes int
	for _, e := range auditEntries(t, env, env.playerID) {
		if e.GetResult() == admingamev1.AuditResult_AUDIT_RESULT_SUCCESS {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("SUCCESS 审计条数 = %d, 期望 1", successes)
	}
}

// TestE2EAdminDryRunDoesNotClaimKey 验证 dry-run 不占幂等键的闭环：同键先 dry-run
// （背包不变、审计 SKIPPED_DRY_RUN、mongo 记录无 idempotency_key），随后用**同一个键**
// 真发放成功——证明 dry-run 未占键、不能用来防重（稀疏唯一索引的语义钉死）。
func TestE2EAdminDryRunDoesNotClaimKey(t *testing.T) {
	env := newAdminEnv(t)
	key := "dry-run-" + uuid.NewString()
	dry, err := env.admin.GrantItem(env.ctx, &admingamev1.GrantItemRequest{
		Context: adminCtx("e2e-admin", key, true), PlayerId: env.playerID, ItemId: adminItemID, Count: 7,
	})
	if err != nil {
		t.Fatalf("dry-run 发放: %v", err)
	}
	if dry.GetApplied() || dry.GetReplayed() || dry.GetAuditId() == "" {
		t.Fatalf("dry-run 回执不符（applied=false/replayed=false/audit_id 非空）: %+v", dry)
	}
	if len(dry.GetViolations()) != 0 {
		t.Fatalf("玩家存在时 dry-run violations 应为空，实际 %v", dry.GetViolations())
	}
	if got := backpackCount(t, env, adminItemID); got != 0 {
		t.Fatalf("dry-run 不得改档，背包数量 = %d", got)
	}
	entry := findAudit(t, auditEntries(t, env, env.playerID), dry.GetAuditId())
	if entry.GetResult() != admingamev1.AuditResult_AUDIT_RESULT_SKIPPED_DRY_RUN || !entry.GetDryRun() {
		t.Fatalf("dry-run 审计投影不符（期望 SKIPPED_DRY_RUN + dry_run=true）: %+v", entry)
	}
	if doc := mongoAudit(t, dry.GetAuditId()); doc["idempotency_key"] != nil {
		t.Fatalf("dry-run 不得落幂等键（稀疏索引）: %+v", doc)
	}

	// 同键真发放：dry-run 未占键，故这里必须是首次请求语义（成功且非重放）。
	rep := grantOnce(t, env, key, 7)
	if !rep.GetApplied() || rep.GetReplayed() {
		t.Fatalf("同键真发放应成功且非重放（证明 dry-run 未占键）: %+v", rep)
	}
	if got := backpackCount(t, env, adminItemID); got != 7 {
		t.Fatalf("同键真发放后背包数量 = %d, 期望 7", got)
	}
}

// TestE2EAdminGrantCap 验证单次发放上限（R12）：count=max+1（含 dry-run）被拒且背包不变、
// 无审计记录；count=max 成功。嵌入式形态未配 admin.max_grant_count → 回退默认 100。
func TestE2EAdminGrantCap(t *testing.T) {
	env := newAdminEnv(t)
	const max = 100
	for _, dryRun := range []bool{false, true} {
		_, err := env.admin.GrantItem(env.ctx, &admingamev1.GrantItemRequest{
			Context:  adminCtx("e2e-admin", "cap-"+uuid.NewString(), dryRun),
			PlayerId: env.playerID, ItemId: adminItemID, Count: max + 1,
		})
		if !errorv1.IsAdminGrantCountExceeded(err) {
			t.Fatalf("dry_run=%v 超限错误 = %v, 期望 AdminGrantCountExceeded", dryRun, err)
		}
	}
	if got := backpackCount(t, env, adminItemID); got != 0 {
		t.Fatalf("超限被拒后背包不得变化，实际 %d", got)
	}
	if entries := auditEntries(t, env, env.playerID); len(entries) != 0 {
		t.Fatalf("超限被拒不得写审计，实际 %d 条", len(entries))
	}
	if rep := grantOnce(t, env, "cap-max-"+uuid.NewString(), max); !rep.GetApplied() {
		t.Fatalf("count=max 应发放成功: %+v", rep)
	}
	if got := backpackCount(t, env, adminItemID); got != max {
		t.Fatalf("背包数量 = %d, 期望 %d", got, max)
	}
}

// TestE2EAdminEdgeFaceRejectsAdminService 验证安全边界（钉死）：edge 面（不可信区）不暴露管理面
// ——调 AdminService/GrantItem 报 Unimplemented；internal 面（可信区）同一服务正常可达。
func TestE2EAdminEdgeFaceRejectsAdminService(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	game := newGame(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := dialAdmin(t, game.EdgeGRPCURL).GrantItem(ctx, &admingamev1.GrantItemRequest{
		Context:  adminCtx("e2e-edge", "edge-"+uuid.NewString(), false),
		PlayerId: "p-edge", ItemId: adminItemID, Count: 1,
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("edge 面调管理面应报 Unimplemented，实际 %v", err)
	}

	rep, err := dialAdmin(t, game.GRPCURL).QueryAudits(ctx, &admingamev1.QueryAuditsRequest{
		Context: adminCtx("e2e-edge", "", false), PageSize: 1,
	})
	if err != nil {
		t.Fatalf("internal 面调管理面应正常: %v", err)
	}
	if rep == nil {
		t.Fatal("internal 面管理面回执为空")
	}
}
