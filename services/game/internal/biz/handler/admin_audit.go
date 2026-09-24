package handler

// admin_audit.go 是管理面审计的落库与投影辅助：占位（PENDING）、收尾（终态）、
// 参数摘要哈希（同键改参判定）、链路 ID 提取与对外读模型映射。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/metrics"
	"go.opentelemetry.io/otel/trace"
)

// maxAuditParamsLen 是审计参数摘要的存储上限（超出截断，不落敏感原文）。
const maxAuditParamsLen = 256

// MetricAdminAuditFinalizeFailed 是审计收尾失败计数：记录保持 PENDING（不扫尾），
// 同键重试报「操作在途」，需人工按 created_at 核查后补单。
const MetricAdminAuditFinalizeFailed = "admin_audit_finalize_failed_total"

// paramsDigest 是审计参数摘要：text 截断后落库，hash 为全量摘要（同键改参判定）。
type paramsDigest struct {
	text string
	hash string
}

// digestGrantParams 生成发道具参数摘要（文本截断落库 + 全量哈希判定改参）。
func digestGrantParams(req *admingamev1.GrantItemRequest) paramsDigest {
	raw := fmt.Sprintf("player_id=%s,item_id=%d,count=%d,reason=%s",
		req.GetPlayerId(), req.GetItemId(), req.GetCount(), req.GetContext().GetReason())
	sum := sha256.Sum256([]byte(raw))
	return paramsDigest{text: truncateText(raw, maxAuditParamsLen), hash: hex.EncodeToString(sum[:])}
}

// truncateText 按字节上限截断文本（截断即加省略标记，内审能看出摘要不完整）。
func truncateText(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "…"
}

// insertAudit 写入审计占位记录（PENDING）；dry-run 不写幂等键（稀疏唯一索引不占键）。
// 幂等键已存在返回 repo.ErrAuditDuplicateKey，调用方按幂等命中处理（不是 500）。
func (h *AdminHandler) insertAudit(ctx context.Context, in *grantInput) (*models.AuditRecord, error) {
	key := in.key
	if in.dryRun {
		key = ""
	}
	rec := &models.AuditRecord{
		AuditID:    h.opts.NewAuditID(),
		Operator:   in.operator,
		Action:     admingamev1.AdminService_GrantItem_FullMethodName,
		TargetType: models.AuditTargetPlayer,
		TargetID:   in.playerID,
		Params:     in.params,
		ParamsHash: in.hash,
		// dry-run 不占幂等键（omitempty + 稀疏唯一索引）。
		IdempotencyKey: key,
		DryRun:         in.dryRun,
		Result:         models.AuditPending,
		TraceID:        traceIDOf(ctx),
		CreatedAt:      h.opts.Now().UnixMilli(),
	}
	if err := h.audit.Insert(ctx, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// finishAudit 审计收尾（成功与失败都写）。收尾失败只记 ERROR 日志 + 指标
// admin_audit_finalize_failed_total：记录保持 PENDING，同键重试报「操作在途」，
// 但不把收尾失败反向变成业务失败（副作用可能已发生）。
func (h *AdminHandler) finishAudit(ctx context.Context, auditID string, result models.AuditResult, reason, message string) {
	err := h.audit.Finalize(ctx, auditID, repo.AuditFinalize{
		Result:     result,
		Reason:     reason,
		Message:    message,
		FinishedAt: h.opts.Now().UnixMilli(),
	})
	if err != nil {
		atlaslog.Errorf("管理面审计收尾失败（记录保持 PENDING，同键重试报操作在途）: audit_id=%s result=%s err=%v",
			auditID, result, err)
		countFinalizeFailed(h.opts.Meter)
	}
}

// countFinalizeFailed 打点一次审计收尾失败（meter 为 nil/noop 时短路，未配置后端零分配）。
func countFinalizeFailed(c metrics.Collector) {
	if c == nil || metrics.IsNoop(c) {
		return
	}
	c.Counter(MetricAdminAuditFinalizeFailed).Add(1)
}

// errorDetail 拆出结构化错误的 reason 与描述（非结构化错误归内部错误 reason）。
func errorDetail(err error) (string, string) {
	se := atlaserrors.FromError(err)
	if se == nil {
		return errorv1.ReasonInternal(), err.Error()
	}
	return se.Reason, se.Message
}

// traceIDOf 取当前 span 的 trace id；无有效 span（未启用链路导出）时返回空串，不伪造链路标识。
func traceIDOf(ctx context.Context) string {
	sc := trace.SpanFromContext(ctx).SpanContext()
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// adminBackpack 把域背包条目映射为管理面读模型（显式字段映射，不共用定义）。
func adminBackpack(items []*gamev1.BackpackItem) []*admingamev1.BackpackItem {
	out := make([]*admingamev1.BackpackItem, 0, len(items))
	for _, it := range items {
		out = append(out, &admingamev1.BackpackItem{ItemId: it.GetItemId(), Count: it.GetCount()})
	}
	return out
}

// adminAuditEntries 把审计记录映射为管理面读模型（不落敏感原文，params 不出面；
// result 走协议枚举，protojson 下发枚举名）。
func adminAuditEntries(recs []*models.AuditRecord) []*admingamev1.AuditEntry {
	out := make([]*admingamev1.AuditEntry, 0, len(recs))
	for _, rec := range recs {
		out = append(out, &admingamev1.AuditEntry{
			AuditId:   rec.AuditID,
			Operator:  rec.Operator,
			Action:    rec.Action,
			TargetId:  rec.TargetID,
			Result:    auditResultOf(rec.Result),
			Reason:    rec.Reason,
			TraceId:   rec.TraceID,
			CreatedAt: rec.CreatedAt,
			DryRun:    rec.DryRun,
		})
	}
	return out
}

// auditResultOf 把落库的结果状态映射为管理面协议枚举；未定义状态归 UNSPECIFIED
// （不猜测、不把未知值硬塞进已定义取值）。
func auditResultOf(r models.AuditResult) admingamev1.AuditResult {
	switch r {
	case models.AuditPending:
		return admingamev1.AuditResult_AUDIT_RESULT_PENDING
	case models.AuditSuccess:
		return admingamev1.AuditResult_AUDIT_RESULT_SUCCESS
	case models.AuditFailed:
		return admingamev1.AuditResult_AUDIT_RESULT_FAILED
	case models.AuditSkippedDryRun:
		return admingamev1.AuditResult_AUDIT_RESULT_SKIPPED_DRY_RUN
	default:
		return admingamev1.AuditResult_AUDIT_RESULT_UNSPECIFIED
	}
}
