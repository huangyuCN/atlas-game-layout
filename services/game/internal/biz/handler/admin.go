package handler

// admin.go 是 game 管理面（GM/运维）实现：只做「校验 + 幂等 + 审计 + 转发」，
// 业务写经 biz.PlayerStateAccess 收敛到 PlayerActor 聚合根，本层不直连存储改业务数据。
//
// 发道具裁决顺序（任一步失败都不产生副作用）：
//  ① 校验：operator / 幂等键必填、参数合法、count <= 单次上限（dry-run 同样受限）
//  ② 审计占位（PENDING；dry-run 不写幂等键，稀疏唯一索引不占键）
//     键冲突 → 读首次记录：参数摘要相同 → 返回首次结果（replayed=true）
//     参数摘要不同 → AdminIdempotencyKeyReused；首次仍 PENDING → AdminOperationInFlight
//  ③ dry-run → 存在性检查（只读）→ 收尾 SKIPPED_DRY_RUN，**不改档**；
//     未通过项写入回执 violations（审计仍是 SKIPPED_DRY_RUN）
//  ④ 执行 biz.PlayerStateAccess.GrantItem（→ PlayerActor 聚合根单写者）
//  ⑤ 审计收尾 SUCCESS / FAILED（成功与失败都写）
//
// 鉴权前提：本轮内网明文、无鉴权、**不加 IP 白名单/ACL**（R12），operator 为调用方自报、可伪造，
// 审计的「谁」只在「内网可信」假设下成立；将来接外部服务必须由鉴权主体注入 operator
// （请求字段被覆盖或直接拒绝）并启用 mTLS。

import (
	"context"
	"errors"
	"time"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/idgen"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"go.opentelemetry.io/otel/attribute"
)

// AdminHandler 是 biz.AdminService 的实现（管理面：GM 发道具 + 查玩家/背包/审计）。
type AdminHandler struct {
	admingamev1.UnimplementedAdminServiceServer

	audit repo.AuditRepo
	state biz.PlayerStateAccess
	opts  biz.AdminOptions
}

// NewAdminHandler 构造管理面实现（单次上限缺省/0 回退 biz.DefaultMaxGrantCount）。
func NewAdminHandler(audit repo.AuditRepo, state biz.PlayerStateAccess, opts biz.AdminOptions) *AdminHandler {
	opts.MaxGrantCount = biz.NormalizeMaxGrantCount(opts.MaxGrantCount)
	if opts.NewAuditID == nil {
		opts.NewAuditID = func() string { return idgen.New("aud") }
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &AdminHandler{audit: audit, state: state, opts: opts}
}

// grantInput 是发道具请求的校验结果（幂等判定、审计与执行三处共用同一份归一结果）。
type grantInput struct {
	operator string
	key      string
	reason   string
	dryRun   bool
	playerID string
	itemID   uint32
	count    uint32
	params   string
	hash     string
}

// GrantItem 实现 admingamev1.AdminServiceServer（GM 发道具）。
func (h *AdminHandler) GrantItem(ctx context.Context, req *admingamev1.GrantItemRequest) (rep *admingamev1.GrantItemReply, err error) {
	ctx, span := observability.StartSpan(ctx, "admin.Game.GrantItem", grantAttrs(req)...)
	defer func() { observability.EndSpan(span, err) }()

	in, err := h.validateGrant(req)
	if err != nil {
		return nil, err
	}
	if in.dryRun {
		return h.dryRunGrant(ctx, in)
	}
	return h.applyGrant(ctx, in)
}

// QueryPlayer 实现管理面查玩家（读操作不要求幂等键，operator 仍必填）。
func (h *AdminHandler) QueryPlayer(ctx context.Context, req *admingamev1.QueryPlayerRequest) (rep *admingamev1.QueryPlayerReply, err error) {
	ctx, endSpan := startQuerySpan(ctx, "admin.Game.QueryPlayer", req.GetContext(), "player.id", req.GetPlayerId())
	defer func() { endSpan(err) }()

	if err := requireReadPlayer(req.GetContext(), req.GetPlayerId()); err != nil {
		return nil, err
	}
	summary, err := h.state.GetPlayer(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}
	if summary == nil {
		return nil, errorv1.ErrPlayerNotFound("玩家不存在")
	}
	return &admingamev1.QueryPlayerReply{
		PlayerId: summary.GetPlayerId(),
		Nickname: summary.GetNickname(),
		Level:    summary.GetLevel(),
	}, nil
}

// QueryBackpack 实现管理面查背包（读模型是管理面自有定义，此处做一次显式字段映射）。
func (h *AdminHandler) QueryBackpack(ctx context.Context, req *admingamev1.QueryBackpackRequest) (rep *admingamev1.QueryBackpackReply, err error) {
	ctx, endSpan := startQuerySpan(ctx, "admin.Game.QueryBackpack", req.GetContext(), "player.id", req.GetPlayerId())
	defer func() { endSpan(err) }()

	if err := requireReadPlayer(req.GetContext(), req.GetPlayerId()); err != nil {
		return nil, err
	}
	items, err := h.state.GetBackpack(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}
	return &admingamev1.QueryBackpackReply{PlayerId: req.GetPlayerId(), Items: adminBackpack(items)}, nil
}

// QueryAudits 实现管理面查审计（游标分页；页大小越界/游标非法报 InvalidParams）。
func (h *AdminHandler) QueryAudits(ctx context.Context, req *admingamev1.QueryAuditsRequest) (rep *admingamev1.QueryAuditsReply, err error) {
	ctx, endSpan := startQuerySpan(ctx, "admin.Game.QueryAudits", req.GetContext(), "audit.target_id", req.GetTargetId())
	defer func() { endSpan(err) }()

	if err := requireOperator(req.GetContext()); err != nil {
		return nil, err
	}
	page, err := h.audit.List(ctx, repo.AuditListOptions{
		TargetID: req.GetTargetId(),
		// operator_filter 是管理面的过滤字段名，映射到仓储的 Operator 过滤。
		Operator:  req.GetOperatorFilter(),
		PageSize:  req.GetPageSize(),
		PageToken: req.GetPageToken(),
	})
	if err != nil {
		if errors.Is(err, repo.ErrAuditPageSize) || errors.Is(err, repo.ErrAuditInvalidCursor) {
			return nil, errorv1.ErrInvalidParams("%s", err.Error())
		}
		return nil, errorv1.ErrInternal("查询审计失败")
	}
	return &admingamev1.QueryAuditsReply{
		Entries:       adminAuditEntries(page.Records),
		NextPageToken: page.NextPageToken,
	}, nil
}

// startQuerySpan 开启管理面读方法的 span（operator + 目标 ID 两个属性），返回带 span 的 ctx
// 与收尾函数；调用方以 `defer func() { endSpan(err) }()` 收尾（err 取具名返回值）。
func startQuerySpan(ctx context.Context, method string, ac *admingamev1.AdminContext, targetKey, targetID string) (context.Context, func(error)) {
	ctx, span := observability.StartSpan(ctx, method,
		attribute.String("admin.operator", ac.GetOperator()),
		attribute.String(targetKey, targetID))
	return ctx, func(err error) { observability.EndSpan(span, err) }
}

// requireReadPlayer 校验管理面读请求的公共部分：operator 必填（审计追责的「谁」，
// 读操作不要求幂等键）+ player_id 非空。
func requireReadPlayer(ac *admingamev1.AdminContext, playerID string) error {
	if err := requireOperator(ac); err != nil {
		return err
	}
	if playerID == "" {
		return errorv1.ErrInvalidParams("player_id 不能为空")
	}
	return nil
}

// validateGrant 校验发道具请求并归一为 grantInput。
// 顺序：operator → 幂等键 → 参数 → 单次上限——上限在**审计占位之前**裁决（超限零副作用零审计）。
func (h *AdminHandler) validateGrant(req *admingamev1.GrantItemRequest) (*grantInput, error) {
	ac := req.GetContext()
	if err := requireOperator(ac); err != nil {
		return nil, err
	}
	if ac.GetIdempotencyKey() == "" {
		return nil, errorv1.ErrAdminIdempotencyKeyMissing("管理面写操作必须携带幂等键")
	}
	if err := requireGrantParams(req); err != nil {
		return nil, err
	}
	if max := h.opts.MaxGrantCount; req.GetCount() > max {
		return nil, errorv1.ErrAdminGrantCountExceeded("单次发放 %d 件超过上限 %d 件", req.GetCount(), max)
	}
	digest := digestGrantParams(req)
	return &grantInput{
		operator: ac.GetOperator(),
		key:      ac.GetIdempotencyKey(),
		reason:   ac.GetReason(),
		dryRun:   ac.GetDryRun(),
		playerID: req.GetPlayerId(),
		itemID:   req.GetItemId(),
		count:    req.GetCount(),
		params:   digest.text,
		hash:     digest.hash,
	}, nil
}

// requireOperator 校验管理面请求的操作人（读与写都必填：审计追责的「谁」）。
func requireOperator(ac *admingamev1.AdminContext) error {
	if ac.GetOperator() == "" {
		return errorv1.ErrAdminOperatorMissing("管理面请求必须携带 operator")
	}
	return nil
}

// requireGrantParams 校验发道具的业务参数（缺一即 InvalidParams）。
func requireGrantParams(req *admingamev1.GrantItemRequest) error {
	switch {
	case req.GetPlayerId() == "":
		return errorv1.ErrInvalidParams("player_id 不能为空")
	case req.GetItemId() == 0:
		return errorv1.ErrInvalidParams("item_id 不能为空")
	case req.GetCount() == 0:
		return errorv1.ErrInvalidParams("count 必须大于 0")
	default:
		return nil
	}
}

// grantAttrs 组装发道具 span 属性（operator/玩家/道具/数量/是否 dry-run）。
func grantAttrs(req *admingamev1.GrantItemRequest) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("admin.operator", req.GetContext().GetOperator()),
		attribute.String("player.id", req.GetPlayerId()),
		attribute.Int("item.id", int(req.GetItemId())),
		attribute.Int("item.count", int(req.GetCount())),
		attribute.Bool("admin.dry_run", req.GetContext().GetDryRun()),
	}
}

// dryRunGrant 处理 dry-run：审计占位（PENDING）→ 存在性检查 → 收尾 SKIPPED_DRY_RUN，全程不改档。
// 边界：只做参数校验（含单次上限）与**存在性检查**，**不做并发竞争预演**——预演不等于预留，
// TOCTOU 仍存在，「预演通过」不等于「一定成功」，GM 不得据此当作预留或成功凭证。
// 未通过项只进回执 violations（dry-run 不是失败，审计不落 reason/message）。
func (h *AdminHandler) dryRunGrant(ctx context.Context, in *grantInput) (*admingamev1.GrantItemReply, error) {
	rec, err := h.insertAudit(ctx, in)
	if err != nil {
		return nil, errorv1.ErrInternal("审计占位失败，已拒单")
	}
	violations := h.dryRunViolations(ctx, in)
	h.finishAudit(ctx, rec.AuditID, models.AuditSkippedDryRun, "", "")
	return &admingamev1.GrantItemReply{AuditId: rec.AuditID, Violations: violations}, nil
}

// dryRunViolations 做 dry-run 的存在性检查（经域面 biz.PlayerStateAccess 只读查询，零副作用）：
// 目标玩家不存在即把原因写入 violations；域面查询失败同样记为未通过项（原因取结构化错误的 reason）。
func (h *AdminHandler) dryRunViolations(ctx context.Context, in *grantInput) []string {
	summary, err := h.state.GetPlayer(ctx, in.playerID)
	switch {
	case err != nil:
		reason, message := errorDetail(err)
		return []string{reason + ": " + message}
	case summary == nil:
		return []string{errorv1.ReasonPlayerNotFound() + ": 玩家不存在"}
	default:
		return nil
	}
}

// applyGrant 真发放：审计占位（冲突即幂等命中）→ 执行 → 收尾。
func (h *AdminHandler) applyGrant(ctx context.Context, in *grantInput) (*admingamev1.GrantItemReply, error) {
	rec, err := h.insertAudit(ctx, in)
	switch {
	case errors.Is(err, repo.ErrAuditDuplicateKey):
		return h.replayGrant(ctx, in)
	case err != nil:
		return nil, errorv1.ErrInternal("审计占位失败，已拒单")
	}
	if err := h.state.GrantItem(ctx, in.playerID, in.itemID, in.count, in.reason); err != nil {
		reason, message := errorDetail(err)
		h.finishAudit(ctx, rec.AuditID, models.AuditFailed, reason, message)
		return nil, err
	}
	h.finishAudit(ctx, rec.AuditID, models.AuditSuccess, "", "")
	return &admingamev1.GrantItemReply{AuditId: rec.AuditID, Applied: true}, nil
}

// replayGrant 处理同键重复（唯一索引冲突）：参数摘要一致返回首次结果，否则按语义拒单。
func (h *AdminHandler) replayGrant(ctx context.Context, in *grantInput) (*admingamev1.GrantItemReply, error) {
	first, err := h.audit.FindByIdempotencyKey(ctx, in.key)
	if errors.Is(err, repo.ErrAuditNotFound) {
		return nil, errorv1.ErrAdminOperationInFlight("同键请求在途，请稍后重试")
	}
	if err != nil {
		return nil, errorv1.ErrInternal("读取首次审计记录失败")
	}
	if first.ParamsHash != in.hash {
		return nil, errorv1.ErrAdminIdempotencyKeyReused("幂等键 %q 已用于不同参数的请求", in.key)
	}
	switch first.Result {
	case models.AuditSuccess:
		return &admingamev1.GrantItemReply{AuditId: first.AuditID, Applied: true, Replayed: true}, nil
	case models.AuditPending:
		return nil, errorv1.ErrAdminOperationInFlight("同键请求在途，请稍后重试")
	default:
		return nil, replayError(first)
	}
}

// replayError 还原首次失败结果：以落库的 reason/message 重建结构化错误
// （首次的错误码未落库，统一按内部错误码返回，reason 保留供调用方判定）。
func replayError(rec *models.AuditRecord) error {
	return atlaserrors.New(errorv1.CodeInternal(), rec.Reason, rec.Message)
}

// 静态保证 AdminHandler 实现 biz.AdminService。
var _ biz.AdminService = (*AdminHandler)(nil)
