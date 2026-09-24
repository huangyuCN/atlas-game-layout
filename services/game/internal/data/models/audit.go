package models

// 本文件是管理面审计记录模型（mongo 集合 admin_audits）：
// 一次管理面写请求一条记录（dry-run 也留痕），成功与失败都写。

// AuditResult 是审计记录的结果状态（有限集合，杜绝魔法字符串）。
type AuditResult string

// AuditResult 的取值。
const (
	// AuditPending 表示审计已占位、业务副作用尚未收尾（同键重试报「操作在途」）。
	AuditPending AuditResult = "PENDING"
	// AuditSuccess 表示业务副作用成功完成。
	AuditSuccess AuditResult = "SUCCESS"
	// AuditFailed 表示业务副作用失败（失败同样留痕）。
	AuditFailed AuditResult = "FAILED"
	// AuditSkippedDryRun 表示 dry-run 仅做校验、未改档。
	AuditSkippedDryRun AuditResult = "SKIPPED_DRY_RUN"
)

// Valid 判定结果状态是否属于已定义取值（拒绝未定义状态落库）。
func (r AuditResult) Valid() bool {
	switch r {
	case AuditPending, AuditSuccess, AuditFailed, AuditSkippedDryRun:
		return true
	default:
		return false
	}
}

// Terminal 判定结果状态是否已收尾（PENDING 之外均为终态）。
func (r AuditResult) Terminal() bool { return r.Valid() && r != AuditPending }

// AuditTargetType 是审计目标类型（有限集合：对谁做了操作）。
type AuditTargetType string

// AuditTargetType 的取值。
const (
	// AuditTargetPlayer 表示目标是一份玩家档（GM 发道具、查档）。
	AuditTargetPlayer AuditTargetType = "player"
	// AuditTargetItem 表示目标是一个道具定义（按道具维度的管理操作）。
	AuditTargetItem AuditTargetType = "item"
)

// Valid 判定目标类型是否属于已定义取值。
func (t AuditTargetType) Valid() bool {
	switch t {
	case AuditTargetPlayer, AuditTargetItem:
		return true
	default:
		return false
	}
}

// AuditRecord 是管理面审计记录：一次管理面写请求一条（dry-run 也留痕）。
// 集合 admin_audits 的索引（见 repo.MongoAuditRepo.EnsureIndexes）：
// 唯一**稀疏**索引 {idempotency_key}（幂等去重真源；dry-run 不写键故必须稀疏）、
// {created_at:-1}（列表）、{target_type,target_id,created_at:-1}（按目标追查）、
// {operator,created_at:-1}（按人追查）。
type AuditRecord struct {
	// AuditID 是审计号（回执给调用方，供补单/纠纷/内审引用）。
	AuditID string `bson:"_id"`
	// Operator 是谁：本轮由调用方自报（内网无鉴权、可伪造）；将来取鉴权主体。
	Operator string `bson:"operator"`
	// Action 做了什么：完整 gRPC 方法名（如 /admin.game.v1.AdminService/GrantItem，
	// 由生成物 AdminService_GrantItem_FullMethodName 提供，不手写字符串）。
	Action string `bson:"action"`
	// TargetType 对谁：目标类型。
	TargetType AuditTargetType `bson:"target_type"`
	// TargetID 对谁：目标 ID。
	TargetID string `bson:"target_id"`
	// Params 是参数摘要（脱敏 + 截断，不落敏感原文）。
	Params string `bson:"params"`
	// ParamsHash 是参数摘要哈希（同键改参判定）。
	ParamsHash string `bson:"params_hash"`
	// IdempotencyKey 是幂等键；dry-run 不写该字段（omitempty + 稀疏唯一索引）。
	IdempotencyKey string `bson:"idempotency_key,omitempty"`
	// DryRun 表示只校验不改档。
	DryRun bool `bson:"dry_run"`
	// Result 是结果状态（取值见 AuditResult）。
	Result AuditResult `bson:"result"`
	// Reason 是失败 reason（成功为空）。
	Reason string `bson:"reason"`
	// Message 是失败描述（成功为空）。
	Message string `bson:"message"`
	// TraceID 是链路关联（otel span context）。
	TraceID string `bson:"trace_id"`
	// CreatedAt 是占位时间（unix 毫秒）。
	CreatedAt int64 `bson:"created_at"`
	// FinishedAt 是收尾时间（unix 毫秒；未收尾 = 0）。
	FinishedAt int64 `bson:"finished_at"`
}
