package repo

// 本文件是管理面审计仓储（mongo 集合 admin_audits）：
// 幂等去重靠唯一**稀疏**索引 {idempotency_key}（dry-run 不写该字段，故必须稀疏）；
// 列表查询走不透明游标分页（created_at 降序 + _id 兜底，不重不漏）。

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// auditCollection 是管理面审计集合名。
const auditCollection = "admin_audits"

// 审计列表分页约定：默认 20、上限 200（超上限报错，不静默截断）。
const (
	defaultAuditPageSize = 20
	maxAuditPageSize     = 200
)

// duplicateKeyCode 是 mongo 唯一索引冲突错误码。
const duplicateKeyCode = 11000

// ErrAuditDuplicateKey 表示幂等键已存在（同键重复插入）：调用方按「幂等命中」处理，
// 读首次记录比对参数摘要，而不是把冲突当 500。
var ErrAuditDuplicateKey = errors.New("repo: 审计幂等键已存在")

// ErrAuditNotFound 表示审计记录不存在。
var ErrAuditNotFound = errors.New("repo: 审计记录不存在")

// ErrAuditInvalidCursor 表示分页游标非法（不透明游标被解析复用）。
var ErrAuditInvalidCursor = errors.New("repo: 审计分页游标非法")

// ErrAuditPageSize 表示分页大小越界（0 = 用默认值；上限见 maxAuditPageSize）。
var ErrAuditPageSize = errors.New("repo: 审计分页大小越界")

// AuditFinalize 是审计收尾内容（成功与失败都写；dry-run 写 SKIPPED_DRY_RUN）。
type AuditFinalize struct {
	// Result 是终态：SUCCESS / FAILED / SKIPPED_DRY_RUN（PENDING 不允许作为收尾值）。
	Result models.AuditResult
	// Reason 是失败 reason（成功为空）。
	Reason string
	// Message 是失败描述（成功为空）。
	Message string
	// FinishedAt 是收尾时间（unix 毫秒）。
	FinishedAt int64
}

// AuditListOptions 是审计列表查询条件（游标分页；空 token = 首页）。
type AuditListOptions struct {
	// TargetID 按目标过滤（空 = 不过滤）。
	TargetID string
	// Operator 按操作人过滤（空 = 不过滤）。
	Operator string
	// PageSize 页大小（0 = 默认 20；上限 200，超限报 ErrAuditPageSize）。
	PageSize uint32
	// PageToken 不透明游标（上一页 next_page_token；不得解析复用）。
	PageToken string
}

// AuditPage 是一页审计记录。
type AuditPage struct {
	// Records 是本页记录（created_at 降序 + _id 兜底）。
	Records []*models.AuditRecord
	// NextPageToken 是下一页游标（空 = 末页）。
	NextPageToken string
}

// AuditRepo 是管理面审计仓储接口（biz 层依赖；实现见 MongoAuditRepo）。
type AuditRepo interface {
	// Insert 写入审计占位记录（PENDING）；幂等键已存在返回 ErrAuditDuplicateKey。
	Insert(ctx context.Context, rec *models.AuditRecord) error
	// Finalize 收尾审计记录（写终态 + reason/message + finished_at）。
	Finalize(ctx context.Context, auditID string, in AuditFinalize) error
	// FindByIdempotencyKey 按幂等键读取首次记录（幂等命中判定）；不存在返回 ErrAuditNotFound。
	FindByIdempotencyKey(ctx context.Context, key string) (*models.AuditRecord, error)
	// List 游标分页查询（created_at 降序 + _id 兜底，不重不漏）。
	List(ctx context.Context, opts AuditListOptions) (*AuditPage, error)
	// EnsureIndexes 创建集合索引（含幂等键唯一稀疏索引）。
	EnsureIndexes(ctx context.Context) error
}

// MongoAuditRepo 是审计仓储的 mongo 实现。
type MongoAuditRepo struct {
	cli  *mongo.Client
	coll *mongodriver.Collection
}

// NewMongoAuditRepo 构造审计仓储（启动期建索引；索引建不上即装配失败）。
func NewMongoAuditRepo(ctx context.Context, cli *mongo.Client) (*MongoAuditRepo, error) {
	r := &MongoAuditRepo{cli: cli, coll: cli.Collection(auditCollection)}
	if err := r.EnsureIndexes(ctx); err != nil {
		return nil, fmt.Errorf("repo: 初始化审计索引失败: %w", err)
	}
	return r, nil
}

// EnsureIndexes 创建审计集合索引（幂等；启动期调用）：
//   - uniq_idempotency_key：唯一 + **稀疏**（dry-run 不写键，多条缺字段文档不冲突）；
//   - idx_created_at：列表分页排序键；
//   - idx_target_created：按目标追查；
//   - idx_operator_created：按人追查。
func (r *MongoAuditRepo) EnsureIndexes(ctx context.Context) error {
	return r.cli.EnsureIndexes(ctx, auditCollection, []mongo.Index{
		{Keys: bson.D{{Key: "idempotency_key", Value: 1}}, Unique: true, Sparse: true, Name: "uniq_idempotency_key"},
		{Keys: bson.D{{Key: "created_at", Value: -1}}, Name: "idx_created_at"},
		{Keys: bson.D{{Key: "target_type", Value: 1}, {Key: "target_id", Value: 1}, {Key: "created_at", Value: -1}}, Name: "idx_target_created"},
		{Keys: bson.D{{Key: "operator", Value: 1}, {Key: "created_at", Value: -1}}, Name: "idx_operator_created"},
	})
}

// Insert 写入审计占位记录；唯一索引冲突归一为 ErrAuditDuplicateKey（幂等命中，不是 500）。
func (r *MongoAuditRepo) Insert(ctx context.Context, rec *models.AuditRecord) error {
	if _, err := r.coll.InsertOne(ctx, rec); err != nil {
		return insertError(err)
	}
	return nil
}

// insertError 归一审计插入错误：唯一索引冲突 → ErrAuditDuplicateKey；其余包装返回（不吞错）。
func insertError(err error) error {
	if isDuplicateKey(err) {
		return ErrAuditDuplicateKey
	}
	return fmt.Errorf("repo: 写入审计记录失败: %w", err)
}

// isDuplicateKey 判定 mongo 错误是否为唯一索引冲突（错误码 11000，支持包装穿透）。
func isDuplicateKey(err error) bool {
	var we mongodriver.WriteException
	if errors.As(err, &we) {
		for _, e := range we.WriteErrors {
			if e.Code == duplicateKeyCode {
				return true
			}
		}
	}
	return mongodriver.IsDuplicateKeyError(err)
}

// Finalize 收尾审计记录；记录不存在返回 ErrAuditNotFound（调用方据此告警，不改档语义）。
func (r *MongoAuditRepo) Finalize(ctx context.Context, auditID string, in AuditFinalize) error {
	if !in.Result.Terminal() {
		return fmt.Errorf("repo: 审计收尾状态非法: %q", in.Result)
	}
	res, err := r.coll.UpdateOne(ctx, bson.M{"_id": auditID}, bson.M{"$set": bson.M{
		"result": in.Result, "reason": in.Reason, "message": in.Message, "finished_at": in.FinishedAt,
	}})
	if err != nil {
		return fmt.Errorf("repo: 收尾审计记录失败: %w", err)
	}
	if res.MatchedCount == 0 {
		return ErrAuditNotFound
	}
	return nil
}

// FindByIdempotencyKey 按幂等键读取首次记录；空键（dry-run 不占键）直接判不存在。
func (r *MongoAuditRepo) FindByIdempotencyKey(ctx context.Context, key string) (*models.AuditRecord, error) {
	if key == "" {
		return nil, ErrAuditNotFound
	}
	return r.findOne(ctx, bson.M{"idempotency_key": key})
}

// findOne 按过滤条件读取单条审计记录；不存在返回 ErrAuditNotFound。
func (r *MongoAuditRepo) findOne(ctx context.Context, filter bson.M) (*models.AuditRecord, error) {
	return findOneDoc[models.AuditRecord](ctx, r.coll, filter, "审计记录", ErrAuditNotFound)
}

// List 游标分页查询：多取一条判定是否还有下一页，返回不透明 next_page_token（空 = 末页）。
func (r *MongoAuditRepo) List(ctx context.Context, opts AuditListOptions) (*AuditPage, error) {
	filter, size, err := auditListQuery(opts)
	if err != nil {
		return nil, err
	}
	cur, err := r.coll.Find(ctx, filter, options.Find().SetSort(auditSortDoc()).SetLimit(int64(size)+1))
	if err != nil {
		return nil, fmt.Errorf("repo: 查询审计列表失败: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()
	var recs []*models.AuditRecord
	if err := cur.All(ctx, &recs); err != nil {
		return nil, fmt.Errorf("repo: 解码审计列表失败: %w", err)
	}
	page := &AuditPage{Records: recs}
	if uint32(len(recs)) > size {
		page.Records = recs[:size]
		last := page.Records[size-1]
		page.NextPageToken = encodeAuditCursor(auditCursor{CreatedAt: last.CreatedAt, ID: last.AuditID})
	}
	return page, nil
}

// auditListQuery 组装列表查询：过滤条件 + 游标（首页无 $or）+ 归一后的页大小。
func auditListQuery(opts AuditListOptions) (bson.M, uint32, error) {
	size, err := normalizeAuditPageSize(opts.PageSize)
	if err != nil {
		return nil, 0, err
	}
	filter := bson.M{}
	if opts.TargetID != "" {
		filter["target_id"] = opts.TargetID
	}
	if opts.Operator != "" {
		filter["operator"] = opts.Operator
	}
	if opts.PageToken == "" {
		return filter, size, nil
	}
	c, err := decodeAuditCursor(opts.PageToken)
	if err != nil {
		return nil, 0, err
	}
	// 降序排序键 (created_at, _id) 的严格后继：时间更早，或同毫秒但审计号更小。
	filter["$or"] = []bson.M{
		{"created_at": bson.M{"$lt": c.CreatedAt}},
		{"created_at": c.CreatedAt, "_id": bson.M{"$lt": c.ID}},
	}
	return filter, size, nil
}

// normalizeAuditPageSize 归一页大小：0 = 默认 20，超上限报错（不静默截断）。
func normalizeAuditPageSize(size uint32) (uint32, error) {
	switch {
	case size == 0:
		return defaultAuditPageSize, nil
	case size > maxAuditPageSize:
		return 0, fmt.Errorf("%w: %d > %d", ErrAuditPageSize, size, maxAuditPageSize)
	default:
		return size, nil
	}
}

// auditSortDoc 是列表排序键：created_at 降序 + _id 兜底（同毫秒记录也有稳定全序）。
func auditSortDoc() bson.D {
	return bson.D{{Key: "created_at", Value: -1}, {Key: "_id", Value: -1}}
}

// auditCursor 是列表游标（排序键取值：created_at + 审计号）。
type auditCursor struct {
	CreatedAt int64
	ID        string
}

// encodeAuditCursor 把游标编码为不透明 token（base64url；调用方不得解析复用）。
func encodeAuditCursor(c auditCursor) string {
	raw := strconv.FormatInt(c.CreatedAt, 10) + ":" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeAuditCursor 解析游标 token；非法（非 base64/缺分隔/时间非数字/审计号为空）报 ErrAuditInvalidCursor。
func decodeAuditCursor(token string) (auditCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return auditCursor{}, fmt.Errorf("%w: %v", ErrAuditInvalidCursor, err)
	}
	ts, id, ok := strings.Cut(string(raw), ":")
	if !ok {
		return auditCursor{}, fmt.Errorf("%w: 缺少分隔符", ErrAuditInvalidCursor)
	}
	createdAt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return auditCursor{}, fmt.Errorf("%w: 时间戳非法", ErrAuditInvalidCursor)
	}
	if id == "" {
		return auditCursor{}, fmt.Errorf("%w: 审计号为空", ErrAuditInvalidCursor)
	}
	return auditCursor{CreatedAt: createdAt, ID: id}, nil
}
