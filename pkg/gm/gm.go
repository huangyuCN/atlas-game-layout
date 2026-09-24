// Package gm 是 game 管理面（admin.game.v1.AdminService）的 GM SDK：
// 面向 GM/运维工具封装「内网 gRPC 拨号 + 统一请求信封（AdminContext）+ 幂等键 + 审计游标翻页」。
//
// 安全前提：管理面只注册在 game 服务的 internal listener（配置 server.grpc.internal_addr），
// 本轮内网明文、无鉴权，operator 由调用方自报且可伪造——不得用于公网或边缘面调用
// （edge 面调 AdminService 只会得到 Unimplemented，这是刻意的信任边界）；
// 将来接鉴权主体后，operator 必须改为由凭据派生而非调用方自报（见管理面规范文档）。
package gm

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/idgen"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 默认配置。
const (
	// defaultTimeout 是单次管理面 RPC 的超时（Dial 时同源用于拨号）。
	defaultTimeout = 10 * time.Second
	// keyPrefix 是缺省幂等键前缀（lib/idgen 生成 <prefix>-<32位hex> 形态）。
	keyPrefix = "gm"
)

// ErrStuckCursor 表示服务端返回的审计游标未推进（同一 page_token 反复返回），
// 迭代器据此中止翻页以避免死循环。
var ErrStuckCursor = errors.New("gm: 审计游标未推进，已中止翻页")

// options 是 Client 的运行配置（由 Option 组装，零值可用）。
type options struct {
	operator       string        // 调用方自报的操作者（必填）
	idempotencyKey string        // 显式幂等键；空 = 写操作按调用自动生成
	reason         string        // 工单/纠纷单号或说明（审计可读性）
	dryRun         bool          // 只校验不改档
	timeout        time.Duration // 单次 RPC 超时
	retries        int           // 瞬时失败重试次数（0 = 不重试）
}

// Option 是 Client 的函数式选项。
type Option func(*options)

// WithOperator 设置调用方自报的操作者；空白值在调用时被拦为 AdminOperatorMissing。
func WithOperator(operator string) Option {
	return func(o *options) { o.operator = strings.TrimSpace(operator) }
}

// WithIdempotencyKey 设置写操作的显式幂等键；缺省由 SDK 按调用生成，同一次调用内的重试复用同一个键。
func WithIdempotencyKey(key string) Option {
	return func(o *options) { o.idempotencyKey = strings.TrimSpace(key) }
}

// WithReason 设置审计可读的工单/纠纷单号或说明（写入 AdminContext.reason）。
func WithReason(reason string) Option {
	return func(o *options) { o.reason = reason }
}

// WithDryRun 设置 dry-run：服务端只做校验与存在性检查、不改档（仍写审计，且不占用幂等键）。
func WithDryRun(dryRun bool) Option {
	return func(o *options) { o.dryRun = dryRun }
}

// WithTimeout 设置单次 RPC 超时（Dial 时同源用于拨号）；缺省 10s，<=0 回落缺省。
func WithTimeout(timeout time.Duration) Option {
	return func(o *options) { o.timeout = timeout }
}

// WithRetry 设置瞬时失败（Unavailable/DeadlineExceeded）的重试次数，0（缺省）= 不重试。
// GrantItem 的每次尝试复用同一幂等键，故重试安全（服务端按唯一索引去重）。
func WithRetry(retries int) Option {
	return func(o *options) { o.retries = retries }
}

// Client 是管理面 GM 客户端：封装 AdminServiceClient 与统一信封、幂等键、超时和重试策略。
type Client struct {
	svc    admingamev1.AdminServiceClient
	opts   options
	closer io.Closer // 仅 Dial 构造时非 nil（关闭自建连接）
}

// New 用已有 gRPC 连接构造客户端（连接生命周期由调用方管理，Close 不关连接）。
func New(cc grpc.ClientConnInterface, opts ...Option) *Client {
	return NewWithClient(admingamev1.NewAdminServiceClient(cc), opts...)
}

// NewWithClient 用现成的管理面客户端构造（测试替身或自行挂拦截器的场景）。
func NewWithClient(svc admingamev1.AdminServiceClient, opts ...Option) *Client {
	o := newOptions(opts...)
	return &Client{svc: svc, opts: o}
}

// Dial 拨号到 game 管理面的 internal listener 并构造客户端（非加密，仅限内网使用）；
// 返回的客户端持有该连接，Close 时一并关闭。
func Dial(ctx context.Context, endpoint string, opts ...Option) (*Client, error) {
	o := newOptions(opts...)
	conn, err := atlasgrpc.DialInsecure(ctx,
		atlasgrpc.WithEndpoint(endpoint),
		atlasgrpc.WithTimeout(o.timeout))
	if err != nil {
		return nil, err
	}
	c := NewWithClient(admingamev1.NewAdminServiceClient(conn), opts...)
	c.closer = conn
	return c, nil
}

// Close 关闭客户端自建的连接（New/NewWithClient 构造的客户端无自建连接，返回 nil）。
func (c *Client) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer.Close()
}

// newOptions 组装选项并补齐缺省值。
func newOptions(opts ...Option) options {
	o := options{timeout: defaultTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	if o.timeout <= 0 {
		o.timeout = defaultTimeout
	}
	return o
}

// GrantItem 发放道具（写操作）：playerID 为目标玩家，itemID/count 为道具与数量（受单次上限约束）。
func (c *Client) GrantItem(ctx context.Context, playerID string, itemID, count uint32) (*admingamev1.GrantItemReply, error) {
	req := &admingamev1.GrantItemRequest{
		Context:  c.envelope(true),
		PlayerId: playerID,
		ItemId:   itemID,
		Count:    count,
	}
	return invoke(ctx, c, req, c.svc.GrantItem)
}

// QueryPlayer 查询玩家读模型（读操作，不带幂等键）。
func (c *Client) QueryPlayer(ctx context.Context, playerID string) (*admingamev1.QueryPlayerReply, error) {
	req := &admingamev1.QueryPlayerRequest{Context: c.envelope(false), PlayerId: playerID}
	return invoke(ctx, c, req, c.svc.QueryPlayer)
}

// QueryBackpack 查询玩家背包读模型（读操作，不带幂等键）。
func (c *Client) QueryBackpack(ctx context.Context, playerID string) (*admingamev1.QueryBackpackReply, error) {
	req := &admingamev1.QueryBackpackRequest{Context: c.envelope(false), PlayerId: playerID}
	return invoke(ctx, c, req, c.svc.QueryBackpack)
}

// QueryAudits 打开审计记录迭代器（读操作）：首页立即拉取（错误即时暴露），
// 其后按 next_page_token 懒翻页、末页（空 token）停止；迭代结束须 Close。
func (c *Client) QueryAudits(ctx context.Context, filter AuditFilter) (Iterator, error) {
	if err := c.checkOperator(); err != nil {
		return nil, err
	}
	it := &auditIterator{ctx: ctx, client: c, filter: filter}
	if err := it.fetch(); err != nil {
		return nil, err
	}
	return it, nil
}

// envelope 组装统一请求信封；写操作（write=true）携带幂等键：显式配置优先，缺省按调用生成；
// 读操作不带幂等键（管理面约定：读操作不要求幂等键）。
func (c *Client) envelope(write bool) *admingamev1.AdminContext {
	env := &admingamev1.AdminContext{
		Operator: c.opts.operator,
		Reason:   c.opts.reason,
		DryRun:   c.opts.dryRun,
	}
	if write {
		env.IdempotencyKey = c.idempotencyKey()
	}
	return env
}

// idempotencyKey 返回本次写操作的幂等键：显式配置优先，否则用 lib/idgen 生成新键。
func (c *Client) idempotencyKey() string {
	if c.opts.idempotencyKey != "" {
		return c.opts.idempotencyKey
	}
	return idgen.New(keyPrefix)
}

// checkOperator 在发出请求前拦截缺 operator 的调用：不依赖服务端拒绝，
// 也避免在管理面留下无法追责的审计记录。
func (c *Client) checkOperator() error {
	if c.opts.operator == "" {
		return errorv1.ErrAdminOperatorMissing("管理面调用缺少 operator（调用方须自报操作者）")
	}
	return nil
}

// invoke 是四个管理面一元调用的公共骨架：先做客户端侧校验，再按客户端配置重试瞬时失败；
// 请求对象在重试间复用，故写操作的幂等键不会随重试变化。
func invoke[Req any, Resp any](ctx context.Context, c *Client, req Req, call func(context.Context, Req, ...grpc.CallOption) (Resp, error)) (Resp, error) {
	var zero Resp
	if err := c.checkOperator(); err != nil {
		return zero, err
	}
	for n := 0; ; n++ {
		resp, err := callOnce(ctx, c.opts.timeout, req, call)
		if err == nil {
			return resp, nil
		}
		if n >= c.opts.retries || !retryable(err) || ctx.Err() != nil {
			return zero, err
		}
	}
}

// callOnce 以单次超时预算发起一次调用（重试的每次尝试各自计时，互不挤占）。
func callOnce[Req any, Resp any](ctx context.Context, timeout time.Duration, req Req, call func(context.Context, Req, ...grpc.CallOption) (Resp, error)) (Resp, error) {
	if timeout <= 0 {
		return call(ctx, req)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return call(ctx, req)
}

// retryable 报告错误是否为可重试的瞬时失败（连接不可用或超时）；业务错误一律不重试。
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// AuditFilter 是审计查询过滤条件（空字段表示不过滤；PageSize 0 = 服务端缺省 20、上限 200）。
type AuditFilter struct {
	TargetID string // 目标 ID（玩家等）
	Operator string // 操作者过滤
	PageSize uint32 // 每页条数
}

// Iterator 是审计记录的流式迭代器（形态与 locator.Iterator 一致：Next 以 io.EOF 表示结束）。
type Iterator interface {
	// Next 取下一个审计投影；迭代结束返回 io.EOF。
	Next() (*admingamev1.AuditEntry, error)
	// Close 结束迭代；调用后 Next 立即返回 io.EOF。
	Close() error
}

// auditIterator 是 Iterator 的游标分页实现：按 next_page_token 逐页拉取（空页继续翻，空 token 停止）。
type auditIterator struct {
	ctx    context.Context
	client *Client
	filter AuditFilter
	token  string // 下一页游标（"" = 首次请求或末页）
	buf    []*admingamev1.AuditEntry
	done   bool // 已收到空 token（末页）
	closed bool
}

// Next 返回下一条审计记录；缓冲耗尽时按游标继续翻页，无下一页则返回 io.EOF。
func (i *auditIterator) Next() (*admingamev1.AuditEntry, error) {
	for len(i.buf) == 0 {
		if i.closed || i.done {
			return nil, io.EOF
		}
		if err := i.fetch(); err != nil {
			return nil, err
		}
	}
	entry := i.buf[0]
	i.buf = i.buf[1:]
	return entry, nil
}

// Close 结束迭代：丢弃缓冲并让后续 Next 立即返回 io.EOF
// （gRPC 分页无服务端游标资源，故不发网络调用）。
func (i *auditIterator) Close() error {
	i.closed = true
	i.buf = nil
	return nil
}

// fetch 拉取下一页并推进游标：空 token 标记末页；同一 token 反复返回视为服务端违约并中止翻页。
func (i *auditIterator) fetch() error {
	req := &admingamev1.QueryAuditsRequest{
		Context:        i.client.envelope(false),
		TargetId:       i.filter.TargetID,
		OperatorFilter: i.filter.Operator,
		PageSize:       i.filter.PageSize,
		PageToken:      i.token,
	}
	resp, err := invoke(i.ctx, i.client, req, i.client.svc.QueryAudits)
	if err != nil {
		return err
	}
	token := resp.GetNextPageToken()
	if token != "" && token == i.token {
		return ErrStuckCursor
	}
	i.buf, i.token = resp.GetEntries(), token
	i.done = token == ""
	return nil
}
