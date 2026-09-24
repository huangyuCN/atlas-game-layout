package gm

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeAdmin 是 AdminServiceClient 的测试替身：记录收到的请求，
// 并按脚本依次返回错误/页面（脚本用尽后返回空回执）。
type fakeAdmin struct {
	mu       sync.Mutex
	grantReq []*admingamev1.GrantItemRequest
	grantErr []error
	grantRes *admingamev1.GrantItemReply
	pageReq  []*admingamev1.QueryAuditsRequest
	pages    []*admingamev1.QueryAuditsReply
	plyReq   []*admingamev1.QueryPlayerRequest
	plyRes   *admingamev1.QueryPlayerReply
	bagReq   []*admingamev1.QueryBackpackRequest
	calls    int
}

// GrantItem 记录发道具请求并返回脚本响应。
func (f *fakeAdmin) GrantItem(_ context.Context, in *admingamev1.GrantItemRequest, _ ...grpc.CallOption) (*admingamev1.GrantItemReply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.grantReq = append(f.grantReq, in)
	if n := len(f.grantErr); n > 0 {
		err := f.grantErr[0]
		f.grantErr = f.grantErr[1:]
		return nil, err
	}
	if f.grantRes != nil {
		return f.grantRes, nil
	}
	return &admingamev1.GrantItemReply{}, nil
}

// QueryPlayer 记录查玩家请求并返回脚本响应。
func (f *fakeAdmin) QueryPlayer(_ context.Context, in *admingamev1.QueryPlayerRequest, _ ...grpc.CallOption) (*admingamev1.QueryPlayerReply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.plyReq = append(f.plyReq, in)
	if f.plyRes != nil {
		return f.plyRes, nil
	}
	return &admingamev1.QueryPlayerReply{}, nil
}

// QueryBackpack 记录查背包请求并返回空背包。
func (f *fakeAdmin) QueryBackpack(_ context.Context, in *admingamev1.QueryBackpackRequest, _ ...grpc.CallOption) (*admingamev1.QueryBackpackReply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.bagReq = append(f.bagReq, in)
	return &admingamev1.QueryBackpackReply{}, nil
}

// QueryAudits 记录审计查询请求并依次返回预置页面（末页为空 token）。
func (f *fakeAdmin) QueryAudits(_ context.Context, in *admingamev1.QueryAuditsRequest, _ ...grpc.CallOption) (*admingamev1.QueryAuditsReply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.pageReq = append(f.pageReq, in)
	if len(f.pages) == 0 {
		return &admingamev1.QueryAuditsReply{}, nil
	}
	page := f.pages[0]
	f.pages = f.pages[1:]
	return page, nil
}

// snapshot 返回调用计数（断言「客户端侧拦截 = 零请求」）。
func (f *fakeAdmin) snapshot() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestRequiresOperator 验证缺 operator 在客户端侧即被拦截（请求不发出）。
func TestRequiresOperator(t *testing.T) {
	fake := &fakeAdmin{}
	ctx := context.Background()
	if _, err := NewWithClient(fake).GrantItem(ctx, "p-1", 1001, 1); !errorv1.IsAdminOperatorMissing(err) {
		t.Fatalf("缺 operator 应返回 AdminOperatorMissing，实际: %v", err)
	}
	// 纯空白 operator 同样视为缺失。
	blank := NewWithClient(fake, WithOperator("   "))
	if _, err := blank.QueryPlayer(ctx, "p-1"); !errorv1.IsAdminOperatorMissing(err) {
		t.Fatalf("空白 operator 应返回 AdminOperatorMissing，实际: %v", err)
	}
	if _, err := blank.QueryBackpack(ctx, "p-1"); !errorv1.IsAdminOperatorMissing(err) {
		t.Fatalf("查背包应同样被拦截，实际: %v", err)
	}
	if _, err := blank.QueryAudits(ctx, AuditFilter{}); !errorv1.IsAdminOperatorMissing(err) {
		t.Fatalf("审计迭代应在打开时即被拦截，实际: %v", err)
	}
	if n := fake.snapshot(); n != 0 {
		t.Fatalf("客户端侧拦截不应发出任何请求，实际 %d 次", n)
	}
}

// TestGrantItemAutoKeyAndRetry 验证幂等键缺省自动生成，且同一次调用内重试复用同一个键。
func TestGrantItemAutoKeyAndRetry(t *testing.T) {
	fake := &fakeAdmin{
		grantErr: []error{status.Error(codes.Unavailable, "连接抖动")},
		grantRes: &admingamev1.GrantItemReply{AuditId: "a-1", Applied: true},
	}
	c := NewWithClient(fake, WithOperator(" gm-alice "), WithRetry(2))
	reply, err := c.GrantItem(context.Background(), "p-1", 1001, 5)
	if err != nil {
		t.Fatalf("重试后应成功，实际: %v", err)
	}
	if reply.GetAuditId() != "a-1" || !reply.GetApplied() {
		t.Fatalf("回执未原样透传: %+v", reply)
	}
	if len(fake.grantReq) != 2 {
		t.Fatalf("应重试一次（共 2 次请求），实际 %d 次", len(fake.grantReq))
	}
	first := fake.grantReq[0].GetContext().GetIdempotencyKey()
	if !strings.HasPrefix(first, "gm-") {
		t.Fatalf("缺省幂等键应为 gm- 前缀，实际 %q", first)
	}
	if second := fake.grantReq[1].GetContext().GetIdempotencyKey(); first != second {
		t.Fatalf("重试必须复用同一幂等键，实际 %q → %q", first, second)
	}
	if op := fake.grantReq[0].GetContext().GetOperator(); op != "gm-alice" {
		t.Fatalf("operator 应去除首尾空白后透传，实际 %q", op)
	}
}

// TestGrantItemExplicitKeyAndRetryLimit 验证显式幂等键原样透传，重试用尽返回最后一次错误。
func TestGrantItemExplicitKeyAndRetryLimit(t *testing.T) {
	fake := &fakeAdmin{grantErr: []error{
		status.Error(codes.Unavailable, "抖动1"),
		status.Error(codes.Unavailable, "抖动2"),
		status.Error(codes.Unavailable, "抖动3"),
	}}
	c := NewWithClient(fake, WithOperator("gm-bob"), WithIdempotencyKey("key-42"), WithRetry(1))
	if _, err := c.GrantItem(context.Background(), "p-9", 2002, 1); status.Code(err) != codes.Unavailable {
		t.Fatalf("重试用尽应返回最后一次 Unavailable，实际: %v", err)
	}
	if len(fake.grantReq) != 2 {
		t.Fatalf("WithRetry(1) 应为 1 次初始 + 1 次重试，实际 %d 次", len(fake.grantReq))
	}
	for i, req := range fake.grantReq {
		if key := req.GetContext().GetIdempotencyKey(); key != "key-42" {
			t.Fatalf("第 %d 次请求应携带显式幂等键，实际 %q", i+1, key)
		}
	}
}

// TestGrantItemBusinessErrorNotRetried 验证业务错误不触发重试（非瞬时错误重试只会放大副作用）。
func TestGrantItemBusinessErrorNotRetried(t *testing.T) {
	fake := &fakeAdmin{grantErr: []error{errorv1.ErrPlayerNotFound("玩家不存在")}}
	c := NewWithClient(fake, WithOperator("gm-bob"), WithRetry(3))
	if _, err := c.GrantItem(context.Background(), "p-404", 1001, 1); !errorv1.IsPlayerNotFound(err) {
		t.Fatalf("应原样返回业务错误，实际: %v", err)
	}
	if len(fake.grantReq) != 1 {
		t.Fatalf("业务错误不应重试，实际 %d 次请求", len(fake.grantReq))
	}
}

// TestEnvelopePassthrough 验证 dry-run/reason 透传，且读操作不带幂等键（proto 约定）。
func TestEnvelopePassthrough(t *testing.T) {
	fake := &fakeAdmin{grantRes: &admingamev1.GrantItemReply{
		AuditId:    "a-2",
		Violations: []string{"count 超上限"},
	}}
	c := NewWithClient(fake, WithOperator("gm-alice"), WithDryRun(true), WithReason("工单-9527"))
	reply, err := c.GrantItem(context.Background(), "p-1", 1001, 999)
	if err != nil {
		t.Fatalf("dry-run 调用失败: %v", err)
	}
	if got := reply.GetViolations(); len(got) != 1 || got[0] != "count 超上限" {
		t.Fatalf("dry-run 违规项未透传: %v", got)
	}
	env := fake.grantReq[0].GetContext()
	if !env.GetDryRun() || env.GetReason() != "工单-9527" || env.GetIdempotencyKey() == "" {
		t.Fatalf("dry-run 信封不符合预期: %+v", env)
	}
	if _, err := c.QueryPlayer(context.Background(), "p-1"); err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	readEnv := fake.plyReq[0].GetContext()
	if readEnv.GetIdempotencyKey() != "" {
		t.Fatalf("读操作不应携带幂等键，实际 %q", readEnv.GetIdempotencyKey())
	}
	if !readEnv.GetDryRun() {
		t.Fatalf("dry-run 选项应同样作用于读操作信封: %+v", readEnv)
	}
}

// TestAuditsPagination 验证游标翻页：多页 + 中间空页（游标未空则继续）+ 空 token 停止。
func TestAuditsPagination(t *testing.T) {
	fake := &fakeAdmin{pages: []*admingamev1.QueryAuditsReply{
		{Entries: []*admingamev1.AuditEntry{{AuditId: "a-1"}, {AuditId: "a-2"}}, NextPageToken: "t1"},
		{NextPageToken: "t2"}, // 空页但游标未结束：不得提前停止
		{Entries: []*admingamev1.AuditEntry{{AuditId: "a-3"}}},
	}}
	c := NewWithClient(fake, WithOperator("gm-alice"))
	it, err := c.QueryAudits(context.Background(), AuditFilter{TargetID: "p-1", Operator: "gm-bob", PageSize: 2})
	if err != nil {
		t.Fatalf("打开审计迭代器失败: %v", err)
	}
	defer func() { _ = it.Close() }()
	var ids []string
	for {
		entry, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("翻页失败: %v", err)
		}
		ids = append(ids, entry.GetAuditId())
	}
	if got := strings.Join(ids, ","); got != "a-1,a-2,a-3" {
		t.Fatalf("迭代内容/顺序不符合预期: %q", got)
	}
	if _, err := it.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("末页之后应持续返回 io.EOF，实际: %v", err)
	}
	if n := len(fake.pageReq); n != 3 {
		t.Fatalf("应请求 3 页，实际 %d 页", n)
	}
	for i, want := range []string{"", "t1", "t2"} {
		if got := fake.pageReq[i].GetPageToken(); got != want {
			t.Fatalf("第 %d 次请求游标应为 %q，实际 %q", i+1, want, got)
		}
	}
	first := fake.pageReq[0]
	if first.GetTargetId() != "p-1" || first.GetOperatorFilter() != "gm-bob" || first.GetPageSize() != 2 {
		t.Fatalf("过滤条件未透传: %+v", first)
	}
}

// TestAuditsEmptyPageStops 验证首页即空且无下一页时立即结束（只发一次请求）。
func TestAuditsEmptyPageStops(t *testing.T) {
	fake := &fakeAdmin{pages: []*admingamev1.QueryAuditsReply{{}}}
	it, err := NewWithClient(fake, WithOperator("gm-alice")).QueryAudits(context.Background(), AuditFilter{})
	if err != nil {
		t.Fatalf("打开审计迭代器失败: %v", err)
	}
	if _, err := it.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("空首页应立即结束，实际: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close 不应报错: %v", err)
	}
	if _, err := it.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("Close 后应返回 io.EOF，实际: %v", err)
	}
	if n := len(fake.pageReq); n != 1 {
		t.Fatalf("应只请求 1 页，实际 %d 页", n)
	}
}

// TestAuditsStuckCursorFails 验证服务端返回未推进的游标时中止翻页，避免死循环。
func TestAuditsStuckCursorFails(t *testing.T) {
	fake := &fakeAdmin{pages: []*admingamev1.QueryAuditsReply{
		{NextPageToken: "same"},
		{NextPageToken: "same"},
	}}
	it, err := NewWithClient(fake, WithOperator("gm-alice")).QueryAudits(context.Background(), AuditFilter{})
	if err != nil {
		t.Fatalf("打开审计迭代器失败: %v", err)
	}
	defer func() { _ = it.Close() }()
	if _, err := it.Next(); !errors.Is(err, ErrStuckCursor) {
		t.Fatalf("游标未推进应返回 ErrStuckCursor，实际: %v", err)
	}
}

// TestAuditsFirstPageError 验证首页拉取失败在打开迭代器时即返回错误（错误不被吞掉）。
func TestAuditsFirstPageError(t *testing.T) {
	fake := &failingAudits{}
	it, err := NewWithClient(fake, WithOperator("gm-alice")).QueryAudits(context.Background(), AuditFilter{})
	if it != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("首页失败应返回 (nil, err)，实际: it=%v err=%v", it, err)
	}
}

// failingAudits 是审计查询固定失败的最小替身（其余方法不参与用例）。
type failingAudits struct {
	admingamev1.AdminServiceClient
}

// QueryAudits 固定返回 PermissionDenied（模拟管理面在 edge 面不可达/被拒）。
func (f *failingAudits) QueryAudits(context.Context, *admingamev1.QueryAuditsRequest, ...grpc.CallOption) (*admingamev1.QueryAuditsReply, error) {
	return nil, status.Error(codes.PermissionDenied, "no admin on edge")
}

// echoServer 是最小管理面服务端：把请求里的 operator 回显到昵称。
type echoServer struct {
	admingamev1.UnimplementedAdminServiceServer
}

// QueryPlayer 回显 player_id 与 operator（证明请求经真实 gRPC 管线抵达）。
func (s *echoServer) QueryPlayer(_ context.Context, req *admingamev1.QueryPlayerRequest) (*admingamev1.QueryPlayerReply, error) {
	return &admingamev1.QueryPlayerReply{PlayerId: req.GetPlayerId(), Nickname: req.GetContext().GetOperator()}, nil
}

// TestNewWrapsConn 用 bufconn 内存传输验证 New 正确包装 grpc.ClientConnInterface。
func TestNewWrapsConn(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	admingamev1.RegisterAdminServiceServer(srv, &echoServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("构造 bufconn 连接失败: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	reply, err := New(conn, WithOperator("gm-carol")).QueryPlayer(context.Background(), "p-7")
	if err != nil {
		t.Fatalf("经 bufconn 查询失败: %v", err)
	}
	if reply.GetPlayerId() != "p-7" || reply.GetNickname() != "gm-carol" {
		t.Fatalf("回执不符合预期: %+v", reply)
	}
}
