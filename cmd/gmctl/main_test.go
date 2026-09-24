package main

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	"google.golang.org/grpc"
)

// fakeAdminServer 是 gmctl 用例的最小管理面服务端（真实 gRPC，监听本机随机端口）：
// 记录收到的请求，并返回可断言的读模型与两页审计。
type fakeAdminServer struct {
	admingamev1.UnimplementedAdminServiceServer
	mu      sync.Mutex
	grants  []*admingamev1.GrantItemRequest
	audits  []*admingamev1.QueryAuditsRequest
	players []*admingamev1.QueryPlayerRequest
}

// GrantItem 记录发放请求并返回已应用回执。
func (s *fakeAdminServer) GrantItem(_ context.Context, req *admingamev1.GrantItemRequest) (*admingamev1.GrantItemReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grants = append(s.grants, req)
	return &admingamev1.GrantItemReply{AuditId: "a-1", Applied: true}, nil
}

// QueryPlayer 记录查询请求并回显玩家与操作者。
func (s *fakeAdminServer) QueryPlayer(_ context.Context, req *admingamev1.QueryPlayerRequest) (*admingamev1.QueryPlayerReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.players = append(s.players, req)
	return &admingamev1.QueryPlayerReply{PlayerId: req.GetPlayerId(), Nickname: req.GetContext().GetOperator()}, nil
}

// QueryBackpack 返回一件固定道具（背包读模型）。
func (s *fakeAdminServer) QueryBackpack(_ context.Context, req *admingamev1.QueryBackpackRequest) (*admingamev1.QueryBackpackReply, error) {
	return &admingamev1.QueryBackpackReply{
		PlayerId: req.GetPlayerId(),
		Items:    []*admingamev1.BackpackItem{{ItemId: 1001, Count: 5}},
	}, nil
}

// QueryAudits 返回两页审计：首页带游标 t1，第二页空游标（末页）。
func (s *fakeAdminServer) QueryAudits(_ context.Context, req *admingamev1.QueryAuditsRequest) (*admingamev1.QueryAuditsReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audits = append(s.audits, req)
	if req.GetPageToken() == "" {
		return &admingamev1.QueryAuditsReply{
			Entries:       []*admingamev1.AuditEntry{{AuditId: "a-1", Result: admingamev1.AuditResult_AUDIT_RESULT_SUCCESS}},
			NextPageToken: "t1",
		}, nil
	}
	return &admingamev1.QueryAuditsReply{
		Entries: []*admingamev1.AuditEntry{{
			AuditId: "a-2", Result: admingamev1.AuditResult_AUDIT_RESULT_FAILED, Reason: "PLAYER_NOT_FOUND",
		}},
	}, nil
}

// newFakeAdmin 起一个本机随机端口的管理面服务端，返回地址与替身（测试结束自动停服）。
func newFakeAdmin(t *testing.T) (string, *fakeAdminServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听本机端口失败: %v", err)
	}
	fake := &fakeAdminServer{}
	srv := grpc.NewServer()
	admingamev1.RegisterAdminServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), fake
}

// TestSubcommandsEndToEnd 让四个子命令打真实 gRPC（本机随机端口），
// 验证参数解析、信封透传、幂等键生成、审计游标翻页与 JSON 输出路径。
func TestSubcommandsEndToEnd(t *testing.T) {
	addr, fake := newFakeAdmin(t)
	base := []string{"-addr", addr, "-operator", "gm-smoke", "-timeout", "3s"}
	cases := [][]string{
		{"query-player", "-player", "p-1"},
		{"query-backpack", "-player", "p-1"},
		{"query-audits", "-player", "p-1", "-page-size", "1", "-max", "10"},
		{"grant-item", "-player", "p-1", "-item", "1001", "-count", "5"},
	}
	for _, args := range cases {
		global := append(append([]string{"-reason", "烟测工单"}, base...), args...)
		if err := run(context.Background(), global); err != nil {
			t.Fatalf("子命令 %v 失败: %v", args, err)
		}
	}
	if len(fake.players) != 1 || fake.players[0].GetPlayerId() != "p-1" {
		t.Fatalf("玩家查询请求不符合预期: %+v", fake.players)
	}
	if len(fake.grants) != 1 {
		t.Fatalf("应发放 1 次，实际 %d 次", len(fake.grants))
	}
	env := fake.grants[0].GetContext()
	if env.GetOperator() != "gm-smoke" || env.GetReason() != "烟测工单" {
		t.Fatalf("全局选项未进信封: %+v", env)
	}
	if env.GetIdempotencyKey() == "" {
		t.Fatal("写操作的幂等键应自动生成")
	}
	if len(fake.audits) != 2 || fake.audits[1].GetPageToken() != "t1" {
		t.Fatalf("审计应按游标翻两页（第二页带 t1），实际 %+v", fake.audits)
	}
}

// TestHelpExitsSuccessfully 验证 -h 走成功路径（帮助可见、不校验 operator、不拨号）。
func TestHelpExitsSuccessfully(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"grant-item", "-h"}, {"query-audits", "-h"}} {
		if err := run(context.Background(), args); err != nil {
			t.Fatalf("gmctl %v 应按成功退出，实际: %v", args, err)
		}
	}
}

// TestMissingOperatorFails 验证缺 operator 时报错提示（不静默用空操作者发请求）。
func TestMissingOperatorFails(t *testing.T) {
	t.Setenv(envOperator, "")
	err := run(context.Background(), []string{"query-player", "-player", "p-1"})
	if err == nil || !strings.Contains(err.Error(), "缺少操作者") {
		t.Fatalf("缺 operator 应报错提示，实际: %v", err)
	}
}
