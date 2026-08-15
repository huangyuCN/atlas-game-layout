package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// memRepo 是玩家仓储的内存实现（缓存 + 持久化两级模拟）。
type memRepo struct {
	players   map[string]*models.Player // 持久化（mongo 模拟）
	snapshots map[string]*models.Player // 快照（redis 模拟）
}

func newMemRepo() *memRepo {
	return &memRepo{players: make(map[string]*models.Player), snapshots: make(map[string]*models.Player)}
}

func (r *memRepo) LoadPlayer(_ context.Context, playerID string) (*models.Player, error) {
	if p, ok := r.snapshots[playerID]; ok {
		return clonePlayer(p), nil
	}
	if p, ok := r.players[playerID]; ok {
		return clonePlayer(p), nil
	}
	return nil, repo.ErrPlayerNotFound
}

func (r *memRepo) FindByAccount(_ context.Context, account string) (*models.Player, error) {
	for _, p := range r.players {
		if p.Account == account {
			return clonePlayer(p), nil
		}
	}
	return nil, repo.ErrPlayerNotFound
}

func (r *memRepo) CreatePlayer(_ context.Context, p *models.Player) error {
	r.players[p.PlayerID] = clonePlayer(p)
	return nil
}

func (r *memRepo) SaveSnapshot(_ context.Context, p *models.Player, _ time.Duration) error {
	r.snapshots[p.PlayerID] = clonePlayer(p)
	return nil
}

func (r *memRepo) SavePlayer(_ context.Context, p *models.Player) error {
	r.players[p.PlayerID] = clonePlayer(p)
	return nil
}

func clonePlayer(p *models.Player) *models.Player {
	out := *p
	out.Items = append([]*models.Item(nil), p.Items...)
	return &out
}

// memSessions 是会话令牌的内存实现。
type memSessions struct{ tokens map[string]string }

func newMemSessions() *memSessions { return &memSessions{tokens: make(map[string]string)} }

func (s *memSessions) Get(_ context.Context, playerID string) (string, error) {
	return s.tokens[playerID], nil
}
func (s *memSessions) Set(_ context.Context, playerID, token string, _ time.Duration) error {
	s.tokens[playerID] = token
	return nil
}
func (s *memSessions) Del(_ context.Context, playerID string) error {
	delete(s.tokens, playerID)
	return nil
}

// newTestHandler 构造测试用 PlayerHandler（固定 ID 生成）。
func newTestHandler() (*PlayerHandler, *memRepo, *memSessions) {
	store := newMemRepo()
	sessions := newMemSessions()
	h := NewPlayerHandler(store, sessions, biz.PlayerServiceOptions{
		SessionTTL:  time.Minute,
		NewPlayerID: func() string { return "p-test" },
	})
	return h, store, sessions
}

func reasonOf(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		t.Fatal("期望业务错误，实际为 nil")
	}
	se := atlaserrors.FromError(err)
	if se == nil {
		t.Fatalf("非结构化错误: %v", err)
	}
	return se.Reason
}

// TestRegister 验证注册成功与重复注册。
func TestRegister(t *testing.T) {
	ctx := context.Background()
	h, _, _ := newTestHandler()

	reply, err := h.Register(ctx, &gamev1.RegisterActorReq{Account: "alice", Password: "pw", Nickname: "爱丽丝"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if !reply.GetOk() || reply.GetPlayerId() != "p-test" {
		t.Fatalf("注册回执不符: %+v", reply)
	}
	if _, err := h.Register(ctx, &gamev1.RegisterActorReq{Account: "alice", Password: "pw"}); reasonOf(t, err) != errorv1.ReasonPlayerAlreadyExists() {
		t.Fatalf("重复注册错误 = %v", err)
	}
	if _, err := h.Register(ctx, &gamev1.RegisterActorReq{}); reasonOf(t, err) != errorv1.ReasonInvalidParams() {
		t.Fatalf("空参数错误 = %v", err)
	}
}

// TestLogin 验证登录成功/未注册/口令错误与会话令牌裁决。
func TestLogin(t *testing.T) {
	ctx := context.Background()
	h, _, sessions := newTestHandler()
	if _, err := h.Register(ctx, &gamev1.RegisterActorReq{Account: "alice", Password: "pw", Nickname: "爱丽丝"}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := h.Login(ctx, &gamev1.LoginActorReq{PlayerId: "p-none", Password: "pw", Token: "t1"}); reasonOf(t, err) != errorv1.ReasonPlayerNotFound() {
		t.Fatalf("未注册登录错误 = %v", err)
	}
	if _, err := h.Login(ctx, &gamev1.LoginActorReq{PlayerId: "p-test", Password: "bad", Token: "t1"}); reasonOf(t, err) != errorv1.ReasonPasswordWrong() {
		t.Fatalf("口令错误 = %v", err)
	}
	reply, err := h.Login(ctx, &gamev1.LoginActorReq{PlayerId: "p-test", Password: "pw", Token: "t1"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if !reply.GetOk() || reply.GetPlayer().GetPlayerId() != "p-test" || reply.GetPlayer().GetNickname() != "爱丽丝" {
		t.Fatalf("登录回执不符: %+v", reply)
	}
	if got, _ := sessions.Get(ctx, "p-test"); got != "t1" {
		t.Fatalf("会话令牌 = %q, want t1", got)
	}
	if _, err := h.Login(ctx, &gamev1.LoginActorReq{PlayerId: "p-test", Password: "pw", Token: "t2"}); err != nil {
		t.Fatalf("二次登录: %v", err)
	}
	if got, _ := sessions.Get(ctx, "p-test"); got != "t2" {
		t.Fatalf("裁决后会话令牌 = %q, want t2", got)
	}
}

// TestLoginLoadsFromCache 验证登录优先读快照缓存（三级链路）。
func TestLoginLoadsFromCache(t *testing.T) {
	ctx := context.Background()
	h, store, _ := newTestHandler()
	if _, err := h.Register(ctx, &gamev1.RegisterActorReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// 写入快照缓存，并让持久化层内容不一致（模拟缓存优先）。
	p, _ := store.LoadPlayer(ctx, "p-test")
	p.Nickname = "缓存昵称"
	if err := store.SaveSnapshot(ctx, p, time.Minute); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	reply, err := h.Login(ctx, &gamev1.LoginActorReq{PlayerId: "p-test", Password: "pw", Token: "t1"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if reply.GetPlayer().GetNickname() != "缓存昵称" {
		t.Fatalf("应命中缓存昵称: %+v", reply.GetPlayer())
	}
}

// TestLogout 验证登出清理与令牌不匹配保护。
func TestLogout(t *testing.T) {
	ctx := context.Background()
	h, _, sessions := newTestHandler()
	if _, err := h.Register(ctx, &gamev1.RegisterActorReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := h.Login(ctx, &gamev1.LoginActorReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if err := h.Logout(ctx, "p-test", "stale"); err != nil {
		t.Fatalf("Logout(旧令牌): %v", err)
	}
	if got, _ := sessions.Get(ctx, "p-test"); got != "t1" {
		t.Fatalf("令牌不匹配登出不应清理: %q", got)
	}
	if err := h.Logout(ctx, "p-test", "t1"); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	if got, _ := sessions.Get(ctx, "p-test"); got != "" {
		t.Fatalf("登出后会话 = %q, want 空", got)
	}
}

var _ = errors.Is
