package handler

import (
	"context"
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

// LoadCredential mongo 专用读（含认证字段；登录口令校验用）。
func (r *memRepo) LoadCredential(_ context.Context, playerID string) (*models.Player, error) {
	if p, ok := r.players[playerID]; ok {
		return clonePlayer(p), nil
	}
	return nil, repo.ErrPlayerNotFound
}

// FlushPlayer 统一落盘（测试桩：快照 + 持久都成功）。
func (r *memRepo) FlushPlayer(_ context.Context, p *models.Player, lastCRC uint32) (repo.FlushResult, uint32, error) {
	r.snapshots[p.PlayerID] = clonePlayer(p)
	r.players[p.PlayerID] = clonePlayer(p)
	return repo.FlushRedisOK, lastCRC + 1, nil
}

// PersistSnapshot 玩家快照 PERSIST（测试桩：no-op）。
func (r *memRepo) PersistSnapshot(_ context.Context, playerID string) error {
	return nil
}

// TTLOfSnapshot 快照 TTL 查询（测试桩：有档固定 72h）。
func (r *memRepo) TTLOfSnapshot(_ context.Context, playerID string) (time.Duration, error) {
	if _, ok := r.snapshots[playerID]; !ok {
		return 0, repo.ErrPlayerNotFound
	}
	return 72 * time.Hour, nil
}

// ExpireSnapshot 恢复快照 TTL（测试桩：no-op）。
func (r *memRepo) ExpireSnapshot(_ context.Context, playerID string, ttl time.Duration) error {
	return nil
}

// SavePlayer mongo 落库（测试桩：无条件 Upsert）。
func (r *memRepo) SavePlayer(_ context.Context, p *models.Player) error {
	r.players[p.PlayerID] = clonePlayer(p)
	return nil
}

func clonePlayer(p *models.Player) *models.Player {
	out := *p
	out.Items = append([]*models.Item(nil), p.Items...)
	return &out
}

// newTestHandler 构造测试用 PlayerHandler（固定 ID 生成）。
func newTestHandler() (*PlayerHandler, *memRepo) {
	store := newMemRepo()
	h := NewPlayerHandler(store, biz.PlayerServiceOptions{
		NewPlayerID: func() string { return "p-test" },
	})
	return h, store
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
	h, _ := newTestHandler()

	reply, err := h.Register(ctx, &gamev1.RegisterReq{Account: "alice", Password: "pw", Nickname: "爱丽丝"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if reply.GetPlayerId() != "p-test" {
		t.Fatalf("注册回执不符: %+v", reply)
	}
	if _, err := h.Register(ctx, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); reasonOf(t, err) != errorv1.ReasonPlayerAlreadyExists() {
		t.Fatalf("重复注册错误 = %v", err)
	}
	if _, err := h.Register(ctx, &gamev1.RegisterReq{}); reasonOf(t, err) != errorv1.ReasonInvalidParams() {
		t.Fatalf("空参数错误 = %v", err)
	}
}

// TestLogin 验证登录成功/未注册/口令错误；game 侧不持久化会话令牌
// （会话裁决单点收敛到 Gateway，登录回执仅玩家摘要）。
func TestLogin(t *testing.T) {
	ctx := context.Background()
	h, _ := newTestHandler()
	if _, err := h.Register(ctx, &gamev1.RegisterReq{Account: "alice", Password: "pw", Nickname: "爱丽丝"}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := h.Login(ctx, &gamev1.LoginReq{PlayerId: "p-none", Password: "pw", Token: "t1"}); reasonOf(t, err) != errorv1.ReasonPlayerNotFound() {
		t.Fatalf("未注册登录错误 = %v", err)
	}
	if _, err := h.Login(ctx, &gamev1.LoginReq{PlayerId: "p-test", Password: "bad", Token: "t1"}); reasonOf(t, err) != errorv1.ReasonPasswordWrong() {
		t.Fatalf("口令错误 = %v", err)
	}
	reply, err := h.Login(ctx, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if reply.GetPlayer().GetPlayerId() != "p-test" || reply.GetPlayer().GetNickname() != "爱丽丝" {
		t.Fatalf("登录回执不符: %+v", reply)
	}
}

// TestLoginLoadsFromCache 验证登录优先读快照缓存（三级链路）。
func TestLoginLoadsFromCache(t *testing.T) {
	ctx := context.Background()
	h, store := newTestHandler()
	if _, err := h.Register(ctx, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// 写入快照缓存，并让持久化层内容不一致（模拟缓存优先：同版本用 Redis）。
	p, _ := store.LoadPlayer(ctx, "p-test")
	p.Nickname = "缓存昵称"
	store.snapshots[p.PlayerID] = clonePlayer(p)
	reply, err := h.Login(ctx, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if reply.GetPlayer().GetNickname() != "缓存昵称" {
		t.Fatalf("应命中缓存昵称: %+v", reply.GetPlayer())
	}
}
