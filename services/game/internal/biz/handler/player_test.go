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
// （会话裁决单点收敛到 Gateway）。biz.Login 只做口令校验：数据选源
// （redis/mongo 版本比较）与回执摘要由 actor 单点完成。
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
	// 成功登录：回执摘要为空（biz 不做选源，摘要由 actor 从选源后的聚合根组装）。
	reply, err := h.Login(ctx, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if reply.GetPlayer() != nil {
		t.Fatalf("biz 登录回执不应携带摘要（actor 单点组装）: %+v", reply.GetPlayer())
	}
}

// TestLoginDoesNotSourceData 验证口令校验不触发数据选源（LoadCredential 专用）：
// 登录成功不读快照缓存、不发生对齐补写——选源单点归 actor（一次登录一份读取与补写）。
func TestLoginDoesNotSourceData(t *testing.T) {
	ctx := context.Background()
	h, store := newTestHandler()
	if _, err := h.Register(ctx, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// 缓存中是另一个昵称（若 biz 做选源会读到它并触发对齐补写）。
	p, _ := store.LoadPlayer(ctx, "p-test")
	p.Nickname = "缓存昵称"
	store.snapshots[p.PlayerID] = clonePlayer(p)
	if _, err := h.Login(ctx, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	// 对齐补写未发生：mongo 内容保持注册基线（Nickname 仍是账号）。
	mp, err := store.LoadCredential(ctx, "p-test")
	if err != nil {
		t.Fatal(err)
	}
	if mp.Nickname == "缓存昵称" {
		t.Fatal("口令校验不应触发对齐补写（选源单点归 actor）")
	}
}
