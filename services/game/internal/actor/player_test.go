package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// memRepo 是玩家仓储的内存实现（快照 + 持久化两级，记录落库次数）。
// 并发安全：actor 定时快照与测试轮询并发访问。
type memRepo struct {
	mu        sync.Mutex
	players   map[string]*models.Player
	snapshots map[string]*models.Player
	saves     int
}

func newMemRepo() *memRepo {
	return &memRepo{players: make(map[string]*models.Player), snapshots: make(map[string]*models.Player)}
}

func (r *memRepo) LoadPlayer(_ context.Context, playerID string) (*models.Player, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.snapshots[playerID]; ok {
		return clonePlayer(p), nil
	}
	if p, ok := r.players[playerID]; ok {
		return clonePlayer(p), nil
	}
	return nil, repo.ErrPlayerNotFound
}

func (r *memRepo) FindByAccount(_ context.Context, account string) (*models.Player, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.players {
		if p.Account == account {
			return clonePlayer(p), nil
		}
	}
	return nil, repo.ErrPlayerNotFound
}

func (r *memRepo) CreatePlayer(_ context.Context, p *models.Player) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.players[p.PlayerID] = clonePlayer(p)
	return nil
}

func (r *memRepo) SaveSnapshot(_ context.Context, p *models.Player, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshots[p.PlayerID] = clonePlayer(p)
	return nil
}

func (r *memRepo) SavePlayer(_ context.Context, p *models.Player) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.players[p.PlayerID] = clonePlayer(p)
	r.saves++
	return nil
}

// Saves 返回落库次数（测试断言用，并发安全）。
func (r *memRepo) Saves() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.saves
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

// newActorEnv 构造本地 actor 运行时（注册 PlayerActor，注册/登录 PID 分离）。
func newActorEnv(t *testing.T, snapTTL, snapTick time.Duration) (*core.LocalRuntime, types.PID, types.PID, *memRepo) {
	t.Helper()
	rt, err := core.NewLocalRuntime()
	if err != nil {
		t.Fatalf("NewLocalRuntime: %v", err)
	}
	// 启动运行时：时间轮（Repeat 定时快照）依赖 Start 拉起。
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Runtime Start: %v", err)
	}
	store := newMemRepo()
	svc := handler.NewPlayerHandler(store, newMemSessions(), biz.PlayerServiceOptions{
		SessionTTL:  time.Minute,
		NewPlayerID: func() string { return "p-test" },
	})
	if err := rt.Register(NewProps(svc, store, snapTTL, snapTick)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	regPID, err := types.NewPID("player", "alice")
	if err != nil {
		t.Fatalf("NewPID(reg): %v", err)
	}
	loginPID, err := types.NewPID("player", "p-test")
	if err != nil {
		t.Fatalf("NewPID(login): %v", err)
	}
	return rt, regPID, loginPID, store
}

// TestPlayerActorRegisterLogin 验证注册（自停）/登录（聚合根加载）/口令错误
// （同节点对象直传形态：生成的桩 switch 直接命中具体消息）。
func TestPlayerActorRegisterLogin(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	regReply, err := rt.Ask(ctx, regPID, &gamev1.RegisterActorReq{Account: "alice", Password: "pw", Nickname: "爱丽丝"})
	if err != nil {
		t.Fatalf("Ask register: %v", err)
	}
	reg := regReply.(*gamev1.RegisterActorReply)
	if reg.GetPlayerId() != "p-test" {
		t.Fatalf("注册回执不符: %+v", reg)
	}
	// 注册两段式：临时实例自停。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := rt.Stats(regPID); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("注册后临时 actor 未自停")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 登录（懒激活 + 聚合根加载）。
	loginReply, err := rt.Ask(ctx, loginPID, &gamev1.LoginActorReq{PlayerId: "p-test", Password: "pw", Token: "t1"})
	if err != nil {
		t.Fatalf("Ask login: %v", err)
	}
	login := loginReply.(*gamev1.LoginActorReply)
	if login.GetPlayer().GetNickname() != "爱丽丝" {
		t.Fatalf("登录回执不符: %+v", login)
	}

	// 口令错误：业务错误以 error 返回（reason 经集群 error 通道往返保留）。
	if _, err = rt.Ask(ctx, loginPID, &gamev1.LoginActorReq{PlayerId: "p-test", Password: "bad", Token: "t2"}); atlaserrors.Reason(err) != "PASSWORD_WRONG" {
		t.Fatalf("口令错误应返回 PASSWORD_WRONG, got %v", err)
	}
}

// TestPlayerActorGrantAndQuery 验证聚合根 undo 写与内存快照查询。
func TestPlayerActorGrantAndQuery(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterActorReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginActorReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}

	// 发放道具两次（累加语义）。
	for i := 0; i < 2; i++ {
		if _, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemActorReq{ItemId: 1001, Count: 3}); err != nil {
			t.Fatalf("grant#%d: %v", i, err)
		}
	}
	// 背包查询（内存快照）。
	bpReply, err := rt.Ask(ctx, loginPID, &gamev1.GetBackpackActorReq{})
	if err != nil {
		t.Fatalf("get backpack: %v", err)
	}
	bp := bpReply.(*gamev1.GetBackpackActorReply)
	if len(bp.GetItems()) != 1 || bp.GetItems()[0].GetCount() != 6 {
		t.Fatalf("背包不符: %+v", bp.GetItems())
	}
	// 玩家摘要查询。
	pReply, err := rt.Ask(ctx, loginPID, &gamev1.GetPlayerActorReq{})
	if err != nil {
		t.Fatalf("get player: %v", err)
	}
	if got := pReply.(*gamev1.GetPlayerActorReply); got.GetPlayer().GetPlayerId() != "p-test" {
		t.Fatalf("玩家摘要不符: %+v", got)
	}
}

// TestPlayerActorSnapshotAndPersist 验证定时快照与下线落库。
func TestPlayerActorSnapshotAndPersist(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, store := newActorEnv(t, time.Minute, 30*time.Millisecond)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterActorReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginActorReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemActorReq{ItemId: 1001, Count: 5}); err != nil {
		t.Fatalf("grant: %v", err)
	}
	// 等待定时快照。
	deadline := time.Now().Add(2 * time.Second)
	for {
		if p, _ := store.LoadPlayer(ctx, "p-test"); p != nil && len(p.Items) == 1 && p.Items[0].Count == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("定时快照未生效")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 登出（Tell 对象直传，本地路由之外的桩 switch 命中）→ 自停 → OnStop 落库。
	if err := rt.Tell(ctx, loginPID, &gamev1.LogoutActorMsg{Token: "t1", Reason: "logout"}); err != nil {
		t.Fatalf("Tell logout: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, ok := rt.Stats(loginPID); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("登出后 actor 未停止")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if store.Saves() == 0 {
		t.Fatal("下线落库未执行")
	}
}

// TestPlayerActorGrantBeforeLogin 验证未登录发放道具被拒（PLAYER_NOT_ONLINE）。
func TestPlayerActorGrantBeforeLogin(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterActorReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemActorReq{ItemId: 1001, Count: 1})
	if atlaserrors.Reason(err) != "PLAYER_NOT_ONLINE" {
		t.Fatalf("未登录发放应返回 PLAYER_NOT_ONLINE, got %v", err)
	}
}

// TestDispatchRejectsUnknownMessage 验证生成的桩对未知 Ask 消息（本地路由未命中）
// 返回 ErrUnknownMessage 哨兵。Tell 的 handler 错误不回传调用方（异步投递，现状语义）。
func TestDispatchRejectsUnknownMessage(t *testing.T) {
	ctx := context.Background()
	rt, regPID, _, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, "unknown-message"); !errors.Is(err, core.ErrUnknownMessage) {
		t.Fatalf("未知消息应返回 ErrUnknownMessage, got %v", err)
	}
}

// vetoServer 是验证生成桩前置钩子契约的最小业务实现：
// OnBeforeAsk 按 veto 标志决定放行或拦截；GetPlayer 是放行后分发到的业务方法。
type vetoServer struct {
	gamev1.UnimplementedPlayerActorServer
	veto error
}

func (s *vetoServer) OnBeforeAsk(_ core.ActorContext, _ any) error { return s.veto }

// GetPlayer 实现 PlayerActorServer：放行后分发到的业务方法（未登录拒查）。
func (s *vetoServer) GetPlayer(_ core.ActorContext, _ *gamev1.GetPlayerActorReq) (*gamev1.GetPlayerActorReply, error) {
	return nil, errorv1.ErrPlayerNotOnline("玩家不在线")
}

// TestDispatchBeforeAskHook 验证生成桩的消息前置钩子契约：
// 业务实现 OnBeforeAsk 返回错误 → 中断本次 Ask（错误即结果）；
// 返回 nil → 继续正常分发到业务方法。
func TestDispatchBeforeAskHook(t *testing.T) {
	s := &vetoServer{}
	h := gamev1.NewPlayerActorServer(s)

	boom := errors.New("钩子拦截")
	s.veto = boom
	if _, err := h.OnAsk(nil, &gamev1.GetPlayerActorReq{}); !errors.Is(err, boom) {
		t.Fatalf("前置钩子拦截应中断 Ask, got %v", err)
	}

	s.veto = nil
	if _, err := h.OnAsk(nil, &gamev1.GetPlayerActorReq{}); atlaserrors.Reason(err) != "PLAYER_NOT_ONLINE" {
		t.Fatalf("钩子放行后应分发到业务方法（未登录拒查）, got %v", err)
	}
}
