package actor

import (
	"context"
	"sync"
	"testing"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"google.golang.org/protobuf/proto"
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

// askBytes 以字节信封向 actor 发起请求（模拟跨节点传输形态）。
func askBytes(ctx context.Context, rt *core.LocalRuntime, pid types.PID, env *gamev1.PlayerActorMsg) (any, error) {
	b, err := proto.Marshal(env)
	if err != nil {
		return nil, err
	}
	return rt.Ask(ctx, pid, b)
}

// registerEnvelope 组注册信封。
func registerEnvelope(account, password string) *gamev1.PlayerActorMsg {
	return &gamev1.PlayerActorMsg{
		Kind: &gamev1.PlayerActorMsg_Register{Register: &gamev1.RegisterActorReq{Account: account, Password: password, Nickname: "爱丽丝"}},
	}
}

// loginEnvelope 组登录信封。
func loginEnvelope(playerID, password, token string) *gamev1.PlayerActorMsg {
	return &gamev1.PlayerActorMsg{
		Kind: &gamev1.PlayerActorMsg_Login{Login: &gamev1.LoginActorReq{PlayerId: playerID, Password: password, Token: token}},
	}
}

// grantEnvelope 组发放道具信封。
func grantEnvelope(itemID, count uint32) *gamev1.PlayerActorMsg {
	return &gamev1.PlayerActorMsg{
		Kind: &gamev1.PlayerActorMsg_GrantItem{GrantItem: &gamev1.GrantItemActorReq{ItemId: itemID, Count: count}},
	}
}

// TestPlayerActorRegisterLogin 验证注册（自停）/登录（聚合根加载）/口令错误。
func TestPlayerActorRegisterLogin(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	regReply, err := askBytes(ctx, rt, regPID, registerEnvelope("alice", "pw"))
	if err != nil {
		t.Fatalf("Ask register: %v", err)
	}
	reg := regReply.(*gamev1.RegisterActorReply)
	if !reg.GetOk() || reg.GetPlayerId() != "p-test" {
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
	loginReply, err := askBytes(ctx, rt, loginPID, loginEnvelope("p-test", "pw", "t1"))
	if err != nil {
		t.Fatalf("Ask login: %v", err)
	}
	login := loginReply.(*gamev1.LoginActorReply)
	if !login.GetOk() || login.GetPlayer().GetNickname() != "爱丽丝" {
		t.Fatalf("登录回执不符: %+v", login)
	}

	// 口令错误。
	loginReply, err = askBytes(ctx, rt, loginPID, loginEnvelope("p-test", "bad", "t2"))
	if err != nil {
		t.Fatalf("Ask login2: %v", err)
	}
	login2 := loginReply.(*gamev1.LoginActorReply)
	if login2.GetOk() || login2.GetErrorReason() != "PASSWORD_WRONG" {
		t.Fatalf("口令错误回执不符: %+v", login2)
	}
}

// TestPlayerActorGrantAndQuery 验证聚合根 undo 写与内存快照查询。
func TestPlayerActorGrantAndQuery(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := askBytes(ctx, rt, regPID, registerEnvelope("alice", "pw")); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := askBytes(ctx, rt, loginPID, loginEnvelope("p-test", "pw", "t1")); err != nil {
		t.Fatalf("login: %v", err)
	}

	// 发放道具两次（累加语义）。
	for i := 0; i < 2; i++ {
		reply, err := askBytes(ctx, rt, loginPID, grantEnvelope(1001, 3))
		if err != nil {
			t.Fatalf("grant#%d: %v", i, err)
		}
		if !reply.(*gamev1.GrantItemActorReply).GetOk() {
			t.Fatalf("grant#%d 回执失败: %+v", i, reply)
		}
	}
	// 背包查询（内存快照）。
	bpReply, err := askBytes(ctx, rt, loginPID, &gamev1.PlayerActorMsg{
		Kind: &gamev1.PlayerActorMsg_GetBackpack{GetBackpack: &gamev1.GetBackpackActorReq{}},
	})
	if err != nil {
		t.Fatalf("get backpack: %v", err)
	}
	bp := bpReply.(*gamev1.GetBackpackActorReply)
	if !bp.GetOk() || len(bp.GetItems()) != 1 || bp.GetItems()[0].GetCount() != 6 {
		t.Fatalf("背包不符: %+v", bp.GetItems())
	}
	// 玩家摘要查询。
	pReply, err := askBytes(ctx, rt, loginPID, &gamev1.PlayerActorMsg{
		Kind: &gamev1.PlayerActorMsg_GetPlayer{GetPlayer: &gamev1.GetPlayerActorReq{}},
	})
	if err != nil {
		t.Fatalf("get player: %v", err)
	}
	if got := pReply.(*gamev1.GetPlayerActorReply); !got.GetOk() || got.GetPlayer().GetPlayerId() != "p-test" {
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
	if _, err := askBytes(ctx, rt, regPID, registerEnvelope("alice", "pw")); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := askBytes(ctx, rt, loginPID, loginEnvelope("p-test", "pw", "t1")); err != nil {
		t.Fatalf("login: %v", err)
	}
	if _, err := askBytes(ctx, rt, loginPID, grantEnvelope(1001, 5)); err != nil {
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
	// 登出自停 → OnStop 落库。
	out, err := proto.Marshal(&gamev1.PlayerActorMsg{
		Kind: &gamev1.PlayerActorMsg_Logout{Logout: &gamev1.LogoutActorMsg{Token: "t1", Reason: "logout"}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := rt.Tell(ctx, loginPID, out); err != nil {
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

// TestPlayerActorGrantBeforeLogin 验证未登录发放道具被拒。
func TestPlayerActorGrantBeforeLogin(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := askBytes(ctx, rt, regPID, registerEnvelope("alice", "pw")); err != nil {
		t.Fatalf("register: %v", err)
	}
	reply, err := askBytes(ctx, rt, loginPID, grantEnvelope(1001, 1))
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	g := reply.(*gamev1.GrantItemActorReply)
	if g.GetOk() || g.GetErrorReason() != "PLAYER_NOT_ONLINE" {
		t.Fatalf("未登录发放回执不符: %+v", g)
	}
}

// TestDecodeEnvelopeRejectsUnknown 验证未知消息类型与坏字节被拒绝。
func TestDecodeEnvelopeRejectsUnknown(t *testing.T) {
	if _, err := decodeEnvelope(42); err == nil {
		t.Fatal("未知消息类型应报错")
	}
	if _, err := decodeEnvelope([]byte("garbage")); err == nil {
		t.Fatal("坏字节应报错")
	}
}
