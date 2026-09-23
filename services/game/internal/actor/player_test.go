package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// flushMode 是 memRepo.FlushPlayer 的注入模式（编排路径故障注入）。
type flushMode int

const (
	flushOK        flushMode = iota // Redis 成功（默认）
	flushFailBoth                   // 双库失败（在线冻结触发）
	flushMongoOnly                  // Redis 失败、Mongo 成功（强制下线触发）
)

// memRepo 是玩家仓储的内存实现（快照 + 持久化两级，记录落库次数）。
// 并发安全：actor 定时快照与测试轮询并发访问。
type memRepo struct {
	mu        sync.Mutex
	players   map[string]*models.Player
	snapshots map[string]*models.Player
	saves     int
	mode      flushMode
}

func newMemRepo() *memRepo {
	return &memRepo{players: make(map[string]*models.Player), snapshots: make(map[string]*models.Player)}
}

// setMode 注入 FlushPlayer 模式（冻结/强制下线场景用）。
func (r *memRepo) setMode(m flushMode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mode = m
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

// LoadCredential mongo 专用读（含认证字段；登录口令校验用）。
func (r *memRepo) LoadCredential(_ context.Context, playerID string) (*models.Player, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.players[playerID]; ok {
		return clonePlayer(p), nil
	}
	return nil, repo.ErrPlayerNotFound
}

// FlushPlayer 统一落盘（测试桩：按注入模式返回结果）。
func (r *memRepo) FlushPlayer(_ context.Context, p *models.Player, lastCRC uint32) (repo.FlushResult, uint32, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.mode {
	case flushFailBoth:
		return repo.FlushBothFailed, lastCRC, errors.New("fake: 双库落盘失败")
	case flushMongoOnly:
		r.players[p.PlayerID] = clonePlayer(p)
		r.saves++
		return repo.FlushMongoOK, lastCRC + 1, nil
	default:
		r.snapshots[p.PlayerID] = clonePlayer(p)
		r.players[p.PlayerID] = clonePlayer(p)
		r.saves++
		return repo.FlushRedisOK, lastCRC + 1, nil
	}
}

// PersistSnapshot 玩家快照 PERSIST（测试桩：no-op）。
func (r *memRepo) PersistSnapshot(_ context.Context, playerID string) error {
	return nil
}

// TTLOfSnapshot 快照 TTL 查询（测试桩：有档固定 72h）。
func (r *memRepo) TTLOfSnapshot(_ context.Context, playerID string) (time.Duration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
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

// fakeMatch 是匹配队列客户端的内存实现（记录入队参数、取消与状态查询）。
type fakeMatch struct {
	mu       sync.Mutex
	enters   []enterCall
	cancels  []string
	statuses []string
	canceled bool
	created  []string // 建队记录（playerID）
	joined   []string // 加入记录（partyID|playerID）
	left     []string // 离开记录（partyID|playerID）
	queued   []string // 整队入队记录（partyID|ruleset）
}

type enterCall struct {
	playerID string
	level    int32
	ruleset  string
}

func newFakeMatch() *fakeMatch { return &fakeMatch{} }

func (f *fakeMatch) Enter(_ context.Context, playerID string, level int32, ruleset string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enters = append(f.enters, enterCall{playerID: playerID, level: level, ruleset: ruleset})
	return nil
}

func (f *fakeMatch) Cancel(_ context.Context, playerID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, playerID)
	return f.canceled, nil
}

func (f *fakeMatch) Status(_ context.Context, playerID string) (*matcherv1.QueryMatchReply, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, playerID)
	return &matcherv1.QueryMatchReply{
		State:    matcherv1.MatchState_MATCH_STATE_WAITING,
		TicketId: "t-1", MatchId: "m-1", BattleId: "b-1",
	}, nil
}

// ---- 组队域（记录调用供断言）----

func (f *fakeMatch) Create(_ context.Context, playerID string, _ int32) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, playerID)
	return "party-" + playerID, nil
}

func (f *fakeMatch) Join(_ context.Context, partyID, playerID string, _ int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joined = append(f.joined, partyID+"|"+playerID)
	return nil
}

func (f *fakeMatch) Leave(_ context.Context, partyID, playerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.left = append(f.left, partyID+"|"+playerID)
	return nil
}

func (f *fakeMatch) Describe(_ context.Context, partyID string) (*matcherv1.PartyInfo, error) {
	return &matcherv1.PartyInfo{
		PartyId:  partyID,
		LeaderId: "leader",
		Members: []*matcherv1.PartyMember{
			{PlayerId: "leader", Level: 10},
			{PlayerId: "member", Level: 11},
		},
	}, nil
}

func (f *fakeMatch) Queue(_ context.Context, partyID, ruleset string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queued = append(f.queued, partyID+"|"+ruleset)
	return "t-party-1", nil
}

// cancelSnapshot 复制取消记录（测试断言用，并发安全）。
func (f *fakeMatch) cancelSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancels...)
}

// newActorEnv 构造本地 actor 运行时（注册 PlayerActor，注册/登录 PID 分离）。
func newActorEnv(t *testing.T, snapTTL, snapTick time.Duration) (*core.LocalRuntime, types.PID, types.PID, *memRepo, *fakeMatch) {
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
	match := newFakeMatch()
	svc := handler.NewPlayerHandler(store, biz.PlayerServiceOptions{
		NewPlayerID: func() string { return "p-test" },
	})
	if err := rt.Register(NewProps(svc, store, match, snapTick)); err != nil {
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
	return rt, regPID, loginPID, store, match
}

// TestPlayerActorRegisterLogin 验证注册（自停）/登录（聚合根加载）/口令错误
// （同节点对象直传形态：生成的桩 switch 直接命中具体消息）。
func TestPlayerActorRegisterLogin(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}

	regReply, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw", Nickname: "爱丽丝"})
	if err != nil {
		t.Fatalf("Ask register: %v", err)
	}
	reg := regReply.(*gamev1.RegisterReply)
	if reg.GetPlayerId() != "p-test" {
		t.Fatalf("注册回执不符: %+v", reg)
	}
	// 注册两段式：临时实例自停。
	waitStop(t, rt, regPID, "注册后临时 actor 未自停")

	// 登录（懒激活 + 聚合根加载）；token 仅链路使用，game 侧不持久化。
	loginReply, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"})
	if err != nil {
		t.Fatalf("Ask login: %v", err)
	}
	login := loginReply.(*gamev1.LoginReply)
	if login.GetPlayer().GetNickname() != "爱丽丝" {
		t.Fatalf("登录回执不符: %+v", login)
	}

	// 口令错误：业务错误以 error 返回（reason 经集群 error 通道往返保留）。
	if _, err = rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "bad", Token: "t2"}); atlaserrors.Reason(err) != "PASSWORD_WRONG" {
		t.Fatalf("口令错误应返回 PASSWORD_WRONG, got %v", err)
	}
}

// TestPlayerActorGrantAndQuery 验证聚合根 undo 写与内存快照查询。
func TestPlayerActorGrantAndQuery(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}

	// 发放道具两次（累加语义）。
	for i := 0; i < 2; i++ {
		if _, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemReq{ItemId: 1001, Count: 3}); err != nil {
			t.Fatalf("grant#%d: %v", i, err)
		}
	}
	// 背包查询（内存快照）。
	bpReply, err := rt.Ask(ctx, loginPID, &gamev1.GetBackpackReq{})
	if err != nil {
		t.Fatalf("get backpack: %v", err)
	}
	bp := bpReply.(*gamev1.BackpackReply)
	if len(bp.GetItems()) != 1 || bp.GetItems()[0].GetCount() != 6 {
		t.Fatalf("背包不符: %+v", bp.GetItems())
	}
	// 玩家摘要查询。
	pReply, err := rt.Ask(ctx, loginPID, &gamev1.GetPlayerReq{})
	if err != nil {
		t.Fatalf("get player: %v", err)
	}
	if got := pReply.(*gamev1.PlayerReply); got.GetPlayer().GetPlayerId() != "p-test" {
		t.Fatalf("玩家摘要不符: %+v", got)
	}
}

// TestPlayerActorSnapshotAndPersist 验证定时快照与下线落库。
func TestPlayerActorSnapshotAndPersist(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, store, _ := newActorEnv(t, time.Minute, 30*time.Millisecond)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemReq{ItemId: 1001, Count: 5}); err != nil {
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
	// 登出（Tell 对象直传，本地路由之外的桩 switch 命中）→ 下线流程反转
	//（先落库短重试，成功才停止自身）→ 自停。
	if err := rt.Tell(ctx, loginPID, &gamev1.LogoutMsg{
		Reason: gamev1.LogoutReason_LOGOUT_REASON_LOGOUT,
	}); err != nil {
		t.Fatalf("Tell logout: %v", err)
	}
	waitStop(t, rt, loginPID, "登出后 actor 未停止")
	if store.Saves() == 0 {
		t.Fatal("下线落库未执行")
	}
}

// TestPlayerActorGrantBeforeLogin 验证未登录发放道具被拒（PLAYER_NOT_ONLINE）。
func TestPlayerActorGrantBeforeLogin(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemReq{ItemId: 1001, Count: 1})
	if atlaserrors.Reason(err) != "PLAYER_NOT_ONLINE" {
		t.Fatalf("未登录发放应返回 PLAYER_NOT_ONLINE, got %v", err)
	}
}

// TestDispatchRejectsUnknownMessage 验证生成的桩对未知 Ask 消息（本地路由未命中）
// 返回 ErrUnknownMessage 哨兵。Tell 的 handler 错误不回传调用方（异步投递，现状语义）。
func TestDispatchRejectsUnknownMessage(t *testing.T) {
	ctx := context.Background()
	rt, regPID, _, _, _ := newActorEnv(t, 0, 0)
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
	gamev1.UnimplementedPlayerServiceActorServer
	veto error
}

func (s *vetoServer) OnBeforeAsk(_ core.ActorContext, _ any) error { return s.veto }

// GetPlayer 实现 PlayerServiceActorServer：放行后分发到的业务方法（未登录拒查）。
func (s *vetoServer) GetPlayer(_ core.ActorContext, _ *gamev1.GetPlayerReq) (*gamev1.PlayerReply, error) {
	return nil, errorv1.ErrPlayerNotOnline("玩家不在线")
}

// TestDispatchBeforeAskHook 验证生成桩的消息前置钩子契约：
// 业务实现 OnBeforeAsk 返回错误 → 中断本次 Ask（错误即结果）；
// 返回 nil → 继续正常分发到业务方法。
func TestDispatchBeforeAskHook(t *testing.T) {
	s := &vetoServer{}
	h := gamev1.NewPlayerServiceActorServer(s)

	boom := errors.New("钩子拦截")
	s.veto = boom
	if _, err := h.OnAsk(nil, &gamev1.GetPlayerReq{}); !errors.Is(err, boom) {
		t.Fatalf("前置钩子拦截应中断 Ask, got %v", err)
	}

	s.veto = nil
	if _, err := h.OnAsk(nil, &gamev1.GetPlayerReq{}); atlaserrors.Reason(err) != "PLAYER_NOT_ONLINE" {
		t.Fatalf("钩子放行后应分发到业务方法（未登录拒查）, got %v", err)
	}
}

// TestPlayerActorReloginKeepsInMemoryState 验证重复登录的幂等守卫（顶号回归）：
// 重登复用内存聚合根（在线权威态），不重载存储——快照间隙的内存写（如发放道具）
// 不被陈旧快照覆盖。回归背景：旧实现重登无条件重载，redis 快照（最多旧 snapTick）
// 会盖掉未落盘的内存写，玩家刚领的道具丢失。
func TestPlayerActorReloginKeepsInMemoryState(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}
	// 发放道具（仅内存：snapTick=0 无定时快照，存储层没有该数据）。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemReq{ItemId: 1001, Count: 6}); err != nil {
		t.Fatalf("grant: %v", err)
	}

	// 重复登录（顶号重登）：应复用内存权威态。
	relog, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t2"})
	if err != nil {
		t.Fatalf("relogin: %v", err)
	}
	if got := relog.(*gamev1.LoginReply); got.GetPlayer().GetPlayerId() != "p-test" {
		t.Fatalf("重登回执不符: %+v", got)
	}

	// 道具不被重载覆盖：背包与重登前内存一致。
	bp, err := rt.Ask(ctx, loginPID, &gamev1.GetBackpackReq{})
	if err != nil {
		t.Fatalf("backpack: %v", err)
	}
	items := bp.(*gamev1.BackpackReply).GetItems()
	if len(items) != 1 || items[0].GetCount() != 6 {
		t.Fatalf("重登后背包被重载覆盖: %+v", items)
	}
}

// waitStop 轮询等待 actor 停止（自停/登出停机的公共断言辅助）。
func waitStop(t *testing.T, rt *core.LocalRuntime, pid types.PID, failMsg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := rt.Stats(pid); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(failMsg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newLoggedInActor 构造已登录的 actor 环境（注册 + 登录完成，聚合根已加载）。
func newLoggedInActor(t *testing.T) (*core.LocalRuntime, types.PID, *memRepo) {
	t.Helper()
	ctx := context.Background()
	rt, regPID, loginPID, store, _ := newActorEnv(t, 0, 10*time.Millisecond)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}
	return rt, loginPID, store
}

// TestPlayerActorFreezeGuards 验证在线冻结编排（设计 §6.2）：
// 双库失败 → 冻结（改档拒绝 SERVER_FROZEN；只读查询与重登放行）；
// Redis 恢复 → 解冻恢复改档。白名单按类型判定（type switch）。
func TestPlayerActorFreezeGuards(t *testing.T) {
	ctx := context.Background()
	rt, loginPID, store := newLoggedInActor(t)

	// 双库失败：等待冻结生效（GrantItem 从正常转为 SERVER_FROZEN）。
	store.setMode(flushFailBoth)
	waitFor(t, 2*time.Second, func() bool {
		_, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemReq{ItemId: 1, Count: 1})
		return atlaserrors.Reason(err) == "SERVER_FROZEN"
	}, "双库失败未进入在线冻结")

	// 冻结期：只读查询放行（内存权威态返回，不触存储）。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.GetPlayerDataReq{}); err != nil {
		t.Fatalf("冻结期只读查询应放行: %v", err)
	}
	// 冻结期：重新登录放行（复用内存权威态，不重载）。
	relog, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t2"})
	if err != nil {
		t.Fatalf("冻结期重登应放行: %v", err)
	}
	if got := relog.(*gamev1.LoginReply); got.GetPlayer().GetPlayerId() != "p-test" {
		t.Fatalf("冻结期重登回执不符: %+v", got)
	}

	// Redis 恢复：解冻，改档恢复。
	store.setMode(flushOK)
	waitFor(t, 2*time.Second, func() bool {
		_, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemReq{ItemId: 2, Count: 1})
		return err == nil
	}, "Redis 恢复未解冻")
}

// TestPlayerActorFreezeLogout 验证冻结期下线放行（编排放行白名单）：
// LogoutMsg 直达下线流程（落库 + 停止），不被冻结拦截。
func TestPlayerActorFreezeLogout(t *testing.T) {
	ctx := context.Background()
	rt, loginPID, store := newLoggedInActor(t)
	store.setMode(flushFailBoth)
	waitFor(t, 2*time.Second, func() bool {
		_, err := rt.Ask(ctx, loginPID, &gamev1.GrantItemReq{ItemId: 1, Count: 1})
		return atlaserrors.Reason(err) == "SERVER_FROZEN"
	}, "双库失败未进入在线冻结")

	if err := rt.Tell(ctx, loginPID, &gamev1.LogoutMsg{Reason: gamev1.LogoutReason_LOGOUT_REASON_LOGOUT}); err != nil {
		t.Fatalf("冻结期下线 Tell 不应被拒: %v", err)
	}
	waitStop(t, rt, loginPID, "冻结期下线后 actor 未停止")
	if store.Saves() == 0 {
		t.Fatal("冻结期下线应先完成落库")
	}
}

// TestPlayerActorForceLogoutOnMongoOnly 验证「仅 Mongo 成功 → 强制下线」
// （设计 §6.2：在线热路径依赖 Redis，与 redis 不可用拒登的降级姿态一致）。
func TestPlayerActorForceLogoutOnMongoOnly(t *testing.T) {
	rt, loginPID, store := newLoggedInActor(t)
	store.setMode(flushMongoOnly)
	waitStop(t, rt, loginPID, "仅 Mongo 落盘成功未强制下线")
	if store.Saves() == 0 {
		t.Fatal("强制下线应先完成落库")
	}
}

// waitFor 轮询等待条件成立（超时 failf 报错，含现场快照）。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, failf string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(failf, args...)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
