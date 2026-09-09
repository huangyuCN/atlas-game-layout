package actor

import (
	"context"
	"sync"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/models"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	lockstepimpl "github.com/huangyuCN/atlas/contrib/lockstep"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// memNotifier 是下行通知的内存实现（帧/结束通知记录）。
type memNotifier struct {
	mu     sync.Mutex
	frames map[string][]*locksteppb.LockstepFrame // playerID → 帧序列
	ends   map[string][]string                    // playerID → 胜者序列
}

func newMemNotifier() *memNotifier {
	return &memNotifier{frames: make(map[string][]*locksteppb.LockstepFrame), ends: make(map[string][]string)}
}

func (n *memNotifier) PublishFrame(_ context.Context, playerID, _ string, f *locksteppb.LockstepFrame) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.frames[playerID] = append(n.frames[playerID], f)
	return nil
}

func (n *memNotifier) PublishEnd(_ context.Context, playerID, _, winner string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ends[playerID] = append(n.ends[playerID], winner)
	return nil
}

// snapshots 返回两个玩家的帧快照（测试断言用）。
func (n *memNotifier) snapshots() (map[string][]*locksteppb.LockstepFrame, map[string][]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	frames := make(map[string][]*locksteppb.LockstepFrame, len(n.frames))
	for k, v := range n.frames {
		frames[k] = append([]*locksteppb.LockstepFrame(nil), v...)
	}
	ends := make(map[string][]string, len(n.ends))
	for k, v := range n.ends {
		ends[k] = append([]string(nil), v...)
	}
	return frames, ends
}

// memResultRepo 是结算结果仓储的内存实现。
type memResultRepo struct {
	mu    sync.Mutex
	saved *models.BattleResult
}

func (r *memResultRepo) Save(_ context.Context, res *models.BattleResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saved = res
	return nil
}

// memPublisher 是结算事件发布的内存实现。
type memPublisher struct {
	mu sync.Mutex
	ev *battlev1.BattleSettledEvent
}

func (p *memPublisher) PublishSettled(_ context.Context, ev *battlev1.BattleSettledEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ev = ev
	return nil
}

// localRT 适配 core.LocalRuntime 到战斗 actor 的 Runtime 接口（剥离可选参数）。
type localRT struct{ *core.LocalRuntime }

// Spawn 适配 Runtime.Spawn（剥离 SpawnOption）。
func (l localRT) Spawn(ctx context.Context, pid types.PID) (core.Ref, error) {
	return l.LocalRuntime.Spawn(ctx, pid)
}

// Stop 适配 Runtime.Stop（剥离 StopOption）。
func (l localRT) Stop(ctx context.Context, pid types.PID) error {
	return l.LocalRuntime.Stop(ctx, pid)
}

// Ask 适配 Runtime.Ask（剥离 SendOption）。
func (l localRT) Ask(ctx context.Context, pid types.PID, req any, opts ...core.SendOption) (any, error) {
	return l.LocalRuntime.Ask(ctx, pid, req)
}

// battleEnv 是战斗 actor 的进程内本地装配（真 lockstep 会话 + 内存依赖）。
type battleEnv struct {
	rt        *core.LocalRuntime
	pid       types.PID
	notifier  *memNotifier
	result    *memResultRepo
	publisher *memPublisher
	reg       *pubsub.Registry
}

// newBattleEnv 起本地 actor 运行时并拉起战斗 actor（短帧间隔加速测试）。
func newBattleEnv(t *testing.T) *battleEnv {
	t.Helper()
	rt, err := core.NewLocalRuntime()
	if err != nil {
		t.Fatalf("NewLocalRuntime: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("Runtime Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	reg, err := pubsub.New(rt)
	if err != nil {
		t.Fatalf("pubsub: %v", err)
	}
	notifier := newMemNotifier()
	result := &memResultRepo{}
	publisher := &memPublisher{}
	err = rt.Register(NewProps(Props{
		Rt:         localRT{rt},
		Registry:   reg,
		Storage:    lockstepimpl.NewMemoryStorage(),
		ResultRepo: result,
		Notifier:   notifier,
		Publisher:  publisher,
		Cfg: Config{
			TickInterval:  int64(10 * time.Millisecond),
			TrackLen:      DefaultTrackLen,
			MaxFrames:     DefaultMaxFrames,
			SnapshotEvery: DefaultSnapshotEvery,
		},
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	pid, err := types.NewPID("battle", "b-test01")
	if err != nil {
		t.Fatalf("NewPID: %v", err)
	}
	if _, err := rt.Spawn(context.Background(), pid); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	return &battleEnv{rt: rt, pid: pid, notifier: notifier, result: result, publisher: publisher, reg: reg}
}

// ask 以具体消息对象向战斗 actor 请求（同节点直传形态，生成的桩 switch 直接命中）。
func (e *battleEnv) ask(ctx context.Context, req any) (any, error) {
	return e.rt.Ask(ctx, e.pid, req)
}

// TestBattleActorFullLoop 验证战斗闭环：开局 → 双人加入 → 帧输入 → 胜负结算
// （帧广播双方一致 + 结束通知 + 结果落库 + 事件发布 + actor 自停）。
func TestBattleActorFullLoop(t *testing.T) {
	env := newBattleEnv(t)
	ctx := context.Background()
	seedAndJoin(t, env, ctx)
	sendFrameInputs(t, env, ctx)

	// 等待结算（快照第 10 帧携带胜者）。
	waitSettled(t, env)
	assertSettleEvent(t, env)
	assertSavedResult(t, env)
	assertFramesConsistent(t, env)
	assertActorStopped(t, env)
}

// seedAndJoin 开局并让双玩家加入（含非参战玩家被拒断言）。
func seedAndJoin(t *testing.T, env *battleEnv, ctx context.Context) {
	t.Helper()
	reply, err := env.ask(ctx, &battlev1.CreateBattleRequest{MatchId: "m-1", PlayerIds: []string{"p-a", "p-b"}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if create, ok := reply.(*battlev1.CreateBattleReply); !ok || create.GetBattleId() != "b-test01" {
		t.Fatalf("开局回执不符: %T %+v", reply, reply)
	}
	for _, p := range []string{"p-a", "p-b"} {
		reply, err := env.ask(ctx, &battlev1.JoinBattleReq{PlayerId: p})
		if err != nil {
			t.Fatalf("join %s: %v", p, err)
		}
		join, ok := reply.(*battlev1.JoinBattleReply)
		if !ok || join.GetMeta().GetSessionId() != "b-test01" || join.GetSnapshot() == nil {
			t.Fatalf("%s 加入回执不符: %T %+v", p, reply, reply)
		}
	}
	// 非参战玩家被拒（错误语义上移到产生点：BATTLE_NOT_FOUND）。
	_, err = env.ask(ctx, &battlev1.JoinBattleReq{PlayerId: "p-x"})
	if atlaserrors.Reason(err) != "BATTLE_NOT_FOUND" {
		t.Fatalf("非参战玩家应被拒 BATTLE_NOT_FOUND, got %v", err)
	}
}

// sendFrameInputs 发送帧输入：a 每帧前进 1（第 5 帧到终点），b 原地不动。
func sendFrameInputs(t *testing.T, env *battleEnv, ctx context.Context) {
	t.Helper()
	for i := uint64(1); i <= 12; i++ {
		send := func(p string, step byte) {
			_, err := env.ask(ctx, &battlev1.FrameInputReq{
				PlayerId: p,
				Input:    &locksteppb.LockstepInput{FrameId: i, PlayerId: p, Payload: []byte{step}},
			})
			if err != nil {
				t.Fatalf("帧输入 %s@%d: %v", p, i, err)
			}
		}
		send("p-a", 1)
		send("p-b", 0)
	}
}

// assertSettleEvent 断言结算事件（胜者与玩家结果）。
func assertSettleEvent(t *testing.T, env *battleEnv) {
	t.Helper()
	env.publisher.mu.Lock()
	ev := env.publisher.ev
	env.publisher.mu.Unlock()
	if ev == nil || len(ev.GetPlayers()) != 2 {
		t.Fatalf("结算事件不符: %+v", ev)
	}
	if winner := settledWinner(ev); winner != "p-a" {
		t.Fatalf("结算胜者 = %q, want p-a", winner)
	}
	for _, p := range ev.GetPlayers() {
		if p.GetPlayerId() == "p-a" && (!p.GetWin() || p.GetScore() != 1) {
			t.Fatalf("胜者结果不符: %+v", p)
		}
	}
}

// assertSavedResult 断言落库结果与事件一致。
func assertSavedResult(t *testing.T, env *battleEnv) {
	t.Helper()
	env.result.mu.Lock()
	saved := env.result.saved
	env.result.mu.Unlock()
	if saved == nil || saved.MatchID != "m-1" || saved.TotalFrames < 10 || len(saved.Players) != 2 {
		t.Fatalf("落库结果不符: %+v", saved)
	}
	for _, p := range saved.Players {
		if p.PlayerID == "p-a" && (!p.Win || p.Score != 1) {
			t.Fatalf("落库胜者不符: %+v", p)
		}
	}
}

// assertFramesConsistent 断言帧广播与结束通知双方一致。
func assertFramesConsistent(t *testing.T, env *battleEnv) {
	t.Helper()
	frames, ends := env.notifier.snapshots()
	aFrames, bFrames := frames["p-a"], frames["p-b"]
	if len(aFrames) == 0 || len(aFrames) != len(bFrames) {
		t.Fatalf("帧广播数不一致: a=%d b=%d", len(aFrames), len(bFrames))
	}
	lastA, lastB := aFrames[len(aFrames)-1], bFrames[len(bFrames)-1]
	if lastA.GetFrameId() != lastB.GetFrameId() || lastA.GetSnapshot() == nil ||
		string(lastA.GetSnapshot().GetStateHash()) != string(lastB.GetSnapshot().GetStateHash()) {
		t.Fatalf("末帧快照不一致: a=%+v b=%+v", lastA, lastB)
	}
	// 结束通知双方一致（胜者相同）。
	if len(ends["p-a"]) != 1 || len(ends["p-b"]) != 1 || ends["p-a"][0] != "p-a" || ends["p-b"][0] != "p-a" {
		t.Fatalf("结束通知不符: %+v", ends)
	}
}

// assertActorStopped 断言结算后战斗 actor 自停。
func assertActorStopped(t *testing.T, env *battleEnv) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := env.rt.Stats(env.pid); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("结算后战斗 actor 未自停")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// settledWinner 从结算事件取胜者玩家 ID。
func settledWinner(ev *battlev1.BattleSettledEvent) string {
	for _, p := range ev.GetPlayers() {
		if p.GetWin() {
			return p.GetPlayerId()
		}
	}
	return ""
}

// waitSettled 轮询直至结算事件发布（断言超时失败）。
func waitSettled(t *testing.T, env *battleEnv) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		env.publisher.mu.Lock()
		ev := env.publisher.ev
		env.publisher.mu.Unlock()
		if ev != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("未在时限内结算")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestBattleActorReconnectRejectsOutsider 验证补帧按参战名单复核：局外人被拒、参战玩家放行。
func TestBattleActorReconnectRejectsOutsider(t *testing.T) {
	env := newBattleEnv(t)
	ctx := context.Background()
	_, err := env.ask(ctx, &battlev1.CreateBattleRequest{MatchId: "m-1", PlayerIds: []string{"p-a"}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// 局外人补帧被拒（INVALID_TOKEN，语义与 gateway 现状对外一致）。
	_, err = env.ask(ctx, &battlev1.ReconnectReq{PlayerId: "p-x", LastSeenFrame: 0})
	if atlaserrors.Reason(err) != "INVALID_TOKEN" {
		t.Fatalf("局外人补帧应被拒 INVALID_TOKEN, got %v", err)
	}
	// 参战玩家补帧放行。
	if _, err = env.ask(ctx, &battlev1.ReconnectReq{PlayerId: "p-a", LastSeenFrame: 0}); err != nil {
		t.Fatalf("reconnect member: %v", err)
	}
}

// TestLockstepType 验证战斗 ID 派生的 lockstep 类型名合法且唯一。
func TestLockstepType(t *testing.T) {
	a := lockstepType("b-1")
	if a != lockstepType("b-1") {
		t.Fatal("同 ID 派生应确定一致")
	}
	if b := lockstepType("b-2"); a == b {
		t.Fatal("不同 ID 派生应不同")
	}
	// 任意战斗 ID（含非法字符）都映射到合法 PID 类型段。
	for _, id := range []string{"b-1", "b-1234abcd56ef", "weird:id"} {
		if _, err := types.NewPID(lockstepType(id), "x"); err != nil {
			t.Fatalf("派生类型非法（%q）: %v", id, err)
		}
	}
}

// TestHashBytes 验证状态哈希编码往返。
func TestHashBytes(t *testing.T) {
	for _, h := range []uint64{0, 1, 0xdeadbeef, 1<<63 + 42} {
		b := hashBytes(h)
		if len(b) != 8 {
			t.Fatalf("哈希编码长度 = %d, want 8", len(b))
		}
		var back uint64
		for _, c := range b {
			back = back<<8 | uint64(c)
		}
		if back != h {
			t.Fatalf("哈希往返 %d → %d", h, back)
		}
	}
}

// TestSessionMeta 验证会话元信息字段。
func TestSessionMeta(t *testing.T) {
	meta := sessionMeta("b-1", int64(100*time.Millisecond))
	if meta.GetSessionId() != "b-1" || meta.GetMaxPlayers() != 2 || meta.GetTickMillis() != 100 ||
		meta.GetMode() != locksteppb.LockstepMode_LOCKSTEP_MODE_SERVER_AUTHORITATIVE {
		t.Fatalf("会话元信息不符: %+v", meta)
	}
}

// TestStateName 验证战斗状态描述。
func TestStateName(t *testing.T) {
	if got := stateName(false); got != "running" {
		t.Fatalf("stateName(false) = %q, want running", got)
	}
	if got := stateName(true); got != "settled" {
		t.Fatalf("stateName(true) = %q, want settled", got)
	}
}
