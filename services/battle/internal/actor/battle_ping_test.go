package actor

// battle_ping_test.go 是直连保活探针（battle.v1.BattleService/Ping）在 battle 侧的用例：
// 探针只让帧面保持活跃（活跃刷新由帧引擎在收包时自动完成，见 battle_ping.go），
// 不参与对局逻辑——不得进 lockstep 输入流、不得改参战名单/帧号/结算；Tell 语义（returns Empty）
// 即无回执，重复与乱序调用幂等。

import (
	"context"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/server"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
)

// pingOp 是保活探针的帧 op 名（路由表与帧面注册共用同一字面量）。
const pingOp = "/battle.v1.BattleService/Ping"

// pingState 查询战斗 actor 的对外状态（名单人数 / 帧号 / 状态描述，GetState 口径）。
func pingState(t *testing.T, env *battleEnv) *battlev1.GetStateReply {
	t.Helper()
	raw, err := env.ask(newTestContext(), &battlev1.GetStateReq{BattleId: "b-test01"})
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	reply, ok := raw.(*battlev1.GetStateReply)
	if !ok {
		t.Fatalf("GetState 回执类型 %T 不符", raw)
	}
	return reply
}

// TestFrameOpsPingIsTellWithoutReply 验证保活探针的真实帧面链路（带票 → frameops.Handle →
// 本地投递 → BattleActor.Ping）：Tell 不回帧（回执为 nil），连续调用幂等，且调用前后
// 参战名单与帧号不变（探针不参与对局逻辑）。
//
// 帧号靠「帧间隔取 1 小时」冻住：ticker 不推进，帧号唯一可能的变化来源就是探针自己，
// 故前后比对不依赖时序、不会偶发漂移。
func TestFrameOpsPingIsTellWithoutReply(t *testing.T) {
	key := frameTestKey()
	cfg := testBattleConfig()
	cfg.TickInterval = int64(time.Hour)
	cfg.TicketKey, cfg.TicketTTL, cfg.EdgeEndpoints = key, time.Minute, testEdgeEndpoints()
	env := newBattleEnvWith(t, cfg)
	if _, err := env.ask(newTestContext(), &battlev1.CreateBattleRequest{
		MatchId: "m-1", PlayerIds: []string{"p-a", "p-b"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	ops, err := server.NewFrameOps(localRT{env.rt}, stream.NewBridge(stream.NewRegistry(), nil), key)
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	slot := ticketSlot(t, key, "p-a", "b-test01", time.Minute)
	if _, err := ops.Handle(frameSlotCtx(7, slot),
		entryOf(t, "/battle.v1.BattleService/JoinBattle"), &battlev1.JoinBattleReq{BattleId: "b-test01"}); err != nil {
		t.Fatalf("帧 JoinBattle: %v", err)
	}
	before := pingState(t, env)
	if before.GetPlayerCount() != 1 {
		t.Fatalf("前置名单不符: %+v", before)
	}
	for i := 1; i <= 3; i++ {
		rep, err := ops.Handle(frameSlotCtx(7, slot), entryOf(t, pingOp), &battlev1.PingReq{BattleId: "b-test01"})
		if err != nil {
			t.Fatalf("第 %d 次 Ping: %v", i, err)
		}
		if rep != nil {
			t.Fatalf("第 %d 次 Ping 带回执 %T（returns Empty 即 Tell，必须无回执）", i, rep)
		}
	}
	after := pingState(t, env)
	if after.GetPlayerCount() != before.GetPlayerCount() || after.GetCurrentFrame() != before.GetCurrentFrame() ||
		after.GetState() != before.GetState() {
		t.Fatalf("Ping 改动了对局状态: before=%+v after=%+v", before, after)
	}
}

// TestBattleActorPingDoesNotDisturbPendingInput 验证探针与在途输入共存：探针插在「输入已上行、
// 尚未被帧聚合消费」的窗口里，不阻断也不改写真实输入（载荷照常进入帧广播），名单不变。
// 「探针不走输入路径」由实现保证（battle_ping.go 的方法体不碰 lockstep 会话），本用例守的是
// 两者混跑时的可观察行为——背景：SendFrameInput 会把载荷写进确定性输入流，探针若图省事走
// 同一条路，就会往输入日志里塞垃圾。
func TestBattleActorPingDoesNotDisturbPendingInput(t *testing.T) {
	cfg := testBattleConfig()
	cfg.MaxFrames = 1 << 20 // 抬高帧数上限：本用例不结算，避免 actor 自停干扰断言
	env := newBattleEnvWith(t, cfg)
	ctx := newTestContext()
	if _, err := env.ask(ctx, &battlev1.CreateBattleRequest{
		MatchId: "m-1", PlayerIds: []string{"p-a", "p-b"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, p := range []string{"p-a", "p-b"} {
		if _, err := env.askAs(ctx, p, &battlev1.JoinBattleReq{BattleId: "b-test01"}); err != nil {
			t.Fatalf("join %s: %v", p, err)
		}
	}
	// 帧号声明为下一帧：保证这条输入在探针之后才被聚合消费（在途窗口内插探针）。
	sendFrameInput(t, env, ctx, "p-a", pingState(t, env).GetCurrentFrame()+1, 7)
	for i := 1; i <= 3; i++ {
		if err := env.rt.Tell(ctx, env.pid, &battlev1.PingReq{BattleId: "b-test01"},
			core.WithSender(mustPlayerPID(t, "p-a"))); err != nil {
			t.Fatalf("第 %d 次 Ping: %v", i, err)
		}
	}
	waitInputPayload(t, env, "p-a", 7)
	if state := pingState(t, env); state.GetPlayerCount() != 2 {
		t.Fatalf("Ping 后名单被改动: %+v", state)
	}
}

// sendFrameInput 以指定玩家身份上行一条带载荷的帧输入（声明帧号 + 载荷字节）。
func sendFrameInput(t *testing.T, env *battleEnv, ctx context.Context, playerID string, frame uint64, payload byte) {
	t.Helper()
	err := env.rt.Tell(ctx, env.pid, &battlev1.FrameInputReq{BattleId: "b-test01",
		Input: &locksteppb.LockstepInput{FrameId: frame, PlayerId: playerID, Payload: []byte{payload}}},
		core.WithSender(mustPlayerPID(t, playerID)))
	if err != nil {
		t.Fatalf("帧输入 %s@%d: %v", playerID, frame, err)
	}
}

// waitInputPayload 轮询帧广播直至某条广播里出现该玩家的指定载荷字节（超时即失败）。
func waitInputPayload(t *testing.T, env *battleEnv, playerID string, payload byte) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		frames, _, _ := env.pusher.snapshots()
		for _, f := range frames[playerID] {
			for _, in := range f.GetInputs() {
				if in.GetPlayerId() == playerID && len(in.GetPayload()) == 1 && in.GetPayload()[0] == payload {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("未在时限内看到 %s 的载荷 %d 进入帧广播（在途输入被顶掉？）", playerID, payload)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
