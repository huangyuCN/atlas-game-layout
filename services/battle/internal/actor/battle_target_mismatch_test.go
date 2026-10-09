package actor

import (
	"sync/atomic"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/server"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"google.golang.org/protobuf/proto"
)

// assertNoActor 断言该对局在本节点没有被拉起（帧面在投递之前拦住 → 不触发懒激活）。
func assertNoActor(t *testing.T, env *battleEnv, uid string) {
	t.Helper()
	pid, err := types.NewPID("battle", uid)
	if err != nil {
		t.Fatalf("NewPID(%s): %v", uid, err)
	}
	if st, ok := env.rt.Stats(pid); ok {
		t.Fatalf("对局 %s 被拉起（state=%v）：帧面未在投递之前拦住", uid, st.State)
	}
}

// TestFrameOpsRejectsTicketPayloadMismatch 验证 P0-1 的验收面：帧票面 battle 与正文 battle_id
// 不一致时，帧面在**投递之前**拒绝（403 FRAME_TARGET_MISMATCH）——既不投给票面对局，也不懒激活
// 正文对局（否则一张合法票就能拉起任意 battle，已结束的对局也会被复活成空名单实例）。
func TestFrameOpsRejectsTicketPayloadMismatch(t *testing.T) {
	key := frameTestKey()
	cfg := testBattleConfig()
	cfg.TicketKey, cfg.TicketTTL, cfg.EdgeEndpoints = key, time.Minute, testEdgeEndpoints()
	env := newBattleEnvWith(t, cfg) // 装置默认对局 b-test01 已在
	reg := stream.NewRegistry()
	ops, err := server.NewFrameOps(localRT{env.rt}, stream.NewBridge(reg, nil), key)
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	// 合法未过期票：票面 battle = b-test02（本节点未开局的另一局）。
	slot := ticketSlot(t, key, "p-a", "b-test02", time.Minute)
	cases := []struct {
		name      string
		operation string
		req       proto.Message
	}{
		{"帧输入指向已存在的 b-test01", battlev1opclient.BattleServiceProtocolOps.SendFrameInput,
			&battlev1.FrameInputReq{BattleId: "b-test01",
				Input: &locksteppb.LockstepInput{FrameId: 1, Payload: []byte{1}}}},
		{"帧输入指向不存在的 b-ghost", battlev1opclient.BattleServiceProtocolOps.SendFrameInput,
			&battlev1.FrameInputReq{BattleId: "b-ghost",
				Input: &locksteppb.LockstepInput{FrameId: 1, Payload: []byte{1}}}},
		{"保活指向不存在的 b-ghost", battlev1opclient.BattleServiceProtocolOps.Ping,
			&battlev1.PingReq{BattleId: "b-ghost"}},
		{"入局指向不存在的 b-ghost", battlev1opclient.BattleServiceProtocolOps.JoinBattle,
			&battlev1.JoinBattleReq{BattleId: "b-ghost"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ops.Handle(frameSlotCtx(7, slot), entryOf(t, tc.operation), tc.req)
			if err == nil {
				t.Fatalf("目标不一致的帧请求被受理（应拒绝）")
			}
			se := atlaserrors.FromError(err)
			if se.Reason != frameops.ReasonTargetMismatch || se.Code != 403 {
				t.Fatalf("reason/code = %q/%d（err=%v），期望 %q/403",
					se.Reason, se.Code, err, frameops.ReasonTargetMismatch)
			}
		})
	}
	// 拒绝发生在投递之前：正文对局与票面对局都不得出现新 actor。
	assertNoActor(t, env, "b-ghost")
	assertNoActor(t, env, "b-test02")
	if _, ok := env.rt.Stats(env.pid); !ok {
		t.Fatal("票面之外的既有 actor（b-test01）被误伤")
	}
}

// settleRaceConn 模拟「身份解析到投递之间恰好结算」的竞态：第一次 Ended 问询（身份解析）
// 答未结束，之后的问询（投递前复核）答已结束。
type settleRaceConn struct {
	inner  server.FrameConn
	raced  atomic.Bool
	marked atomic.Bool // 是否已放行过一次「未结束」
}

// Record 实现 server.FrameConn：透传登记。
func (c *settleRaceConn) Record(playerID string, conn stream.Conn) { c.inner.Record(playerID, conn) }

// ReplayEnd 实现 server.FrameConn：透传补投。
func (c *settleRaceConn) ReplayEnd(playerID string, conn stream.Conn) bool {
	return c.inner.ReplayEnd(playerID, conn)
}

// Ended 实现 server.FrameConn：首次答否（解析期未结算），其后答是（投递前已结算）。
func (c *settleRaceConn) Ended(battleID string) bool {
	if c.inner.Ended(battleID) {
		return true
	}
	if c.marked.CompareAndSwap(false, true) {
		c.raced.Store(true)
		return false
	}
	return true
}

// TestFrameOpsEndedRecheckedBeforeDeliver 验证 P0-1 的第二条口径：身份解析之后、投递之前
// 会**再判一次**目标对局是否已结束。模拟解析答「未结束」、复核答「已结束」的竞态，
// 断言请求被 BATTLE_ENDED 拒绝且目标对局没有被懒激活。
func TestFrameOpsEndedRecheckedBeforeDeliver(t *testing.T) {
	key := frameTestKey()
	cfg := testBattleConfig()
	cfg.TicketKey, cfg.TicketTTL, cfg.EdgeEndpoints = key, time.Minute, testEdgeEndpoints()
	env := newBattleEnvWith(t, cfg)
	reg := stream.NewRegistry()
	conn := &settleRaceConn{inner: stream.NewBridge(reg, nil)}
	ops, err := server.NewFrameOps(localRT{env.rt}, conn, key)
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	slot := ticketSlot(t, key, "p-a", "b-race", time.Minute)
	_, err = ops.Handle(frameSlotCtx(7, slot), entryOf(t, battlev1opclient.BattleServiceProtocolOps.JoinBattle),
		&battlev1.JoinBattleReq{BattleId: "b-race"})
	if !errorv1.IsBattleEnded(err) {
		t.Fatalf("投递前复核未生效：err = %v（reason=%s），期望 BATTLE_ENDED", err, atlaserrors.Reason(err))
	}
	if !conn.raced.Load() {
		t.Fatal("身份解析未答「未结束」：竞态未被构造出来")
	}
	assertNoActor(t, env, "b-race")
}
