package actor

import (
	"context"
	"testing"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// TestPlayerActorMatchQueue 验证匹配域链路：未登录拒绝、登录后入队
// （属性取聚合根权威 level）、业务错误透传。
func TestPlayerActorMatchQueue(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, store, match := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 未登录入队拒绝（PLAYER_NOT_ONLINE）。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}); atlaserrors.Reason(err) != "PLAYER_NOT_ONLINE" {
		t.Fatalf("未登录入队应拒绝, got %v", err)
	}

	// 登录后入队：属性取聚合根权威 level（非客户端自报）。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}); err != nil {
		t.Fatalf("入队: %v", err)
	}
	agg, err := store.LoadPlayer(ctx, "p-test")
	if err != nil {
		t.Fatalf("LoadPlayer: %v", err)
	}
	match.mu.Lock()
	nEnters := len(match.enters)
	var call enterCall
	if nEnters > 0 {
		call = match.enters[0]
	}
	match.mu.Unlock()
	if nEnters != 1 {
		t.Fatalf("入队次数 = %d, want 1", nEnters)
	}
	if call.playerID != "p-test" || call.level != agg.Level || call.ruleset != "casual" {
		t.Fatalf("入队参数不符: %+v (聚合根 level=%d)", call, agg.Level)
	}
}

// TestPlayerActorCancelAndStatus 验证取消匹配（幂等转发）与状态查询（轮询兜底）。
func TestPlayerActorCancelAndStatus(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, match := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}

	// 取消匹配：幂等转发（回执 canceled 取 fake 预置值）。
	match.mu.Lock()
	match.canceled = true
	match.mu.Unlock()
	rep, err := rt.Ask(ctx, loginPID, &gamev1.CancelMatchReq{})
	if err != nil {
		t.Fatalf("取消: %v", err)
	}
	if !rep.(*gamev1.CancelMatchReply).GetCanceled() {
		t.Fatal("取消回执应为 true")
	}
	// 状态查询：转发回执（matched 态 battle_id 供重登恢复）。
	st, err := rt.Ask(ctx, loginPID, &gamev1.GetMatchStatusReq{})
	if err != nil {
		t.Fatalf("状态查询: %v", err)
	}
	got := st.(*gamev1.MatchStatusReply)
	if got.GetState() != matcherv1.MatchState_MATCH_STATE_WAITING || got.GetTicketId() != "t-1" || got.GetMatchId() != "m-1" || got.GetBattleId() != "b-1" {
		t.Fatalf("状态回执不符: %+v", got)
	}
	if len(match.statuses) != 1 || match.statuses[0] != "p-test" {
		t.Fatalf("状态查询参数不符: %v", match.statuses)
	}
}

// TestPlayerActorOnStopCancelsMatch 验证下线联动：登出自停时自动取消在队匹配。
func TestPlayerActorOnStopCancelsMatch(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, match := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}
	// 登出（Tell）→ actor 自停 → OnStop 取消匹配。
	if err := rt.Tell(ctx, loginPID, &gamev1.LogoutMsg{Reason: gamev1.LogoutReason_LOGOUT_REASON_LOGOUT}); err != nil {
		t.Fatalf("logout: %v", err)
	}
	// 断言「存在登录 actor 的取消记录」而非精确次数：
	// 注册临时 actor 自停同样触发幂等取消（真实 matcher 中为 no-op）。
	waitFor(t, 6*time.Second, func() bool {
		match.mu.Lock()
		defer match.mu.Unlock()
		for _, id := range match.cancels {
			if id == "p-test" {
				return true
			}
		}
		return false
	}, "下线未联动取消匹配: cancels=%v loginActorAlive=%v", match.cancelSnapshot(), actorAlive(rt, loginPID))
}

// TestPlayerActorParty 验证建队链路：未登录拒、建队快照（队长为唯一成员）、重复建队拒。
func TestPlayerActorParty(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, _ := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// 未登录建队拒绝。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.CreatePartyReq{}); atlaserrors.Reason(err) != "PLAYER_NOT_ONLINE" {
		t.Fatalf("未登录建队应拒绝, got %v", err)
	}

	// 登录后建队：快照含队长自身。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}
	rep, err := rt.Ask(ctx, loginPID, &gamev1.CreatePartyReq{})
	if err != nil {
		t.Fatalf("CreateParty: %v", err)
	}
	created := rep.(*gamev1.PartyReply)
	if created.GetPartyId() != "party-p-test" || created.GetLeaderId() != "p-test" || len(created.GetMembers()) != 1 {
		t.Fatalf("建队回执不符: %+v", created)
	}

	// 重复建队拒绝（ALREADY_IN_PARTY）。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.CreatePartyReq{}); atlaserrors.Reason(err) != "ALREADY_IN_PARTY" {
		t.Fatalf("重复建队应拒绝, got %v", err)
	}
}

// TestPlayerActorPartyJoinLeave 验证加入/离开链路：建队后真实离开、
// 重复离开幂等（空快照）、加入转发名册快照。
func TestPlayerActorPartyJoinLeave(t *testing.T) {
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
	// 建队（释放引用后离开幂等断言）。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.CreatePartyReq{}); err != nil {
		t.Fatalf("CreateParty: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LeavePartyReq{}); err != nil {
		t.Fatalf("LeaveParty: %v", err)
	}

	// 离开（幂等）：不在队离开回执空快照。
	lp, err := rt.Ask(ctx, loginPID, &gamev1.LeavePartyReq{})
	if err != nil {
		t.Fatalf("重复离开应幂等: %v", err)
	}
	if lp.(*gamev1.PartyReply).GetPartyId() != "" {
		t.Fatalf("不在队离开应回执空快照: %+v", lp)
	}

	// 加入队伍：转发 + 名册快照（Describe 回执）。
	jp, err := rt.Ask(ctx, loginPID, &gamev1.JoinPartyReq{PartyId: "party-x"})
	if err != nil {
		t.Fatalf("JoinParty: %v", err)
	}
	if jp.(*gamev1.PartyReply).GetPartyId() != "party-x" {
		t.Fatalf("加入回执名册不符: %+v", jp)
	}
}

// TestPlayerActorPartyQueue 验证整队入队链路：未组队拒绝、组队后整队入队转发
// ruleset、撮合域调用记录（加入/整队入队）。
func TestPlayerActorPartyQueue(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, match := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}

	// 未组队整队入队拒绝（NOT_IN_PARTY）。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.QueuePartyReq{Ruleset: "casual"}); atlaserrors.Reason(err) != "NOT_IN_PARTY" {
		t.Fatalf("未组队入队应拒绝, got %v", err)
	}

	// 组队后整队入队：转发 ruleset。
	if _, err := rt.Ask(ctx, loginPID, &gamev1.JoinPartyReq{PartyId: "party-y"}); err != nil {
		t.Fatalf("JoinParty y: %v", err)
	}
	qp, err := rt.Ask(ctx, loginPID, &gamev1.QueuePartyReq{Ruleset: "casual"})
	if err != nil {
		t.Fatalf("QueueParty: %v", err)
	}
	if qp.(*gamev1.PartyQueueReply).GetTicketId() != "t-party-1" {
		t.Fatalf("整队入队回执不符: %+v", qp)
	}

	match.mu.Lock()
	defer match.mu.Unlock()
	if len(match.created) != 0 || len(match.joined) != 1 || len(match.queued) != 1 || match.queued[0] != "party-y|casual" {
		t.Fatalf("组域调用记录不符: created=%v joined=%v queued=%v", match.created, match.joined, match.queued)
	}
}

// TestPlayerActorOnStopLeavesParty 验证下线联动：登出自停时自动离开队伍
// （撮合域 LeaveParty 会联动取消在匹配中的整队票）。
func TestPlayerActorOnStopLeavesParty(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, match := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.CreatePartyReq{}); err != nil {
		t.Fatalf("CreateParty: %v", err)
	}
	// 登出（Tell）→ actor 自停 → OnStop 离队。
	if err := rt.Tell(ctx, loginPID, &gamev1.LogoutMsg{Reason: gamev1.LogoutReason_LOGOUT_REASON_LOGOUT}); err != nil {
		t.Fatalf("logout: %v", err)
	}
	waitFor(t, 6*time.Second, func() bool {
		match.mu.Lock()
		defer match.mu.Unlock()
		for _, rec := range match.left {
			if rec == "party-p-test|p-test" {
				return true
			}
		}
		return false
	}, "下线未联动离队: left=%v", match.cancelSnapshot())
}

// TestPlayerActorLogoutStopsOnAnyReason 验证登出停机语义（会话裁决收敛到 Gateway 后）：
// game 侧不持 token 副本，LogoutMsg 不再携带 token——任意 reason（含 SESSION_EXPIRED）
// 收到即停止 actor 并联动 OnStop（离队/取消匹配/落库）；接管裁决由 Gateway 单点完成。
func TestPlayerActorLogoutStopsOnAnyReason(t *testing.T) {
	ctx := context.Background()
	rt, regPID, loginPID, _, match := newActorEnv(t, 0, 0)
	if _, err := rt.Spawn(ctx, regPID); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := rt.Ask(ctx, regPID, &gamev1.RegisterReq{Account: "alice", Password: "pw"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.LoginReq{PlayerId: "p-test", Password: "pw", Token: "t1"}); err != nil {
		t.Fatalf("login: %v", err)
	}
	if _, err := rt.Ask(ctx, loginPID, &gamev1.CreatePartyReq{}); err != nil {
		t.Fatalf("CreateParty: %v", err)
	}

	// 异常下线（SESSION_EXPIRED，无 token）：收到即停机 → OnStop 离队。
	if err := rt.Tell(ctx, loginPID, &gamev1.LogoutMsg{
		Reason: gamev1.LogoutReason_LOGOUT_REASON_SESSION_EXPIRED,
	}); err != nil {
		t.Fatalf("logout expired: %v", err)
	}
	waitFor(t, 6*time.Second, func() bool {
		match.mu.Lock()
		defer match.mu.Unlock()
		for _, rec := range match.left {
			if rec == "party-p-test|p-test" {
				return true
			}
		}
		return false
	}, "异常下线未联动离队: left=%v", match.cancelSnapshot())
}

// actorAlive 返回 actor 是否存活（测试报错现场用）。
func actorAlive(rt *core.LocalRuntime, pid types.PID) bool {
	_, alive := rt.Stats(pid)
	return alive
}
