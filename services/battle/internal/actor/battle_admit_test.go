package actor

import (
	"context"
	"errors"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"google.golang.org/protobuf/proto"
)

// admit 调一次 Admit（matcher 成局后唯一的入场入口：开局 + 出票合并）。
func admit(t *testing.T, env *battleEnv, playerIDs ...string) *battlev1.IssueEntryTicketReply {
	t.Helper()
	raw, err := env.ask(context.Background(), &battlev1.AdmitBattleReq{
		MatchId:   "m-admit",
		PlayerIds: playerIDs,
		BattleId:  testBattleID,
	})
	if err != nil {
		t.Fatalf("开局+出票: %v", err)
	}
	reply, ok := raw.(*battlev1.IssueEntryTicketReply)
	if !ok {
		t.Fatalf("入场回执类型 %T 不符", raw)
	}
	return reply
}

// TestAdmitIssuesTicketsInOneCall 验证「开局 + 出票」一次调用完成：
// 回执自带逐人票（可解出本人 + 本局），且名单同时已登记（参战玩家可直接加入）。
func TestAdmitIssuesTicketsInOneCall(t *testing.T) {
	env := newTicketEnv(t)
	reply := admit(t, env, "p-a", "p-b")

	if got := len(reply.GetTickets()); got != 2 {
		t.Fatalf("票据张数 = %d, 期望 2（一次调用即出票）", got)
	}
	for _, entry := range reply.GetTickets() {
		tk, err := ticket.Decode(entry.GetTicket(), testTicketKey())
		if err != nil {
			t.Fatalf("玩家 %s 的票无法解出: %v", entry.GetPlayerId(), err)
		}
		if tk.PlayerID != entry.GetPlayerId() || tk.BattleID != testBattleID {
			t.Errorf("票内身份 = %s/%s, 期望 %s/%s", tk.PlayerID, tk.BattleID, entry.GetPlayerId(), testBattleID)
		}
	}
	// 名单与票同一次调用产生：参战玩家加入不再被「不在名单」拒绝。
	if _, err := env.askAs(context.Background(), "p-a", &battlev1.JoinBattleReq{BattleId: testBattleID}); err != nil {
		t.Fatalf("参战玩家加入失败（名单未随开局登记？）: %v", err)
	}
}

// TestAdmitIdempotent 验证同一局重复调用不重复出票：回执逐字段一致（票密文相同、
// 过期时刻不变），且随后 IssueEntryTicket 仍回执同一份（不重签）。
func TestAdmitIdempotent(t *testing.T) {
	env := newTicketEnv(t)
	first := admit(t, env, "p-a", "p-b")
	second := admit(t, env, "p-a", "p-b")
	if !proto.Equal(first, second) {
		t.Fatalf("重复调用回执不一致:\n首次 %v\n再次 %v", first, second)
	}
	raw, err := env.ask(context.Background(), &battlev1.IssueEntryTicketReq{BattleId: testBattleID})
	if err != nil {
		t.Fatalf("重取票: %v", err)
	}
	if got, ok := raw.(*battlev1.IssueEntryTicketReply); !ok || !proto.Equal(got, first) {
		t.Fatalf("重取票回执 = %v, 期望与首次出票一致 %v", raw, first)
	}
}

// TestAdmitSignFailureLeavesNoRoster 验证原子性：签票失败（密钥不合法）时
// 名单也不登记——不留「有名单无票」，随后出票仍按空名单失败。
func TestAdmitSignFailureLeavesNoRoster(t *testing.T) {
	cfg := testBattleConfig()
	cfg.TicketKey = []byte("too-short")
	cfg.TicketTTL = DefaultTicketTTL
	cfg.EdgeEndpoints = testEdgeEndpoints()
	env := newBattleEnvWith(t, cfg)

	_, err := env.ask(context.Background(), &battlev1.AdmitBattleReq{
		MatchId:   "m-admit",
		PlayerIds: []string{"p-a", "p-b"},
		BattleId:  testBattleID,
	})
	if err == nil {
		t.Fatal("密钥长度不符时应返回错误，实际为 nil")
	}
	if !errors.Is(err, ticket.ErrKeySize) && atlaserrors.Reason(err) != "INTERNAL" {
		t.Errorf("错误应可判定为密钥长度问题或 INTERNAL，实际 %v", err)
	}
	// 名单未登记 ⇒ 再取票仍是「名单为空」。
	if _, err := env.ask(context.Background(), &battlev1.IssueEntryTicketReq{BattleId: testBattleID}); atlaserrors.Reason(err) != "BATTLE_NOT_FOUND" {
		t.Fatalf("开局失败后取票 reason = %q, 期望 BATTLE_NOT_FOUND（名单不得残留）: %v", atlaserrors.Reason(err), err)
	}
	// 玩家也加不进该局（名单确实为空）。
	if _, err := env.askAs(context.Background(), "p-a", &battlev1.JoinBattleReq{BattleId: testBattleID}); err == nil {
		t.Fatal("参战名单为空时加入应被拒绝")
	}
}
