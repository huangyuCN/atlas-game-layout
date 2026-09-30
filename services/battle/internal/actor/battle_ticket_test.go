package actor

import (
	"context"
	"errors"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

const (
	// testBattleID 是测试对局 ID（票据 battle_id 必须与之一致）。
	testBattleID = "b-test01"
)

// testEdgeEndpoints 返回三面接入层地址列表（回执 endpoints 的唯一来源是 battle 配置；
// 用三面而非单面，才能断言回执的面与地址都逐字来自配置）。
func testEdgeEndpoints() []*battlev1.EdgeEndpoint {
	return []*battlev1.EdgeEndpoint{
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.example.com:7100"},
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: "edge.example.com:7101"},
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UDP, Address: "edge.example.com:7102"},
	}
}

// testTicketKey 返回 32 字节测试密钥（与生产同形；真实密钥由配置注入）。
func testTicketKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

// newTicketEnv 起带出票配置的战斗 actor（出票用例共用）。
func newTicketEnv(t *testing.T) *battleEnv {
	t.Helper()
	cfg := testBattleConfig()
	cfg.TicketKey = testTicketKey()
	cfg.TicketTTL = DefaultTicketTTL
	cfg.EdgeEndpoints = testEdgeEndpoints()
	return newBattleEnvWith(t, cfg)
}

// seedRoster 开局登记参战名单（出票前置）。
func seedRoster(t *testing.T, env *battleEnv, playerIDs ...string) {
	t.Helper()
	reply, err := env.ask(context.Background(), &battlev1.CreateBattleRequest{
		MatchId:   "m-ticket",
		PlayerIds: playerIDs,
	})
	if err != nil {
		t.Fatalf("开局: %v", err)
	}
	if _, ok := reply.(*battlev1.CreateBattleReply); !ok {
		t.Fatalf("开局回执类型 %T 不符", reply)
	}
}

// TestIssueEntryTicketPerPlayer 验证逐人签票：名单里每个玩家恰好一张，
// 每票可被 ticket.Decode 解出本人 player_id 与本局 battle_id、版本/KID 正确、
// 票的过期时刻与回执 expires_at_unix_ms 一致、有效期等于配置 TTL。
func TestIssueEntryTicketPerPlayer(t *testing.T) {
	env := newTicketEnv(t)
	seedRoster(t, env, "p-a", "p-b")

	raw, err := env.ask(context.Background(), &battlev1.IssueEntryTicketReq{BattleId: testBattleID})
	if err != nil {
		t.Fatalf("出票: %v", err)
	}
	reply, ok := raw.(*battlev1.IssueEntryTicketReply)
	if !ok {
		t.Fatalf("出票回执类型 %T 不符", raw)
	}
	if got := reply.GetEndpoints(); len(got) != 3 {
		t.Fatalf("回执面数 = %d, 期望 3（逐字来自 battle 配置 edge_endpoints）", len(got))
	}
	for i, want := range testEdgeEndpoints() {
		ep := reply.GetEndpoints()[i]
		if ep.GetTransport() != want.GetTransport() || ep.GetAddress() != want.GetAddress() {
			t.Errorf("endpoints[%d] = %s/%s, 期望 %s/%s（配置是地址的唯一来源）", i,
				ep.GetTransport(), ep.GetAddress(), want.GetTransport(), want.GetAddress())
		}
	}
	if got := len(reply.GetTickets()); got != 2 {
		t.Fatalf("票据张数 = %d, 期望 2（名单每人恰好一张）", got)
	}
	seen := make(map[string]bool, 2)
	for _, entry := range reply.GetTickets() {
		tk, derr := ticket.Decode(entry.GetTicket(), testTicketKey())
		if derr != nil {
			t.Fatalf("玩家 %s 的票无法解出: %v", entry.GetPlayerId(), derr)
		}
		if tk.PlayerID != entry.GetPlayerId() {
			t.Errorf("票内 player_id = %q, 名单条目 = %q", tk.PlayerID, entry.GetPlayerId())
		}
		if tk.BattleID != testBattleID {
			t.Errorf("票内 battle_id = %q, 期望 %q", tk.BattleID, testBattleID)
		}
		if tk.Version != ticket.Version1 || tk.KID != 1 {
			t.Errorf("票版本/KID = %d/%d, 期望 %d/1", tk.Version, tk.KID, ticket.Version1)
		}
		if got := tk.ExpiresAt.UnixMilli(); got != reply.GetExpiresAtUnixMs() {
			t.Errorf("票过期 %d 与回执 expires_at_unix_ms %d 不一致", got, reply.GetExpiresAtUnixMs())
		}
		if got := tk.ExpiresAt.Sub(tk.IssuedAt); got != DefaultTicketTTL {
			t.Errorf("票有效期 = %v, 期望配置 TTL %v", got, DefaultTicketTTL)
		}
		if drift := time.Since(tk.IssuedAt); drift < 0 || drift > 5*time.Second {
			t.Errorf("票签发时刻偏移 = %v, 期望接近当前时刻", drift)
		}
		seen[tk.PlayerID] = true
	}
	if len(seen) != 2 || !seen["p-a"] || !seen["p-b"] {
		t.Errorf("票据名单 = %v, 期望恰好覆盖 p-a/p-b", seen)
	}
}

// TestIssueEntryTicketEmptyRoster 验证名单为空（未开局/对局不存在）返回既有结构化错误
// （BATTLE_NOT_FOUND；不新造 reason 字面量）。
func TestIssueEntryTicketEmptyRoster(t *testing.T) {
	env := newTicketEnv(t)
	raw, err := env.ask(context.Background(), &battlev1.IssueEntryTicketReq{BattleId: testBattleID})
	if err == nil {
		t.Fatalf("空名单出票应失败，实际回执 %T", raw)
	}
	if got := atlaserrors.Reason(err); got != "BATTLE_NOT_FOUND" {
		t.Errorf("错误 reason = %q, 期望 BATTLE_NOT_FOUND（%v）", got, err)
	}
}

// TestIssueEntryTicketShortKeyFails 验证密钥装配异常时 actor 层不静默签票（兜底装配期校验）。
func TestIssueEntryTicketShortKeyFails(t *testing.T) {
	cfg := testBattleConfig()
	cfg.TicketKey = []byte("too-short")
	cfg.TicketTTL = DefaultTicketTTL
	cfg.EdgeEndpoints = testEdgeEndpoints()
	env := newBattleEnvWith(t, cfg)
	seedRoster(t, env, "p-a")

	_, err := env.ask(context.Background(), &battlev1.IssueEntryTicketReq{BattleId: testBattleID})
	if err == nil {
		t.Fatal("密钥长度不符时应返回错误，实际为 nil")
	}
	if !errors.Is(err, ticket.ErrKeySize) && atlaserrors.Reason(err) != "INTERNAL" {
		t.Errorf("错误应可判定为密钥长度问题或 INTERNAL，实际 %v", err)
	}
}
