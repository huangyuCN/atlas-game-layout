// 多实例出票用例（阶段 3 批次 7）：出票时按 player_id 哈希从接入层实例列表里选一个实例，
// 逐人下发**该实例**的面列表。
//
//   - ① 同一玩家多次出票 ⇒ 同一地址（确定性哈希，跨对局/跨进程一致 ⇒ 数据报面粘性）；
//   - ② 不同玩家 ⇒ 分布到多个实例；
//   - ③ 列表只有一个 ⇒ 恒它（单实例行为与改造前一致）；
//   - ④ 票内容与签名不变（票里不含地址：换实例不改票）。
package actor

import (
	"context"
	"fmt"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
)

// testEdgeInstances 返回三个接入层实例（每实例三面）：地址含实例编号，便于按地址认出实例。
func testEdgeInstances() [][]*battlev1.EdgeEndpoint {
	mk := func(host string) []*battlev1.EdgeEndpoint {
		return []*battlev1.EdgeEndpoint{
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: host + ":7100"},
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: host + ":7101"},
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UDP, Address: host + ":7102"},
		}
	}
	return [][]*battlev1.EdgeEndpoint{mk("edge-1.example.com"), mk("edge-2.example.com"), mk("edge-3.example.com")}
}

// newTicketEnvWithInstances 起带多实例配置的战斗 actor（EdgeEndpoints 仍取首个实例，
// 覆盖「兼容字段与列表同时存在」的真实装配形态）。
func newTicketEnvWithInstances(t *testing.T, instances [][]*battlev1.EdgeEndpoint) *battleEnv {
	t.Helper()
	cfg := testBattleConfig()
	cfg.TicketKey = testTicketKey()
	cfg.TicketTTL = DefaultTicketTTL
	cfg.EdgeEndpoints = instances[0]
	cfg.EdgeInstances = instances
	return newBattleEnvWith(t, cfg)
}

// issueTickets 触发出票并返回回执。
func issueTickets(t *testing.T, env *battleEnv) *battlev1.IssueEntryTicketReply {
	t.Helper()
	raw, err := env.ask(context.Background(), &battlev1.IssueEntryTicketReq{BattleId: testBattleID})
	if err != nil {
		t.Fatalf("出票: %v", err)
	}
	reply, ok := raw.(*battlev1.IssueEntryTicketReply)
	if !ok {
		t.Fatalf("出票回执类型 %T 不符", raw)
	}
	return reply
}

// ticketEntryOf 取某玩家的票条目。
func ticketEntryOf(t *testing.T, reply *battlev1.IssueEntryTicketReply, playerID string) *battlev1.BattleTicketEntry {
	t.Helper()
	for _, entry := range reply.GetTickets() {
		if entry.GetPlayerId() == playerID {
			return entry
		}
	}
	t.Fatalf("回执里没有玩家 %s 的票", playerID)
	return nil
}

// instanceHost 断言 eps 恰好是某个配置实例的**完整**面列表，返回该实例的首面地址（实例标识）。
func instanceHost(t *testing.T, instances [][]*battlev1.EdgeEndpoint, eps []*battlev1.EdgeEndpoint) string {
	t.Helper()
	if len(eps) == 0 {
		t.Fatal("逐人下发的实例面列表为空（多实例下每个玩家都必须拿到自己实例的地址）")
	}
	for _, inst := range instances {
		if len(inst) != len(eps) {
			continue
		}
		same := true
		for i := range inst {
			if inst[i].GetTransport() != eps[i].GetTransport() || inst[i].GetAddress() != eps[i].GetAddress() {
				same = false
				break
			}
		}
		if same {
			return eps[0].GetAddress()
		}
	}
	t.Fatalf("逐人面列表 %v 不是任何配置实例的完整面列表（不得跨实例拼地址）", eps)
	return ""
}

// issueOne 起一个单人参战的对局实例并返回该玩家的逐人面列表（用新 env 模拟「另一次出票」）。
func issueOne(t *testing.T, instances [][]*battlev1.EdgeEndpoint, playerID string) []*battlev1.EdgeEndpoint {
	t.Helper()
	env := newTicketEnvWithInstances(t, instances)
	seedRoster(t, env, playerID)
	return ticketEntryOf(t, issueTickets(t, env), playerID).GetEndpoints()
}

// TestIssueEntryTicketStickyPerPlayer 覆盖 ①：同一玩家在不同对局实例里出票，落到**同一个**
// 接入层实例（哈希确定、跨进程一致 ⇒ 数据报面 flow 状态不跨实例漂移）。
func TestIssueEntryTicketStickyPerPlayer(t *testing.T) {
	instances := testEdgeInstances()
	for _, playerID := range []string{"p-a", "p-b", "p-c"} {
		first := instanceHost(t, instances, issueOne(t, instances, playerID))
		for i := 0; i < 3; i++ {
			if got := instanceHost(t, instances, issueOne(t, instances, playerID)); got != first {
				t.Fatalf("玩家 %s 第 %d 次出票落到 %s, 期望恒定 %s（哈希必须确定）", playerID, i+2, got, first)
			}
		}
	}
}

// TestIssueEntryTicketDistributesAcrossInstances 覆盖 ②④：不同玩家分布到多个实例，
// 且票内容/签名不变（票可解、玩家与对局与版本/过期时刻不变；票密文里不含地址）。
func TestIssueEntryTicketDistributesAcrossInstances(t *testing.T) {
	instances := testEdgeInstances()
	env := newTicketEnvWithInstances(t, instances)
	players := make([]string, 0, 30)
	for i := 0; i < 30; i++ {
		players = append(players, fmt.Sprintf("p-%02d", i))
	}
	seedRoster(t, env, players...)
	reply := issueTickets(t, env)

	used := make(map[string]int, len(instances))
	for _, entry := range reply.GetTickets() {
		used[instanceHost(t, instances, entry.GetEndpoints())]++

		tk, err := ticket.Decode(entry.GetTicket(), testTicketKey())
		if err != nil {
			t.Fatalf("玩家 %s 的票无法解出: %v", entry.GetPlayerId(), err)
		}
		if tk.PlayerID != entry.GetPlayerId() || tk.BattleID != testBattleID {
			t.Fatalf("票内身份/对局 = %s/%s, 期望 %s/%s（票内容不随实例选择变化）",
				tk.PlayerID, tk.BattleID, entry.GetPlayerId(), testBattleID)
		}
		if tk.Version != ticket.Version1 || tk.KID != 1 {
			t.Fatalf("票版本/KID = %d/%d, 期望 %d/1（签名格式不变）", tk.Version, tk.KID, ticket.Version1)
		}
		if got := tk.ExpiresAt.UnixMilli(); got != reply.GetExpiresAtUnixMs() {
			t.Fatalf("票过期 %d 与回执 expires_at_unix_ms %d 不一致", got, reply.GetExpiresAtUnixMs())
		}
	}
	if len(used) < 2 {
		t.Fatalf("30 个玩家只落到 %d 个实例（%v），期望分布到多个地址", len(used), used)
	}
	// 回执级 endpoints 是首个实例的面列表（旧客户端兜底口径，单实例下即全部）。
	for i, want := range instances[0] {
		ep := reply.GetEndpoints()[i]
		if ep.GetTransport() != want.GetTransport() || ep.GetAddress() != want.GetAddress() {
			t.Fatalf("回执级 endpoints[%d] = %s/%s, 期望首实例 %s/%s", i,
				ep.GetTransport(), ep.GetAddress(), want.GetTransport(), want.GetAddress())
		}
	}
}

// TestIssueEntryTicketSingleInstanceAlwaysIt 覆盖 ③：列表只有一个实例时，所有玩家都拿到它
// （与改造前行为一致：回执级 endpoints 与逐人 endpoints 都是这一份面列表）。
func TestIssueEntryTicketSingleInstanceAlwaysIt(t *testing.T) {
	single := [][]*battlev1.EdgeEndpoint{testEdgeEndpoints()}
	env := newTicketEnvWithInstances(t, single)
	seedRoster(t, env, "p-a", "p-b", "p-c")
	reply := issueTickets(t, env)

	for _, entry := range reply.GetTickets() {
		eps := entry.GetEndpoints()
		if len(eps) != len(testEdgeEndpoints()) {
			t.Fatalf("玩家 %s 的实例面数 = %d, 期望 %d", entry.GetPlayerId(), len(eps), len(testEdgeEndpoints()))
		}
		for i, want := range testEdgeEndpoints() {
			if eps[i].GetTransport() != want.GetTransport() || eps[i].GetAddress() != want.GetAddress() {
				t.Fatalf("玩家 %s 的 endpoints[%d] = %s/%s, 期望 %s/%s", entry.GetPlayerId(), i,
					eps[i].GetTransport(), eps[i].GetAddress(), want.GetTransport(), want.GetAddress())
			}
		}
	}
}
