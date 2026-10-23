package matchfunc

import (
	"testing"

	"github.com/huangyuCN/atlas/matchmaker"
)

// levelTicket 构造带等级属性的票（players 数即队伍人数）。
func levelTicket(id string, level int32, n int) matchmaker.Ticket {
	players := make([]matchmaker.Player, 0, n)
	for i := 0; i < n; i++ {
		players = append(players, matchmaker.Player{
			ID:         id + "-p" + string(rune('a'+i)),
			Attributes: map[string]matchmaker.Attribute{"level": {Type: matchmaker.AttributeNumber, Number: float64(level)}},
		})
	}
	return matchmaker.Ticket{ID: id, Players: players}
}

// TestFlexTeams_SoloPairing 验证 T=1 特例（两张单人票配 1v1，与旧 LevelClose 语义一致）。
func TestFlexTeams_SoloPairing(t *testing.T) {
	f := FlexTeams{MaxGap: 3, MaxSide: 5}
	ms, err := f.Run(t.Context(), []matchmaker.Candidate{
		{Ticket: levelTicket("t-a", 10, 1)},
		{Ticket: levelTicket("t-b", 12, 1)},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ms) != 1 {
		t.Fatalf("want 1 match, got %d", len(ms))
	}
	if len(ms[0].Teams[0].PlayerIDs) != 1 || len(ms[0].Teams[1].PlayerIDs) != 1 {
		t.Fatalf("1v1 人数不符: %+v", ms[0].Teams)
	}
}

// TestFlexTeams_PartyVsSolos 验证整队票与两张单人票配成 2v2（T=2 组合补位）。
func TestFlexTeams_PartyVsSolos(t *testing.T) {
	f := FlexTeams{MaxGap: 3, MaxSide: 5}
	ms, err := f.Run(t.Context(), []matchmaker.Candidate{
		{Ticket: levelTicket("t-p", 10, 2)}, // party（2 人）
		{Ticket: levelTicket("t-c", 11, 1)},
		{Ticket: levelTicket("t-d", 9, 1)},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ms) != 1 {
		t.Fatalf("want 1 match, got %d", len(ms))
	}
	for _, team := range ms[0].Teams {
		if len(team.PlayerIDs) != 2 {
			t.Fatalf("2v2 每侧应 2 人: %+v", team)
		}
	}
	// party 成员必然同侧。
	partyIDs := map[string]bool{"t-p-pa": true, "t-p-pb": true}
	red := ms[0].Teams[0].PlayerIDs
	sameSide := (partyIDs[red[0]] && partyIDs[red[1]]) || (!partyIDs[red[0]] && !partyIDs[red[1]])
	if !sameSide {
		t.Fatalf("party 成员被拆队: %+v", ms[0].Teams)
	}
}

// TestFlexTeams_PartyVsParty 验证两张整队票直接配对。
func TestFlexTeams_PartyVsParty(t *testing.T) {
	f := FlexTeams{MaxGap: 3, MaxSide: 5}
	ms, err := f.Run(t.Context(), []matchmaker.Candidate{
		{Ticket: levelTicket("t-a", 10, 2)},
		{Ticket: levelTicket("t-b", 12, 2)},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ms) != 1 {
		t.Fatalf("want 1 match, got %d", len(ms))
	}
	if ms[0].ID == "" {
		t.Fatal("match ID 为空")
	}
}

// TestFlexTeams_GapTooWide 验证等级差超限不成局。
func TestFlexTeams_GapTooWide(t *testing.T) {
	f := FlexTeams{MaxGap: 3, MaxSide: 5}
	ms, err := f.Run(t.Context(), []matchmaker.Candidate{
		{Ticket: levelTicket("t-a", 10, 1)},
		{Ticket: levelTicket("t-b", 99, 1)},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ms) != 0 {
		t.Fatalf("等级差超限不应成局: %+v", ms)
	}
}

// soloCandidates 构造 n 张同等级单人票（等级相同 ⟹ 排序退化为按 ID 升序，确定且可控）。
func soloCandidates(n int) []matchmaker.Candidate {
	out := make([]matchmaker.Candidate, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, matchmaker.Candidate{Ticket: levelTicket("t-"+string(rune('a'+i)), 10, 1)})
	}
	return out
}

// matchedTicketIDs 汇总一次 Run 结果里出现的票 ID（按出现顺序）与每局票数。
func matchedTicketIDs(ms []matchmaker.Match) ([]string, []int) {
	ids := make([]string, 0, len(ms)*2)
	sizes := make([]int, 0, len(ms))
	for _, m := range ms {
		sizes = append(sizes, len(m.Tickets))
		for _, tk := range m.Tickets {
			ids = append(ids, tk.ID)
		}
	}
	return ids, sizes
}

// TestFlexTeams_RunFormsMultipleMatchesPerCall 验证**一轮 Run 能成多局**：
// 10 张单人票在 MaxSide=1（1v1）下应成 5 局——旧实现每轮恒定只返回 1 局。
func TestFlexTeams_RunFormsMultipleMatchesPerCall(t *testing.T) {
	f := FlexTeams{MaxGap: 3, MaxSide: 1}
	ms, err := f.Run(t.Context(), soloCandidates(10))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ms) != 5 {
		t.Fatalf("want 5 matches（10 张单人票 1v1）, got %d", len(ms))
	}
	ids, sizes := matchedTicketIDs(ms)
	for i, n := range sizes {
		if n != 2 {
			t.Fatalf("第 %d 局票数 = %d, 期望 2（1v1）", i, n)
		}
	}
	if len(ids) != 10 {
		t.Fatalf("成局票数合计 = %d, 期望 10（不重复配对）", len(ids))
	}
}

// TestFlexTeams_MultipleMatchesPartitionAllTickets 验证同一批多次成型后
// **不重复配对、不漏配**：12 张单人票在 MaxSide=2（2v2）下成 3 局且票恰好各出现一次。
func TestFlexTeams_MultipleMatchesPartitionAllTickets(t *testing.T) {
	f := FlexTeams{MaxGap: 3, MaxSide: 2}
	ms, err := f.Run(t.Context(), soloCandidates(12))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ms) != 3 {
		t.Fatalf("want 3 matches（12 张单人票 2v2）, got %d", len(ms))
	}
	seen := make(map[string]bool, 12)
	for _, id := range func() []string { ids, _ := matchedTicketIDs(ms); return ids }() {
		if seen[id] {
			t.Fatalf("票 %s 出现在多局里（重复配对）", id)
		}
		seen[id] = true
	}
	if len(seen) != 12 {
		t.Fatalf("成局票去重后 = %d, 期望 12（漏配）", len(seen))
	}
}

// TestFlexTeams_MultipleMatchesLeaveUnmatchableRest 验证余票语义：11 张单人票在
// MaxSide=1 下成 5 局，剩 1 张**原样留在池里**（既不被吞掉也不被重复匹配）。
func TestFlexTeams_MultipleMatchesLeaveUnmatchableRest(t *testing.T) {
	f := FlexTeams{MaxGap: 3, MaxSide: 1}
	cands := soloCandidates(11)
	ms, err := f.Run(t.Context(), cands)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ms) != 5 {
		t.Fatalf("want 5 matches（11 张单人票 1v1 余 1 张）, got %d", len(ms))
	}
	ids, _ := matchedTicketIDs(ms)
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		seen[id] = true
	}
	rest := 0
	for _, c := range cands {
		if !seen[c.Ticket.ID] {
			rest++
		}
	}
	if rest != 1 {
		t.Fatalf("余票 = %d, 期望 1（未成局票必须原样留下）", rest)
	}
}

// TestFlexTeams_BigMatchFirst 验证公平性语义保持不变：同一批优先成「能容纳的最大对局」
// （10 张单人票 + MaxSide=5 ⟹ 首局 5v5 吃掉全部，不得拆成 5 个 1v1）。
func TestFlexTeams_BigMatchFirst(t *testing.T) {
	f := FlexTeams{MaxGap: 3, MaxSide: 5}
	ms, err := f.Run(t.Context(), soloCandidates(10))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ms) != 1 {
		t.Fatalf("want 1 match（最大对局优先）, got %d", len(ms))
	}
	if len(ms[0].Teams[0].PlayerIDs) != 5 || len(ms[0].Teams[1].PlayerIDs) != 5 {
		t.Fatalf("首局应为 5v5: %+v", ms[0].Teams)
	}
}

// TestFlexTeams_Deterministic 验证同输入同输出（可重试契约）。
func TestFlexTeams_Deterministic(t *testing.T) {
	f := FlexTeams{MaxGap: 3, MaxSide: 5}
	cands := []matchmaker.Candidate{
		{Ticket: levelTicket("t-a", 10, 2)},
		{Ticket: levelTicket("t-b", 11, 1)},
		{Ticket: levelTicket("t-c", 9, 1)},
	}
	first, err := f.Run(t.Context(), cands)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	second, err := f.Run(t.Context(), cands)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(first) != 1 || len(second) != 1 || first[0].ID != second[0].ID {
		t.Fatalf("同输入应同输出: %v vs %v", first[0].ID, second[0].ID)
	}
}
