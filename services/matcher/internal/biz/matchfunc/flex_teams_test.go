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
