package matchfunc

import (
	"context"
	"testing"

	"github.com/huangyuCN/atlas/matchmaker"
)

// mkCandidate 构造带等级属性的候选。
func mkCandidate(id string, level float64) matchmaker.Candidate {
	return matchmaker.Candidate{
		Ticket: matchmaker.Ticket{
			ID: "t-" + id,
			Players: []matchmaker.Player{{ID: "p-" + id, Attributes: map[string]matchmaker.Attribute{
				"level": {Type: matchmaker.AttributeNumber, Number: level},
			}}},
		},
	}
}

// TestLevelCloseMatch 验证等级差内成局与差外不成局。
func TestLevelCloseMatch(t *testing.T) {
	f := LevelClose{MaxGap: 2}

	// 空候选/单候选：不成局。
	if ms, err := f.Run(context.Background(), nil); err != nil || len(ms) != 0 {
		t.Fatalf("空候选: ms=%v err=%v", ms, err)
	}
	if ms, err := f.Run(context.Background(), []matchmaker.Candidate{mkCandidate("a", 10)}); err != nil || len(ms) != 0 {
		t.Fatalf("单候选: ms=%v err=%v", ms, err)
	}

	// 等级差内：成局（最早两人）。
	ms, err := f.Run(context.Background(), []matchmaker.Candidate{
		mkCandidate("a", 10), mkCandidate("b", 12), mkCandidate("c", 20),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ms) != 1 {
		t.Fatalf("成局数 = %d, want 1", len(ms))
	}
	m := ms[0]
	if len(m.Teams) != 2 {
		t.Fatalf("队伍数 = %d, want 2", len(m.Teams))
	}
	ids := append(append([]string(nil), m.Teams[0].PlayerIDs...), m.Teams[1].PlayerIDs...)
	if ids[0] != "p-a" || ids[1] != "p-b" {
		t.Fatalf("应配 p-a 与 p-b，实际 %v", ids)
	}
	if m.ID == "" {
		t.Fatal("match ID 不能为空")
	}

	// 全部超出等级差：不成局。
	ms, err = f.Run(context.Background(), []matchmaker.Candidate{
		mkCandidate("a", 10), mkCandidate("b", 20),
	})
	if err != nil || len(ms) != 0 {
		t.Fatalf("超差候选: ms=%v err=%v", ms, err)
	}
}

// TestLevelCloseDeterministic 验证同输入同输出（确定性，可重试）。
func TestLevelCloseDeterministic(t *testing.T) {
	f := LevelClose{MaxGap: 5}
	ins := []matchmaker.Candidate{mkCandidate("a", 10), mkCandidate("b", 14), mkCandidate("c", 11)}
	first, err := f.Run(context.Background(), ins)
	if err != nil {
		t.Fatalf("Run1: %v", err)
	}
	second, err := f.Run(context.Background(), ins)
	if err != nil {
		t.Fatalf("Run2: %v", err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("成局数异常: %d/%d", len(first), len(second))
	}
	a, b := first[0], second[0]
	if a.ID == "" || a.ID != b.ID {
		t.Fatalf("同输入应产生相同对局: %q vs %q", a.ID, b.ID)
	}
}

// TestLevelCloseMissingAttribute 验证缺失等级属性的候选被跳过。
func TestLevelCloseMissingAttribute(t *testing.T) {
	f := LevelClose{MaxGap: 2}
	cand := matchmaker.Candidate{Ticket: matchmaker.Ticket{
		ID: "t-x", Players: []matchmaker.Player{{ID: "p-x"}},
	}}
	ms, err := f.Run(context.Background(), []matchmaker.Candidate{cand, mkCandidate("a", 10)})
	if err != nil || len(ms) != 0 {
		t.Fatalf("缺属性候选应跳过: ms=%v err=%v", ms, err)
	}
}

// TestLevelCloseValidate 验证规则校验。
func TestLevelCloseValidate(t *testing.T) {
	if err := (LevelClose{MaxGap: -1}).Validate(); err == nil {
		t.Fatal("负 MaxGap 应报错")
	}
	if err := (LevelClose{MaxGap: 0}).Validate(); err != nil {
		t.Fatalf("MaxGap=0 应合法: %v", err)
	}
}
