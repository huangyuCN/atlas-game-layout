// Package matchfunc 提供示例撮合规则：等级相近 1v1（MatchFunction）。
package matchfunc

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas/matchmaker"
)

// LevelClose 将候选集中「等级差 ≤ MaxGap」的最早两人配成 1v1。
// 确定性、可重试：不依赖外部状态，遍历顺序即候选集顺序。
type LevelClose struct {
	// MaxGap 是允许的最大等级差（含）。
	MaxGap int32
}

// Run 实现 matchmaker.MatchFunction。
func (f LevelClose) Run(_ context.Context, candidates []matchmaker.Candidate) ([]matchmaker.Match, error) {
	if len(candidates) < 2 {
		return nil, nil
	}
	for i := 0; i < len(candidates); i++ {
		for j := i + 1; j < len(candidates); j++ {
			a, b := candidates[i].Ticket, candidates[j].Ticket
			la, okA := levelOf(a)
			lb, okB := levelOf(b)
			if !okA || !okB {
				continue
			}
			if absDiff(la, lb) > f.MaxGap {
				continue
			}
			// match ID 由双方 ticket 派生：确定性（MatchFunction 可重试要求同输入同输出）。
			return []matchmaker.Match{{
				ID: fmt.Sprintf("m-%s-%s", a.ID, b.ID),
				Teams: []matchmaker.Team{
					{Name: "red", PlayerIDs: a.PlayerIDs()},
					{Name: "blue", PlayerIDs: b.PlayerIDs()},
				},
				Tickets: []matchmaker.Ticket{a, b},
			}}, nil
		}
	}
	return nil, nil
}

// levelOf 提取 ticket 首个玩家的等级属性（attribute 名 "level"）。
func levelOf(t matchmaker.Ticket) (int32, bool) {
	for _, p := range t.Players {
		if a, ok := p.Attributes["level"]; ok && a.Type == matchmaker.AttributeNumber {
			return int32(a.Number), true
		}
	}
	return 0, false
}

// absDiff 返回两数的绝对差。
func absDiff(a, b int32) int32 {
	if a >= b {
		return a - b
	}
	return b - a
}

// Validate 校验规则配置（Matchmaker 构造前调用）。
func (f LevelClose) Validate() error {
	if f.MaxGap < 0 {
		return fmt.Errorf("matchfunc: MaxGap 不能为负")
	}
	return nil
}
