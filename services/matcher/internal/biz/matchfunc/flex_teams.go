package matchfunc

import (
	"context"
	"fmt"
	"sort"

	"github.com/huangyuCN/atlas/matchmaker"
)

// FlexTeams 将候选票（任意大小 1..K 人的 solo/整队票）组合成总人数相等的
// 两支队伍，两侧平均等级差 ≤ MaxGap；party 成员同侧由整队票天然保证。
// 确定性、可重试：不依赖外部状态，同输入同输出。
type FlexTeams struct {
	// MaxGap 是两侧平均等级的最大允许差（含）。
	MaxGap int32
	// MaxSide 是单侧人数上限（≥1；T=1 即 1v1 特例）。
	MaxSide int
}

// Run 实现 matchmaker.MatchFunction：
// 优先成型「本批候选能容纳的最大对局」——单侧人数 T 自大到小尝试
// （全部玩家参与的大对局优先于小对局，避免不满员 party 长期饿死）。
func (f FlexTeams) Run(_ context.Context, candidates []matchmaker.Candidate) ([]matchmaker.Match, error) {
	if len(candidates) < 2 || f.MaxSide <= 0 {
		return nil, nil
	}
	// 确定性排序：平均等级升序，等级相同按 ticket ID 升序。
	sorted := append([]matchmaker.Ticket(nil), ticketsOf(candidates)...)
	sort.SliceStable(sorted, func(i, j int) bool {
		li, lj := avgLevel(sorted[i]), avgLevel(sorted[j])
		if li != lj {
			return li < lj
		}
		return sorted[i].ID < sorted[j].ID
	})
	totalPlayers := 0
	for _, t := range sorted {
		totalPlayers += len(t.PlayerIDs())
	}
	maxT := f.MaxSide
	if half := totalPlayers / 2; half < maxT {
		maxT = half
	}

	for t := maxT; t >= 1; t-- {
		a, ok := pickSide(sorted, t)
		if !ok {
			continue
		}
		rest := exclude(sorted, a)
		b, ok := pickSide(rest, t)
		if !ok {
			continue
		}
		la, lb := sideLevel(a, t), sideLevel(b, t)
		if absDiff(la, lb) > f.MaxGap {
			continue
		}
		return []matchmaker.Match{{
			ID: fmt.Sprintf("m-%s-%s", joinIDs(a), joinIDs(b)),
			Teams: []matchmaker.Team{
				{Name: "red", PlayerIDs: ticketPlayerIDs(a)},
				{Name: "blue", PlayerIDs: ticketPlayerIDs(b)},
			},
			Tickets: append(append([]matchmaker.Ticket(nil), a...), b...),
		}}, nil
	}
	return nil, nil
}

// pickSide 在候选中找「人数和恰为 size」的第一个组合（固定顺序 DFS，确定性）。
func pickSide(sorted []matchmaker.Ticket, size int) ([]matchmaker.Ticket, bool) {
	if len(sorted) == 0 {
		return nil, false
	}
	out := make([]matchmaker.Ticket, 0, len(sorted))
	if dfsSide(sorted, 0, size, &out) {
		return out, true
	}
	return nil, false
}

// dfsSide 从 index 起尝试把 ticket 依次加入组合，人数和恰为 size 即成功。
func dfsSide(sorted []matchmaker.Ticket, index, remain int, out *[]matchmaker.Ticket) bool {
	if remain == 0 {
		return len(*out) > 0
	}
	for i := index; i < len(sorted); i++ {
		size := len(sorted[i].PlayerIDs())
		if size > remain {
			continue
		}
		*out = append(*out, sorted[i])
		if dfsSide(sorted, i+1, remain-size, out) {
			return true
		}
		*out = (*out)[:len(*out)-1]
	}
	return false
}

// exclude 返回 sorted 中剔除 side 后的剩余票（保序）。
func exclude(sorted, side []matchmaker.Ticket) []matchmaker.Ticket {
	drop := make(map[string]bool, len(side))
	for _, t := range side {
		drop[t.ID] = true
	}
	rest := make([]matchmaker.Ticket, 0, len(sorted)-len(side))
	for _, t := range sorted {
		if !drop[t.ID] {
			rest = append(rest, t)
		}
	}
	return rest
}

// sideLevel 计算一侧组合的加权平均等级。
func sideLevel(side []matchmaker.Ticket, totalPlayers int) int32 {
	if totalPlayers <= 0 {
		return 0
	}
	sum := int64(0)
	for _, t := range side {
		n := len(t.PlayerIDs())
		sum += int64(avgLevel(t)) * int64(n)
	}
	return int32(sum / int64(totalPlayers))
}

// avgLevel 计算票内玩家的加权平均等级（缺 level 属性的玩家按 0 计）。
func avgLevel(t matchmaker.Ticket) int32 {
	n := len(t.PlayerIDs())
	if n == 0 {
		return 0
	}
	sum := int32(0)
	for _, p := range t.Players {
		if a, ok := p.Attributes["level"]; ok && a.Type == matchmaker.AttributeNumber {
			sum += int32(a.Number)
		}
	}
	return sum / int32(n)
}

// ticketsOf 提取候选的 ticket 列表。
func ticketsOf(candidates []matchmaker.Candidate) []matchmaker.Ticket {
	out := make([]matchmaker.Ticket, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.Ticket)
	}
	return out
}

// ticketPlayerIDs 汇总一组票的全部玩家 ID（保序）。
func ticketPlayerIDs(tickets []matchmaker.Ticket) []string {
	out := make([]string, 0, len(tickets))
	for _, t := range tickets {
		out = append(out, t.PlayerIDs()...)
	}
	return out
}

// joinIDs 拼接票 ID（Match ID 派生用，确定性）。
func joinIDs(tickets []matchmaker.Ticket) string {
	s := ""
	for _, t := range tickets {
		s += t.ID + ","
	}
	return s
}

// Validate 校验规则配置（Matchmaker 构造前调用）。
func (f FlexTeams) Validate() error {
	if f.MaxGap < 0 {
		return fmt.Errorf("matchfunc: MaxGap 不能为负")
	}
	if f.MaxSide <= 0 {
		return fmt.Errorf("matchfunc: MaxSide 必须为正")
	}
	return nil
}
