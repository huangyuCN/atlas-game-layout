// 入场域：开局与出票合并成**一次** actor 调用（Admit，INTERNAL）。
//
// 为什么合并（12288 入场崩塌的根因）：matcher 过去对每一局先发 CreateBattle 懒激活、
// 再发 IssueEntryTicket 取票，两跳跨进程 RPC 的尾延迟在万级并发下叠加，任一跳超时即
// 「既无票也无成局事件」；合并后两件事在同一次调用里完成，且原子化——签票失败不登记名单。

package actor

import (
	"sort"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas/contrib/actor/core"
)

// Admit 实现 battlev1actor.BattleService：开局 + 出票（matcher 成局后的唯一入场入口）。
//
// 原子性：签票是纯计算（不改 actor 状态），成功后才登记 match_id 与参战名单——
// 签票失败即整体不产生，不会留下「有名单无票」；票只经成功回执下发，不会「有票无名单」。
// 幂等：首次成功即冻结回执（票密文与过期时刻都不变），重复调用原样返回——名单不翻倍、
// 不重复出票，调用方超时重试安全。
func (b *BattleActor) Admit(_ core.ActorContext, req *battlev1.AdmitBattleReq) (*battlev1.IssueEntryTicketReply, error) {
	if b.admission != nil {
		return b.admission, nil
	}
	players := b.mergedRoster(req.GetPlayerIds())
	if len(players) == 0 {
		return nil, errorv1.ErrInvalidParams("开局失败：参战名单为空")
	}
	reply, err := b.signTickets(players)
	if err != nil {
		return nil, err
	}
	b.matchID = req.GetMatchId()
	for _, playerID := range players {
		b.players[playerID] = struct{}{}
	}
	b.admission = reply
	return reply, nil
}

// mergedRoster 返回「已登记名单 ∪ 本次请求名单」的稳定快照（去重 + 升序）：
// 出票必须覆盖登记过的全部参战者，故取并集而不是只取本次请求。
func (b *BattleActor) mergedRoster(playerIDs []string) []string {
	seen := make(map[string]struct{}, len(b.players)+len(playerIDs))
	out := make([]string, 0, len(b.players)+len(playerIDs))
	for playerID := range b.players {
		seen[playerID] = struct{}{}
		out = append(out, playerID)
	}
	for _, playerID := range playerIDs {
		if playerID == "" {
			continue
		}
		if _, ok := seen[playerID]; ok {
			continue
		}
		seen[playerID] = struct{}{}
		out = append(out, playerID)
	}
	sort.Strings(out)
	return out
}
