// Package matchpush 按玩家构造成局开局通知（game.v1.MatchStartedNotify）。
//
// 入场票据是直连的身份凭据（持有即可在该局冒充该玩家），因此**绝不能群发同一份 payload**：
// 每个收件玩家只能拿到自己的那张票，扇出方（网关）遍历参战名单逐人构造、逐人下发。
//
// 本包是纯函数（无 I/O、无全局状态）：事件由 matcher 发布（接入层面列表与票据来自 battle 出票回执），
// 这里只做「成局事件 + 玩家 → 该玩家的通知」的映射——把「谁该收到哪张票」的判定集中一处，
// 避免扇出方各自实现导致漏票、错票或空票下发。
package matchpush

import (
	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
)

// NotifyFor 按玩家构造成局通知：battle_ticket 必须是**该玩家自己的那张**。
//
// 返回 ok=false（且 notify 为 nil）表示该玩家不在参战名单里，或名单中没有他的票
// （空票同样不算有票）——调用方应跳过该玩家，不得下发空票：空票连不上接入层，
// 只会把「服务端没配好」伪装成「客户端连不上」，比不下发更难排查。
//
// endpoints 面列表与对局字段照抄事件（面列表对所有人相同，**不按人裁剪**：
// 选哪个面是 SDK 的事；其非空由 battle 装配期配置校验保证）。
func NotifyFor(ev *matcherv1.MatchStartedEvent, playerID string) (*gamev1.MatchStartedNotify, bool) {
	if ev == nil || playerID == "" || !inRoster(ev.GetPlayerIds(), playerID) {
		return nil, false
	}
	raw, ok := ticketOf(ev.GetBattleTickets(), playerID)
	if !ok {
		return nil, false
	}
	return &gamev1.MatchStartedNotify{
		MatchId:      ev.GetMatchId(),
		BattleId:     ev.GetBattleId(),
		PlayerIds:    append([]string(nil), ev.GetPlayerIds()...), // 拷贝：通知与事件不共享底层数组
		Endpoints:    append([]*battlev1.EdgeEndpoint(nil), ev.GetBattleEndpoints()...),
		BattleTicket: raw,
	}, true
}

// inRoster 报告玩家是否在参战名单里（名单是事件权威来源，不在名单不予下发）。
func inRoster(playerIDs []string, playerID string) bool {
	for _, id := range playerIDs {
		if id == playerID {
			return true
		}
	}
	return false
}

// ticketOf 取出该玩家的票据密文：无票或空票都视为无票（空票不可用于连接）。
func ticketOf(entries []*battlev1.BattleTicketEntry, playerID string) ([]byte, bool) {
	for _, entry := range entries {
		if entry.GetPlayerId() == playerID && len(entry.GetTicket()) > 0 {
			return entry.GetTicket(), true
		}
	}
	return nil, false
}
