// 出票域：按本局参战名单**逐人**签发直连入场票据（IssueEntryTicket，INTERNAL actor 方法）。
//
// 为什么票必须先于连接到达客户端（规格 §5）：直连后 JoinBattle 本身就走直连通道，
// 而「能连上」的前提是先有票——因此票只能随成局推送下发。battle 在出票时按名单逐人
// 加密（每票绑 player_id + battle_id），接入层与 battle 共持同一把 32 字节密钥
//（票据实现见 contrib/edge/ticket）。

package actor

import (
	"sort"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
)

// DefaultTicketTTL 是入场票据的默认有效期（规格 §3.1 建议 120s：
// 大于掉线重连窗口，窗口内重连可复用同一张票）。
const DefaultTicketTTL = 120 * time.Second

// ticketKID 是本轮票据的密钥版本（轮换预留；接入层与 battle 共持同一把密钥，恒 1）。
const ticketKID uint8 = 1

// IssueEntryTicket 实现 battlev1actor.BattleService：按参战名单逐人签票
// （每人恰好一张，票绑本人 player_id + 本局 battle_id），并回填接入层各面地址与统一过期时刻。
// 名单为空（未开局/对局不存在）返回 BATTLE_NOT_FOUND；签票失败返回 INTERNAL。
func (b *BattleActor) IssueEntryTicket(_ core.ActorContext, _ *battlev1.IssueEntryTicketReq) (*battlev1.IssueEntryTicketReply, error) {
	players := b.roster()
	if len(players) == 0 {
		return nil, errorv1.ErrBattleNotFound("战斗 %s 参战名单为空，无法出票", b.battleID)
	}
	now := time.Now()
	expiresAt := now.Add(b.cfg.TicketTTL)
	tickets := make([]*battlev1.BattleTicketEntry, 0, len(players))
	for _, playerID := range players {
		raw, err := ticket.Encode(ticket.Ticket{
			Version:   ticket.Version1,
			KID:       ticketKID,
			PlayerID:  playerID,
			BattleID:  b.battleID,
			IssuedAt:  now,
			ExpiresAt: expiresAt,
		}, b.cfg.TicketKey)
		if err != nil {
			return nil, errorv1.ErrInternal("战斗 %s 为玩家 %s 出票失败: %v", b.battleID, playerID, err)
		}
		tickets = append(tickets, &battlev1.BattleTicketEntry{PlayerId: playerID, Ticket: raw})
	}
	return &battlev1.IssueEntryTicketReply{
		// 接入层地址的唯一来源是本 actor 的配置（只读，故回执与配置共用同一份面列表）。
		Endpoints:       b.cfg.EdgeEndpoints,
		Tickets:         tickets,
		ExpiresAtUnixMs: expiresAt.UnixMilli(),
	}, nil
}

// roster 返回参战名单的稳定快照（按 player_id 升序，回执顺序可复现、便于比对）。
func (b *BattleActor) roster() []string {
	out := make([]string, 0, len(b.players))
	for playerID := range b.players {
		out = append(out, playerID)
	}
	sort.Strings(out)
	return out
}
