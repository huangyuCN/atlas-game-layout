// 出票域：按本局参战名单**逐人**签发直连入场票据（IssueEntryTicket，INTERNAL actor 方法）。
//
// 为什么票必须先于连接到达客户端（规格 §5）：直连后 JoinBattle 本身就走直连通道，
// 而「能连上」的前提是先有票——因此票只能随成局推送下发。battle 在出票时按名单逐人
// 加密（每票绑 player_id + battle_id），接入层与 battle 共持同一把 32 字节密钥
//（票据实现见 contrib/edge/ticket）。

package actor

import (
	"hash/crc32"
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
//
// 幂等：入场已走 Admit（开局+出票一次完成），本方法对其返回**首次签发的那一份**——
// 不重签、不刷新过期时刻（否则「同一局重复调用不重复出票」在重取票路径上失效）。
func (b *BattleActor) IssueEntryTicket(_ core.ActorContext, _ *battlev1.IssueEntryTicketReq) (*battlev1.IssueEntryTicketReply, error) {
	if b.admission != nil {
		return b.admission, nil
	}
	players := b.roster()
	if len(players) == 0 {
		return nil, errorv1.ErrBattleNotFound("战斗 %s 参战名单为空，无法出票", b.battleID)
	}
	return b.signTickets(players)
}

// signTickets 按给定名单逐人签票（纯计算：不改 actor 状态，失败即无副作用），
// 回执带接入层各面地址与统一的过期时刻。名单顺序即回执顺序（调用方已排序）。
func (b *BattleActor) signTickets(players []string) (*battlev1.IssueEntryTicketReply, error) {
	now := time.Now()
	expiresAt := now.Add(b.cfg.TicketTTL)
	instances := b.cfg.edgeInstances()
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
		// 逐人下发**被选中实例**的面列表：同一玩家恒定同一实例（哈希确定、跨进程一致），
		// 数据报面的 flow 状态因此不跨实例漂移；票据本身不含地址（换实例不改票、不改签名）。
		tickets = append(tickets, &battlev1.BattleTicketEntry{
			PlayerId:  playerID,
			Ticket:    raw,
			Endpoints: edgeInstanceOf(instances, playerID),
		})
	}
	return &battlev1.IssueEntryTicketReply{
		// 回执级 endpoints 取**首个实例**（单实例部署 = 全部）：仅作旧客户端的兜底。
		Endpoints:       instances[0],
		Tickets:         tickets,
		ExpiresAtUnixMs: expiresAt.UnixMilli(),
	}, nil
}

// edgeInstances 返回接入层实例列表：未配多实例时把 EdgeEndpoints 视作唯一实例
// （单实例行为与改造前一致，不强制改配置）。
func (c Config) edgeInstances() [][]*battlev1.EdgeEndpoint {
	if len(c.EdgeInstances) > 0 {
		return c.EdgeInstances
	}
	return [][]*battlev1.EdgeEndpoint{c.EdgeEndpoints}
}

// edgeInstanceOf 按 player_id 从实例列表里选一个实例，返回其面列表。
//
// 选择必须**确定性且跨进程一致**：同一玩家换节点、换对局、掉线重连都要落到同一实例，
// 否则数据报面的会话状态（flow-id / 探活 / 接管）会跨实例漂移，表现为「重连后下行收不到」。
// 因此用 CRC32（IEEE）而非 Go 的 map 哈希（后者每进程随机种子）。
func edgeInstanceOf(instances [][]*battlev1.EdgeEndpoint, playerID string) []*battlev1.EdgeEndpoint {
	if len(instances) == 0 {
		return nil
	}
	return instances[int(crc32.ChecksumIEEE([]byte(playerID))%uint32(len(instances)))]
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
