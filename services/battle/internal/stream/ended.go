package stream

import (
	"sync"
	"time"
)

// DefaultEndedTTL 是结束留档的默认生命周期（装配期按票据有效期与掉线窗口推导，
// 见 EndedTTL；本值仅在调用方未给出正 TTL 时兜底）。
const DefaultEndedTTL = 5 * time.Minute

// MaxEndReplays 是同一玩家在一局留档期内最多获得的补投次数（有界：迟到的帧 op
// 会反复触发补投，不封顶等于给已结束的对局留一条无限重发的通道）。
const MaxEndReplays = 5

// EndRetries 是结算关闭前对每一条未确认连接的重投次数（数据报面无重传，
// 单次结算推送丢包即「玩家不知道结果」；重投次数有界，不做确认重传）。
const EndRetries = 2

// EndedTTL 返回结束留档的生命周期：票据有效期 + 掉线窗口。
// 取值依据——票据过期后不再有任何合法帧 op 能指向该局（墓碑再长也无事可做），
// 而掉线窗口是「窗口内还能用同一张票回座」的最后期限，留档必须把它整段覆盖。
func EndedTTL(ticketTTL, offlineTimeout time.Duration) time.Duration {
	ttl := ticketTTL + offlineTimeout
	if ttl <= 0 {
		return DefaultEndedTTL
	}
	return ttl
}

// endedEntry 是一局战斗的结束留档：墓碑 + 胜负 + 参战名单 + 各玩家的补投计数。
type endedEntry struct {
	winner   string
	players  map[string]struct{}
	expireAt time.Time
	replays  map[string]int // playerID → 已补投次数
}

// expiredAt 判定留档在给定时刻是否已过期。
func (e *endedEntry) expiredAt(now time.Time) bool { return !now.Before(e.expireAt) }

// EndedBook 是「已结束对局」的留档表：结算时写一次，帧面在**懒激活之前**据此拒绝迟到的
// 帧 op（否则 SpawnAuto 会重建一个空名单实例，恢复快照后立刻再次结算并关闭直连），
// 并在玩家重连后把留档结果补投到新连接。
//
// 生命周期：留档按 TTL 过期（EndedTTL），过期后判定为「未结束」——新对局复用同一
// battle_id 不受影响（本仓 battle_id 为 idgen.Battle 的 UUID，实际不复用）。
type EndedBook struct {
	mu      sync.Mutex
	ttl     time.Duration
	now     func() time.Time // 时钟可替换（测试注入假时钟，避免 sleep）
	entries map[string]*endedEntry
}

// NewEndedBook 构造结束留档表（ttl ≤ 0 取 DefaultEndedTTL）。
func NewEndedBook(ttl time.Duration) *EndedBook {
	if ttl <= 0 {
		ttl = DefaultEndedTTL
	}
	return &EndedBook{ttl: ttl, now: time.Now, entries: make(map[string]*endedEntry)}
}

// Record 留档一局战斗的结算结果（幂等：同局以首次留档为准，重复调用不覆盖、不延长 TTL）。
func (b *EndedBook) Record(battleID, winner string, players []string) {
	if battleID == "" {
		return
	}
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sweepLocked(now)
	if _, ok := b.entries[battleID]; ok {
		return
	}
	roster := make(map[string]struct{}, len(players))
	for _, p := range players {
		if p != "" {
			roster[p] = struct{}{}
		}
	}
	b.entries[battleID] = &endedEntry{
		winner:   winner,
		players:  roster,
		expireAt: now.Add(b.ttl),
		replays:  make(map[string]int, len(roster)),
	}
}

// Ended 判定该对局是否仍处于「已结束」留档期（过期的留档顺手清除）。
func (b *EndedBook) Ended(battleID string) bool {
	_, ok := b.Winner(battleID)
	return ok
}

// Winner 返回留档的胜者（未留档或已过期返回 false）：关闭前重投与重连补投都取它。
func (b *EndedBook) Winner(battleID string) (string, bool) {
	if battleID == "" {
		return "", false
	}
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.entries[battleID]
	if !ok {
		return "", false
	}
	if e.expiredAt(now) {
		delete(b.entries, battleID)
		return "", false
	}
	return e.winner, true
}

// Claim 为一次补投取额度：留档存在、该玩家在参战名单内且未超出 MaxEndReplays → 放行。
// 拒绝的语义是「不再补投」（不报错）：迟到 op 照样被稳定 reason 拒绝，只是不再重发结果。
func (b *EndedBook) Claim(battleID, playerID string) (string, bool) {
	if battleID == "" || playerID == "" {
		return "", false
	}
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.entries[battleID]
	if !ok {
		return "", false
	}
	if e.expiredAt(now) {
		delete(b.entries, battleID)
		return "", false
	}
	if _, ok := e.players[playerID]; !ok {
		return "", false
	}
	if e.replays[playerID] >= MaxEndReplays {
		return "", false
	}
	e.replays[playerID]++
	return e.winner, true
}

// Len 返回当前留档条数（观测与测试用）。
func (b *EndedBook) Len() int {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sweepLocked(now)
	return len(b.entries)
}

// sweepLocked 清除已过期的留档（写入与统计路径顺手清理，表不随对局数无限增长）。
func (b *EndedBook) sweepLocked(now time.Time) {
	for id, e := range b.entries {
		if e.expiredAt(now) {
			delete(b.entries, id)
		}
	}
}
