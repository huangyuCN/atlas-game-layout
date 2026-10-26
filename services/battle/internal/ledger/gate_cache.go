package ledger

import "time"

// 负缓存表的内存上界（条目数）：一条条目 ≈ 一个 battle ID + 一个过期时刻，4096 条约几百 KB。
// 到顶先清过期条目；仍满则**放弃缓存本条**——负缓存是省一次存储查询的优化，
// 绝不能因为局数暴涨变成内存风险。
const negativeCacheMaxEntries = 4096

// missCached 报告该 battle_id 是否落在 miss 负缓存窗口内（窗口内视为「刚刚确认过未留档」）。
func (g *Gate) missCached(uid string) bool {
	if g.negTTL <= 0 || uid == "" {
		return false
	}
	now := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	until, ok := g.missUntil[uid]
	if !ok {
		return false
	}
	if now.Before(until) {
		return true
	}
	delete(g.missUntil, uid)
	return false
}

// cacheMiss 记录一次「未留档」判定及其过期时刻；负缓存关闭或表满时不记录。
// 只缓存 miss：命中（已结算）与查询失败都不进表（见 AllowActivation 的两条口径）。
func (g *Gate) cacheMiss(uid string) {
	if g.negTTL <= 0 || uid == "" {
		return
	}
	now := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.missUntil) >= negativeCacheMaxEntries {
		g.pruneLocked(now)
		if len(g.missUntil) >= negativeCacheMaxEntries {
			return
		}
	}
	g.missUntil[uid] = now.Add(g.negTTL)
}

// pruneLocked 清掉已过期的负缓存条目（调用方必须持锁）。
func (g *Gate) pruneLocked(now time.Time) {
	for uid, until := range g.missUntil {
		if !now.Before(until) {
			delete(g.missUntil, uid)
		}
	}
}
