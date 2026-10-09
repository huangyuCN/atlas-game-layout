// Package ledger 提供结算留档的**跨节点**存储与激活闸门（P1-7）。
//
// 解决什么：结算留档（墓碑 + 胜负 + 参战名单）原先只落在**结算节点本地的内存表**
// （stream.EndedBook），目录归属在 actor 停止后已释放——一条迟到的帧 op、rpc 面调用或
// 重投的激活请求落到**别的节点**时，那个节点不知道这局已结束，会按 SpawnAuto 重新拉起
// 一个空名单实例（恢复快照后立刻再结算并反复关闭直连，阶段 3 验收的 A1 缺陷在跨节点形态下复现）。
//
// 本包两个落点：
//   - Store：留档的共享存储接缝（MemoryStore 单机/单测，EtcdStore 生产），结算节点写一次、
//     任意节点可读；TTL 与本地留档同源（see stream.EndedTTL）。
//   - Gate：框架 cluster.ActivationGate 的实现——**任何节点**在为一个 battle PID 创建 cell
//     之前问一次，共享留档命中即拒绝复活。
package ledger

import (
	"context"
	"sync"
	"time"
)

// Entry 是一局已结束战斗的留档内容（胜负 + 参战名单）。
// 闸门只关心「是否已留档」，胜负与名单供排障与后续按名单补投使用。
type Entry struct {
	// Winner 是胜者玩家 ID（空 = 平局）。
	Winner string `json:"winner"`
	// Players 是结算时的参战名单。
	Players []string `json:"players"`
}

// Store 是留档的共享存储接缝：结算节点写一次（带 TTL），任意节点读得到。
// 实现必须并发安全；Lookup 未命中返回 ok=false 且 err=nil（未留档不是错误）。
type Store interface {
	// Record 写入一局对局的留档；同局重复写入幂等（以最新内容为准，TTL 重新起算）。
	Record(ctx context.Context, battleID string, e Entry, ttl time.Duration) error
	// Lookup 读一局对局的留档；未留档或已过期返回 ok=false。
	Lookup(ctx context.Context, battleID string) (Entry, bool, error)
}

// memEntry 是进程内留档项及其过期时刻。
type memEntry struct {
	entry    Entry
	expireAt time.Time
}

// MemoryStore 是留档的进程内实现（单机形态与单测）：按 TTL 过期，时钟可注入。
type MemoryStore struct {
	mu      sync.Mutex
	now     func() time.Time
	entries map[string]memEntry
}

// NewMemoryStore 构造进程内留档表。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{now: time.Now, entries: make(map[string]memEntry)}
}

// Record 实现 Store：写入留档并按 TTL 记过期时刻（ttl ≤ 0 视为立即过期，不静默永久保留）。
func (s *MemoryStore) Record(_ context.Context, battleID string, e Entry, ttl time.Duration) error {
	if battleID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[battleID] = memEntry{entry: e, expireAt: s.now().Add(ttl)}
	return nil
}

// Lookup 实现 Store：命中且未过期返回值，过期顺手清除并返回未命中。
func (s *MemoryStore) Lookup(_ context.Context, battleID string) (Entry, bool, error) {
	if battleID == "" {
		return Entry{}, false, nil
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.entries[battleID]
	if !ok {
		return Entry{}, false, nil
	}
	if !now.Before(item.expireAt) {
		delete(s.entries, battleID)
		return Entry{}, false, nil
	}
	return item.entry, true, nil
}
