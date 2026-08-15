// Package session 提供 gateway 分布式会话管理（D13）：
// 本地连接表（connID → Session，多通道聚合）与 redis 路由表
// （playerID → Route，TTL + 心跳续租），支撑挤下线与下行推送的跨实例寻址。
package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrSessionNotFound 表示玩家会话不存在（本地无连接且路由已过期）。
var ErrSessionNotFound = errors.New("session: not found")

// Channel 是通道类别（规格 §5 双通道模型）。
type Channel string

const (
	// ChannelBiz 是业务通道（tcp/ws）。
	ChannelBiz Channel = "biz"
	// ChannelBattle 是战斗通道（kcp/udp/ws）。
	ChannelBattle Channel = "battle"
)

// Route 是玩家连接的路由信息（redis 分布式路由表条目）。
// 双通道聚合：biz 与 battle 各自独立连接 ID，推送时 battle 优先、缺省回退 biz。
type Route struct {
	InstanceID   string `json:"instance_id"`    // 持有连接的 gateway 实例 ID
	Token        string `json:"token"`          // 会话令牌（单点登录裁决依据）
	BizConnID    uint64 `json:"biz_conn_id"`    // 业务通道连接 ID（0=未绑定）
	BizKind      string `json:"biz_kind"`       // 业务通道传输种类（tcp/ws）
	BattleConnID uint64 `json:"battle_conn_id"` // 战斗通道连接 ID（0=未绑定）
	BattleKind   string `json:"battle_kind"`    // 战斗通道传输种类（kcp/udp/ws）
	UpdatedAt    int64  `json:"updated_at"`     // 最近心跳时间（unix 毫秒）
}

// Conn 是会话视角的单条连接：寻址 ID、传输种类与回写函数。
type Conn struct {
	ID   uint64
	Kind string
	// Ref 是连接身份键（如 "ws:123"、"udp:1.2.3.4:5"），供会话反向索引
	// （帧输入/补帧按连接反查玩家身份）。同一种类的连接 Ref 唯一。
	Ref string
	// Send 回写该连接（服务端推送）；由 server 层装配为 transport Server 的 PushRaw。
	Send func(operation string, payload []byte) error
}

// Session 是玩家会话的本地视图（多通道聚合）。
type Session struct {
	PlayerID      string
	Token         string
	Biz           *Conn
	Battle        *Conn
	LastHeartbeat time.Time
}

// Manager 是分布式会话管理器：本地连接表 + redis 路由表。
// 所有方法并发安全；心跳清扫由 Start/SweepOnce 驱动。
type Manager struct {
	store      Store
	instanceID string
	ttl        time.Duration

	mu    sync.RWMutex
	local map[string]*Session // playerID → 本地会话
	refs  map[string]string   // 连接 Ref → playerID（按连接反查玩家身份）
}

// NewManager 构造会话管理器。
func NewManager(store Store, instanceID string, ttl time.Duration) *Manager {
	return &Manager{
		store:      store,
		instanceID: instanceID,
		ttl:        ttl,
		local:      make(map[string]*Session),
		refs:       make(map[string]string),
	}
}

// Bind 将连接绑定到玩家会话的指定通道，并向 redis 路由表登记。
// 返回绑定前的旧路由（nil 表示首次登录），供挤下线判断；
// 新登录会使旧路由（含旧 token）失效。
func (m *Manager) Bind(ctx context.Context, playerID string, c *Conn, channel Channel, token string) (*Route, error) {
	if playerID == "" || c == nil {
		return nil, fmt.Errorf("session: playerID/conn 不能为空")
	}
	if token == "" {
		return nil, fmt.Errorf("session: token 不能为空")
	}
	now := time.Now()

	// 本地表：每次绑定生成新的会话快照（旧快照保持不可变，供挤下线等外部引用安全使用）。
	m.mu.Lock()
	prev := m.local[playerID]
	next := &Session{PlayerID: playerID, LastHeartbeat: now}
	if prev != nil {
		next.Biz = prev.Biz
		next.Battle = prev.Battle
	}
	next.Token = token
	switch channel {
	case ChannelBiz:
		next.Biz = c
	case ChannelBattle:
		next.Battle = c
	default:
		m.mu.Unlock()
		return nil, fmt.Errorf("session: 未知通道类别 %q", channel)
	}
	m.local[playerID] = next
	// 反向索引：新连接登记；被替换/移除的旧连接注销（防旧连接残留身份）。
	m.dropStaleRefs(prev, next)
	if c.Ref != "" {
		m.refs[c.Ref] = playerID
	}
	m.mu.Unlock()

	// 路由表：单命令原子「写新值 + 设 TTL + 取旧值」（SET ... GET，见 RedisStore）。
	r := Route{
		InstanceID: m.instanceID,
		Token:      next.Token,
		UpdatedAt:  now.UnixMilli(),
	}
	if next.Biz != nil {
		r.BizConnID, r.BizKind = next.Biz.ID, next.Biz.Kind
	}
	if next.Battle != nil {
		r.BattleConnID, r.BattleKind = next.Battle.ID, next.Battle.Kind
	}
	old, err := m.store.GetSet(ctx, playerID, &r, m.ttl)
	if err != nil {
		return nil, err
	}
	return old, nil
}

// Unbind 解绑玩家会话（登出/挤下线清理）。
// 仅当指定 connID 仍属于该玩家会话时生效（防旧连接的迟到解绑误删新会话）；
// redis 路由仅在仍归属本实例的该连接时删除（防跨实例挤下线误删新实例路由）。
func (m *Manager) Unbind(ctx context.Context, playerID string, connID uint64) {
	m.mu.Lock()
	sess := m.local[playerID]
	if sess == nil {
		m.mu.Unlock()
		return
	}
	if owned := (sess.Biz != nil && sess.Biz.ID == connID) || (sess.Battle != nil && sess.Battle.ID == connID); !owned {
		m.mu.Unlock()
		return
	}
	delete(m.local, playerID)
	m.dropConnRefs(sess)
	m.mu.Unlock()
	m.deleteRouteIfOwned(ctx, playerID, connID)
}

// dropStaleRefs 注销旧会话中已不在新会话的连接反向索引（调用方持写锁）。
func (m *Manager) dropStaleRefs(prev, next *Session) {
	if prev == nil {
		return
	}
	for _, old := range []*Conn{prev.Biz, prev.Battle} {
		if old == nil || old.Ref == "" {
			continue
		}
		keep := (next.Biz != nil && next.Biz.Ref == old.Ref) ||
			(next.Battle != nil && next.Battle.Ref == old.Ref)
		if !keep {
			delete(m.refs, old.Ref)
		}
	}
}

// dropConnRefs 注销会话全部连接的反向索引（调用方持写锁）。
func (m *Manager) dropConnRefs(sess *Session) {
	for _, c := range []*Conn{sess.Biz, sess.Battle} {
		if c != nil && c.Ref != "" {
			delete(m.refs, c.Ref)
		}
	}
}

// PlayerByRef 按连接身份反查玩家（帧输入/补帧的身份来源）；未绑定返回 false。
func (m *Manager) PlayerByRef(ref string) (string, bool) {
	if ref == "" {
		return "", false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	pid, ok := m.refs[ref]
	return pid, ok
}

// deleteRouteIfOwned 仅当 redis 路由仍归属本实例的该连接时删除。
func (m *Manager) deleteRouteIfOwned(ctx context.Context, playerID string, connID uint64) {
	r, err := m.store.Get(ctx, playerID)
	if err != nil || r == nil {
		return
	}
	if r.InstanceID != m.instanceID {
		return
	}
	if r.BizConnID != connID && r.BattleConnID != connID {
		return
	}
	_ = m.store.Delete(ctx, playerID)
}

// Heartbeat 续租玩家会话（更新本地心跳 + redis TTL）；令牌不符返回 false。
func (m *Manager) Heartbeat(ctx context.Context, playerID, token string) bool {
	m.mu.Lock()
	sess := m.local[playerID]
	if sess == nil || sess.Token != token {
		m.mu.Unlock()
		return false
	}
	sess.LastHeartbeat = time.Now()
	m.mu.Unlock()
	return m.store.Expire(ctx, playerID, m.ttl) == nil
}

// Validate 校验令牌与路由表一致（单点登录裁决：旧 token 在 redis 被覆盖后即失效）。
func (m *Manager) Validate(ctx context.Context, playerID, token string) (bool, error) {
	r, err := m.store.Get(ctx, playerID)
	if err != nil {
		return false, err
	}
	if r == nil || r.Token == "" || r.Token != token {
		return false, nil
	}
	return true, nil
}

// Route 读取玩家路由（nil 表示无路由）。
func (m *Manager) Route(ctx context.Context, playerID string) (*Route, error) {
	return m.store.Get(ctx, playerID)
}

// LocalSession 返回本实例上的玩家会话（不存在返回 false）。
func (m *Manager) LocalSession(playerID string) (*Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sess, ok := m.local[playerID]
	return sess, ok
}

// PushRaw 向玩家下发推送：battle 通道优先，缺省回退 biz 通道。
// 玩家不在本实例时返回 ErrSessionNotFound。
func (m *Manager) PushRaw(playerID string, operation string, payload []byte) error {
	m.mu.RLock()
	sess, ok := m.local[playerID]
	if !ok {
		m.mu.RUnlock()
		return ErrSessionNotFound
	}
	var target *Conn
	if sess.Battle != nil {
		target = sess.Battle
	} else if sess.Biz != nil {
		target = sess.Biz
	} else {
		m.mu.RUnlock()
		return ErrSessionNotFound
	}
	m.mu.RUnlock()
	return target.Send(operation, payload)
}

// SweepOnce 清扫心跳超过 ttl 的过期会话（本地 + redis 同步删除），返回清理数。
func (m *Manager) SweepOnce(ctx context.Context) int {
	deadline := time.Now().Add(-m.ttl)
	var expired []string
	m.mu.RLock()
	for id, sess := range m.local {
		if sess.LastHeartbeat.Before(deadline) {
			expired = append(expired, id)
		}
	}
	m.mu.RUnlock()

	removed := 0
	for _, id := range expired {
		// 删除前复核心跳：避免与并发 Bind/Heartbeat 竞争误删。
		m.mu.Lock()
		sess := m.local[id]
		if sess != nil && sess.LastHeartbeat.Before(deadline) {
			delete(m.local, id)
			m.dropConnRefs(sess)
			removed++
		}
		m.mu.Unlock()
		// 路由删除同样做属主校验：会话已被新实例接管时保留新路由。
		connID := uint64(0)
		if sess != nil && sess.Biz != nil {
			connID = sess.Biz.ID
		}
		m.deleteRouteIfOwned(ctx, id, connID)
	}
	return removed
}

// Start 启动后台心跳清扫循环；ctx 取消时退出。
func (m *Manager) Start(ctx context.Context) {
	interval := m.ttl / 2
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.SweepOnce(ctx)
			}
		}
	}()
}

// SessionCount 返回本实例当前会话数（测试与可观测用）。
func (m *Manager) SessionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.local)
}
