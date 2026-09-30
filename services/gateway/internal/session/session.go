// Package session 提供 gateway 分布式会话管理（D13）：
// 本地连接表（玩家 → 会话，单一业务通道）与 redis 路由表
// （playerID → Route，TTL + 心跳续租），支撑挤下线与下行推送的跨实例寻址。
//
// 阶段 3 批次 5 起战斗帧不再经网关（客户端直连接入层 → battle 帧面），
// 故会话模型由「业务/战斗双通道聚合」收敛为**单一业务通道**：一个玩家至多一条连接。
package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/huangyuCN/atlas/metrics"
)

// 会话层指标名（Prometheus 抓取；gauge 事件驱动 Set，counter 单调递增）。
const (
	MetricSessions      = "gateway_sessions"             // 当前本地会话数（gauge）
	MetricSessionBinds  = "gateway_session_binds_total"  // 会话绑定计数
	MetricSessionSweeps = "gateway_session_sweeps_total" // 心跳过期清扫计数
)

// ErrSessionNotFound 表示玩家会话不存在（本地无连接且路由已过期）。
var ErrSessionNotFound = errors.New("session: not found")

// Route 是玩家连接的路由信息（redis 分布式路由表条目）：
// 单一业务通道，一个玩家至多一条连接（战斗帧直连后网关不再持有战斗通道）。
type Route struct {
	InstanceID string `json:"instance_id"` // 持有连接的 gateway 实例 ID
	Token      string `json:"token"`       // 会话令牌（单点登录裁决依据）
	ConnID     uint64 `json:"conn_id"`     // 业务通道连接 ID（0=未绑定）
	Kind       string `json:"kind"`        // 业务通道传输种类（tcp/ws）
	UpdatedAt  int64  `json:"updated_at"`  // 最近心跳时间（unix 毫秒）
}

// Conn 是会话视角的单条连接：寻址 ID、传输种类与回写函数。
type Conn struct {
	ID   uint64
	Kind string
	// Ref 是连接身份键（如 "ws:conn:123"、"udp:1.2.3.4:5"），供会话反向索引
	// （业务 op 按连接反查玩家身份）。同一种类的连接 Ref 唯一。
	Ref string
	// Send 回写该连接（服务端推送）；由 server 层装配为 transport Server 的 PushRaw。
	Send func(operation string, payload []byte) error
}

// Session 是玩家会话的本地视图（单一业务通道）。
type Session struct {
	PlayerID      string
	Token         string
	Conn          *Conn
	LastHeartbeat time.Time
}

// Manager 是分布式会话管理器：本地连接表 + redis 路由表。
// 所有方法并发安全；心跳清扫由 Start/SweepOnce 驱动。
type Manager struct {
	store         Store
	instanceID    string
	ttl           time.Duration              // 生效会话租期（路由 TTL 与心跳续租周期，见 Options）
	sweepInterval time.Duration              // 生效过期清扫周期（Start 的 ticker）
	sweptHook     func(swept []SweptSession) // 过期清扫联动钩子（nil = 不联动）
	meter         metrics.Collector          // 可选指标采集器（nil = 不打点）

	mu    sync.RWMutex
	local map[string]*Session // playerID → 本地会话
	refs  map[string]string   // 连接 Ref → playerID（按连接反查玩家身份）
	toks  map[string]string   // 会话凭据 → playerID（帧会话槽反查玩家身份；Bind 时登记）
}

// NewManager 构造会话管理器（opts 零值即默认租期与清扫周期，见 Options）。
func NewManager(store Store, instanceID string, opts Options) *Manager {
	ttl, sweepInterval := opts.resolve()
	return &Manager{
		store:         store,
		instanceID:    instanceID,
		ttl:           ttl,
		sweepInterval: sweepInterval,
		local:         make(map[string]*Session),
		refs:          make(map[string]string),
		toks:          make(map[string]string),
	}
}

// SetMeter 注入指标采集器（装配期一次；nil = 不打点）。
func (m *Manager) SetMeter(c metrics.Collector) { m.meter = c }

// Bind 将连接绑定到玩家会话（单一业务通道），并向 redis 路由表登记。
// 返回绑定前的旧路由（nil 表示首次登录），供挤下线判断；
// 新登录会使旧路由（含旧 token）失效。
func (m *Manager) Bind(ctx context.Context, playerID string, c *Conn, token string) (*Route, error) {
	if playerID == "" || c == nil {
		return nil, fmt.Errorf("session: playerID/conn 不能为空")
	}
	if token == "" {
		return nil, fmt.Errorf("session: token 不能为空")
	}
	now := time.Now()

	// 本地表更新（内部加锁）与路由表同步分开：
	// 前者只改内存态，后者是单命令原子远端写（SET ... GET，见 RedisStore）。
	next, err := m.bindLocal(playerID, c, token, now)
	if err != nil {
		return nil, err
	}
	m.countBind()
	return m.persistRoute(ctx, playerID, next, now)
}

// countBind 打点一次会话绑定并刷新会话数 gauge（meter 为 nil/noop 时短路）。
func (m *Manager) countBind() {
	if m.meter == nil || metrics.IsNoop(m.meter) {
		return
	}
	m.meter.Counter(MetricSessionBinds).Add(1)
	m.meter.Gauge(MetricSessions).Set(float64(m.Count()))
}

// bindLocal 更新本地会话表与反向索引（内部加锁），返回本次会话快照。
// 每次绑定生成新的会话快照（旧快照保持不可变，供挤下线等外部引用安全使用）；
// 新连接登记 refs，被替换/移除的旧连接注销（防旧连接残留身份）。
func (m *Manager) bindLocal(playerID string, c *Conn, token string, now time.Time) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev := m.local[playerID]
	next := &Session{PlayerID: playerID, Token: token, Conn: c, LastHeartbeat: now}
	m.local[playerID] = next
	m.dropStaleRefs(prev, next)
	if c.Ref != "" {
		m.refs[c.Ref] = playerID
	}
	// 新凭据登记（覆盖式：接管后旧凭据失效）；旧会话凭据随覆盖失效。
	if prev != nil && prev.Token != token {
		m.tokenIndexRemove(prev.Token)
	}
	m.tokenIndexSet(next)
	return next, nil
}

// persistRoute 将会话快照同步到 redis 路由表（单命令原子「写新值 + 设 TTL + 取旧值」）。
// 返回绑定前的旧路由（nil 表示首次登录），供挤下线判断。
func (m *Manager) persistRoute(ctx context.Context, playerID string, next *Session, now time.Time) (*Route, error) {
	r := Route{
		InstanceID: m.instanceID,
		Token:      next.Token,
		UpdatedAt:  now.UnixMilli(),
	}
	if next.Conn != nil {
		r.ConnID, r.Kind = next.Conn.ID, next.Conn.Kind
	}
	return m.store.GetSet(ctx, playerID, &r, m.ttl)
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
	if sess.Conn == nil || sess.Conn.ID != connID {
		m.mu.Unlock()
		return
	}
	delete(m.local, playerID)
	m.dropConnRefs(sess)
	m.tokenIndexRemove(sess.Token)
	m.mu.Unlock()
	m.countRemove()
	m.deleteRouteIfOwned(ctx, playerID, connID)
}

// countRemove 打点一次会话移除并刷新会话数 gauge（meter 为 nil/noop 时短路）。
func (m *Manager) countRemove() {
	if m.meter == nil || metrics.IsNoop(m.meter) {
		return
	}
	m.meter.Gauge(MetricSessions).Set(float64(m.Count()))
}

// dropStaleRefs 注销旧会话中已不在新会话的连接反向索引（调用方持写锁）。
func (m *Manager) dropStaleRefs(prev, next *Session) {
	if prev == nil || prev.Conn == nil || prev.Conn.Ref == "" {
		return
	}
	if next != nil && next.Conn != nil && next.Conn.Ref == prev.Conn.Ref {
		return
	}
	delete(m.refs, prev.Conn.Ref)
}

// tokenIndexSet 把本地会话凭据登记进 token 反向索引（bindLocal/unbind 共用）。
func (m *Manager) tokenIndexSet(sess *Session) {
	if sess == nil || sess.Token == "" {
		return
	}
	m.toks[sess.Token] = sess.PlayerID
}

// tokenIndexRemove 按凭据移除反向索引。
func (m *Manager) tokenIndexRemove(token string) {
	if token == "" {
		return
	}
	delete(m.toks, token)
}

// dropConnRefs 注销会话连接的反向索引（调用方持写锁）。
func (m *Manager) dropConnRefs(sess *Session) {
	if sess != nil && sess.Conn != nil && sess.Conn.Ref != "" {
		delete(m.refs, sess.Conn.Ref)
	}
}

// PlayerByRef 按连接身份反查玩家（业务 op 的身份来源）；未绑定返回 false。
func (m *Manager) PlayerByRef(ref string) (string, bool) {
	if ref == "" {
		return "", false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	pid, ok := m.refs[ref]
	return pid, ok
}

// PlayerByToken 按帧会话槽携带的凭据反查玩家身份（网关自留会话接口的备用身份来源）。
// 凭据索引与连接绑定同生命周期：接管/清理时随本地会话移除。
func (m *Manager) PlayerByToken(token string) (string, bool) {
	if token == "" {
		return "", false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	pid, ok := m.toks[token]
	return pid, ok
}

// routeTakenOver 判断路由是否已被其他会话接管（另一实例或本实例新连接）。
// 路由不存在（TTL 自然过期）不算接管。
func (m *Manager) routeTakenOver(ctx context.Context, playerID string, connID uint64) bool {
	r, err := m.store.Get(ctx, playerID)
	if err != nil || r == nil {
		return false
	}
	if r.InstanceID != m.instanceID {
		return true
	}
	return r.ConnID != connID
}

// deleteRouteIfOwned 仅当路由仍归属本实例的该连接时删除（挤下线清理用）。
func (m *Manager) deleteRouteIfOwned(ctx context.Context, playerID string, connID uint64) {
	r, err := m.store.Get(ctx, playerID)
	if err != nil || r == nil {
		return
	}
	if r.InstanceID != m.instanceID || r.ConnID != connID {
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

// PushRaw 向玩家下发推送：走其唯一业务通道。
// 玩家不在本实例时返回 ErrSessionNotFound。
func (m *Manager) PushRaw(playerID string, operation string, payload []byte) error {
	m.mu.RLock()
	sess, ok := m.local[playerID]
	if !ok || sess.Conn == nil {
		m.mu.RUnlock()
		return ErrSessionNotFound
	}
	target := sess.Conn
	m.mu.RUnlock()
	return target.Send(operation, payload)
}

// SweptSession 是一次过期清扫的记录（联动通知用）。
type SweptSession struct {
	PlayerID string
	Token    string // 过期会话的令牌（接管裁决依据）
}

// SweepOnce 清扫心跳超过 ttl 的过期会话（本地 + redis 同步删除），
// 返回清理数；对「确认属主且未被新登录接管」的会话回调 SweptHook
// （异常下线联动撮合域：取消匹配/离队）。
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
	var swept []SweptSession
	for _, id := range expired {
		// 删除前复核心跳：避免与并发 Bind/Heartbeat 竞争误删。
		m.mu.Lock()
		sess := m.local[id]
		if sess != nil && sess.LastHeartbeat.Before(deadline) {
			delete(m.local, id)
			m.dropConnRefs(sess)
			m.tokenIndexRemove(sess.Token)
			removed++
		}
		m.mu.Unlock()
		// 属主校验：路由被其他会话接管时保留新路由且跳过联动（新会话接管）；
		// 路由自然过期（TTL 先于清扫）视为无接管，同样联动。
		connID := uint64(0)
		if sess != nil && sess.Conn != nil {
			connID = sess.Conn.ID
		}
		if !m.routeTakenOver(ctx, id, connID) {
			m.deleteRouteIfOwned(ctx, id, connID) // 属主路由删除
			token := ""
			if sess != nil {
				token = sess.Token
			}
			swept = append(swept, SweptSession{PlayerID: id, Token: token})
		}
	}
	if removed > 0 {
		m.countRemove()
		if m.meter != nil && !metrics.IsNoop(m.meter) {
			m.meter.Counter(MetricSessionSweeps).Add(float64(removed))
		}
	}
	if len(swept) > 0 && m.sweptHook != nil {
		m.sweptHook(swept)
	}
	return removed
}

// SetSweptHook 设置过期清扫联动钩子（Gateway 注入，向 game PlayerActor 发下线联动）。
// 仅「未被接管」的会话触发：新登录接管（路由属主变更）时跳过，避免误停新会话的 actor。
func (m *Manager) SetSweptHook(fn func(swept []SweptSession)) {
	m.sweptHook = fn
}

// Start 启动后台心跳清扫循环（周期为生效清扫周期，见 Options）；ctx 取消时退出。
func (m *Manager) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(m.sweepInterval)
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

// Count 返回本实例当前会话数（测试与可观测用）。
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.local)
}
