package resolver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/contrib/edge"
)

// 本地缓存参数（全是兜底：正常情况下事件驱动的映射不需要它们）。
const (
	// DefaultOwnerFreshTTL 是属主条目的最长有效期（兜底）：目录记录在租约续期时会被重写，
	// watch 事件因此持续刷新它；超过该时长没有任何事件（监听可能已静默断开）就按过期处理、
	// 重新查一次目录——旧实现最多错一个 TTL，新实现不能因为监听断了就无限错。
	DefaultOwnerFreshTTL = 2 * time.Minute
	// DefaultFrameFreshTTL 是帧面实例快照的最长有效期（兜底），语义同上。
	DefaultFrameFreshTTL = time.Minute
	// DefaultOwnerIdleTTL 是属主条目的空闲回收时长：既无解析命中也无 watch 事件超过它即回收
	// （并取消该局的目录监听）。正常结束的对局由目录删除事件即时回收，本项只是兜底：
	// 防止「监听断开后错过删除事件」让条目与 watch 按历史对局数一直累积。
	DefaultOwnerIdleTTL = 10 * time.Minute
	// sweepInterval 是空闲回收的巡检间隔（空闲上限的一半：最多多留半个周期）。
	sweepInterval = DefaultOwnerIdleTTL / 2
	// watchRetryInterval 是监听建立失败后的重试间隔：失败期间该局不缓存（退回两跳查询），
	// 但不至于每次 hello 都去建一条注定失败的 watch（注册中心抖动时不放大成风暴）。
	watchRetryInterval = 5 * time.Second
)

// cacheLookup 是一次本地查缓存的判定：既用于命中率口径，也用于合并两层的结论。
type cacheLookup int

// 判定结果的全部取值。
const (
	lookupHit   cacheLookup = iota // 本地命中：本次没有网络往返
	lookupMiss                     // 没有可用条目：走了网络
	lookupStale                    // 条目已过期：走了网络
)

// merge 合并两层的判定：只要有一层走了网络就不算命中；过期比未命中更能说明「监听滞后了」。
func (a cacheLookup) merge(b cacheLookup) cacheLookup {
	if a == lookupStale || b == lookupStale {
		return lookupStale
	}
	if a == lookupMiss || b == lookupMiss {
		return lookupMiss
	}
	return lookupHit
}

// ownerEntry 是一个对局的属主本地条目（battle_id → 属主节点）。
type ownerEntry struct {
	owner    string    // 最近一次事实：属主节点 ID
	seen     time.Time // 最近一次「证据」时间（解析命中、watch 事件或回填）
	eventAt  time.Time // 最近一次 watch 事件时间（判定「查询结果是否更旧」用；零值 = 从无事件）
	cancel   func()    // 该局目录监听的取消函数（nil = 监听未建立，条目不可用）
	starting bool      // 正在建立监听（防并发重复建）
	retryAt  time.Time // 监听建立失败后的下次尝试时间
}

// frameSnapshot 是帧面实例的全量快照：由唯一的全局监听维护，按节点索引便于 O(1) 命中。
type frameSnapshot struct {
	byNode map[string]Instance
	at     time.Time
}

// lookupOwner 查该局的属主条目：监听已建立且未过期才算命中（过期按 stale 记账）。
// 命中会顺带刷新 seen——「持续有人用」本身就是条目仍然有效的证据。
func (r *Resolver) lookupOwner(battleID string, now time.Time) (string, cacheLookup) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.owners[battleID]
	// 监听已建但还没有属主（首次查询失败过）：不算命中——空属主不是一个可用的解析结果，
	// 让它走一次目录查询，而不是把空值当答案传给 pickInstance。
	if e == nil || e.cancel == nil || e.owner == "" {
		return "", lookupMiss
	}
	if now.After(e.seen.Add(DefaultOwnerFreshTTL)) {
		return "", lookupStale
	}
	e.seen = now
	return e.owner, lookupHit
}

// storeOwner 回填属主：只有该局**已建立目录监听**时才写入（监听不可用就不缓存，退回两跳查询）。
// started 是本次查询的起点：查询期间到达的 watch 事件更新，不能被更旧的查询结果覆盖。
func (r *Resolver) storeOwner(battleID, owner string, started time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.owners[battleID]
	if e == nil || e.cancel == nil || e.eventAt.After(started) {
		return
	}
	e.owner, e.seen = owner, started
}

// ensureOwnerWatch 确保该局有一条目录监听（幂等）：已建立、正在建立或仍在退避窗口内即跳过。
func (r *Resolver) ensureOwnerWatch(pid types.PID, battleID string) {
	if !r.beginOwnerWatch(battleID) {
		return
	}
	cancel, err := r.dir.WatchOwner(r.ctx, pid.String(), func(owner string) {
		r.onOwnerChange(battleID, owner)
	})
	if err != nil {
		r.failOwnerWatch(battleID, err)
		return
	}
	r.attachOwnerWatch(battleID, cancel)
}

// beginOwnerWatch 抢占「建立监听」的权利：已有监听、正在建立或处于退避窗口内返回 false。
func (r *Resolver) beginOwnerWatch(battleID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	now := r.now()
	e := r.owners[battleID]
	if e == nil {
		e = &ownerEntry{seen: now}
		r.owners[battleID] = e
	}
	if e.cancel != nil || e.starting || now.Before(e.retryAt) {
		return false
	}
	e.starting = true
	return true
}

// failOwnerWatch 记录监听建立失败：该局此轮不缓存（解析照旧走两跳），按退避窗口择机重试。
func (r *Resolver) failOwnerWatch(battleID string, err error) {
	r.mu.Lock()
	if e := r.owners[battleID]; e != nil {
		e.starting, e.retryAt = false, r.now().Add(watchRetryInterval)
	}
	r.mu.Unlock()
	r.log.Debug("resolver: 属主监听建立失败（该局退回两跳查询）", "battle", battleID, "err", err)
}

// attachOwnerWatch 挂上监听取消函数；建立期间条目已被回收或接入层已停机则立刻取消。
func (r *Resolver) attachOwnerWatch(battleID string, cancel func()) {
	r.mu.Lock()
	e := r.owners[battleID]
	if e == nil || r.closed {
		r.mu.Unlock()
		cancel()
		return
	}
	e.starting, e.cancel = false, cancel
	r.mu.Unlock()
}

// onOwnerChange 处理目录事件：属主变了即**更新**映射（事件就是事实，不必等下一次查询）；
// 记录消失（空属主）即作废条目，下一次解析重新查目录。两种情况都通知接入层失效该局的 TTL 条目
// （拆流仍归 BackendGuard，两边互不越界）。
func (r *Resolver) onOwnerChange(battleID, owner string) {
	now := r.now()
	var stop func()
	r.mu.Lock()
	e := r.owners[battleID]
	if e == nil {
		r.mu.Unlock()
		return
	}
	if owner == "" {
		stop, e.cancel = e.cancel, nil
		delete(r.owners, battleID)
		r.mu.Unlock()
		if stop != nil {
			stop()
		}
		r.notify(battleID)
		return
	}
	changed := e.owner != owner
	e.owner, e.seen, e.eventAt = owner, now, now
	r.mu.Unlock()
	if changed {
		r.notify(battleID)
	}
}

// lookupFrame 查帧面实例快照：快照存在且未过期才算命中（过期按 stale 记账）。
func (r *Resolver) lookupFrame(nodeID string, now time.Time) (Instance, cacheLookup) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshot == nil {
		return Instance{}, lookupMiss
	}
	if now.After(r.snapshot.at.Add(DefaultFrameFreshTTL)) {
		return Instance{}, lookupStale
	}
	inst, ok := r.snapshot.byNode[nodeID]
	if !ok {
		return Instance{}, lookupMiss
	}
	return inst, lookupHit
}

// storeFrames 整份替换快照（注册中心给的就是全量列表，不做增量合并）；监听没建立时不写快照
// ——没有事件来源的快照会永久陈旧，宁可每次查询。
func (r *Resolver) storeFrames(list []Instance, at time.Time) {
	byNode := make(map[string]Instance, len(list))
	for _, inst := range list {
		byNode[inst.NodeID] = inst
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frameWatch == nil {
		return
	}
	r.snapshot = &frameSnapshot{byNode: byNode, at: at}
}

// ensureFrameWatch 确保实例监听已建立（幂等；全局只需一条，失败按退避窗口重试）。
func (r *Resolver) ensureFrameWatch() {
	r.mu.Lock()
	if r.closed || r.frameWatch != nil || r.frameStarting || r.now().Before(r.frameRetry) {
		r.mu.Unlock()
		return
	}
	r.frameStarting = true
	r.mu.Unlock()
	cancel, err := r.frames.WatchInstances(r.ctx, r.frameService, r.onFrames)
	if err != nil {
		r.mu.Lock()
		r.frameStarting, r.frameRetry = false, r.now().Add(watchRetryInterval)
		r.mu.Unlock()
		r.log.Debug("resolver: 帧面实例监听建立失败（退回每次查询注册中心）", "service", r.frameService, "err", err)
		return
	}
	r.mu.Lock()
	r.frameStarting = false
	if r.closed {
		r.mu.Unlock()
		cancel()
		return
	}
	r.frameWatch = cancel
	r.mu.Unlock()
}

// onFrames 处理实例变化：整份替换快照（实例上下线即时生效）。
func (r *Resolver) onFrames(list []Instance) {
	r.storeFrames(list, r.now())
}

// pickInstance 选属主节点上承载该传输面的帧面实例：快照命中即用；未命中/过期/该节点没有这个面
// 才查一次注册中心并整份回填（快照里有该节点却没有该面时也查——不猜，保持与旧实现同口径）。
func (r *Resolver) pickInstance(ctx context.Context, nodeID, face string) (Instance, cacheLookup, error) {
	inst, lookup := r.lookupFrame(nodeID, r.now())
	if lookup == lookupHit && inst.hasPort(face) {
		return inst, lookupHit, nil
	}
	if lookup == lookupHit {
		lookup = lookupMiss // 快照命中但没有这个面：仍要查一次注册中心
	}
	r.ensureFrameWatch()
	list, err := r.frames.Instances(ctx, r.frameService)
	if err != nil {
		return Instance{}, lookup, reject(fmt.Errorf("查询帧面实例（%s）失败: %w", r.frameService, err))
	}
	r.storeFrames(list, r.now())
	for _, inst := range list {
		if inst.NodeID == nodeID && inst.hasPort(face) {
			return inst, lookup, nil
		}
	}
	return Instance{}, lookup, reject(fmt.Errorf("节点 %s 上没有承载 %q 面的 %s 实例", nodeID, face, r.frameService))
}

// report 记一次命中率判定（每次解析恰好记一次；两层共用同一指标，不重复记账）。
func (r *Resolver) report(lookup cacheLookup) {
	switch lookup {
	case lookupHit:
		r.stats.Hit()
	case lookupStale:
		r.stats.Stale()
	default:
		r.stats.Miss()
	}
}

// OnResolveChanged 实现 edge.ResolveChangeNotifier：登记「该局解析结果已变」回调
// （接入层据此清掉该局的 TTL 条目）。传 nil 即注销；返回的注销函数幂等。
func (r *Resolver) OnResolveChanged(onChange func(battleID string)) func() {
	r.mu.Lock()
	r.onChange = onChange
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			r.onChange = nil
			r.mu.Unlock()
		})
	}
}

// notify 通知接入层「该局解析结果已变」：只失效它的 TTL 条目，不拆流（拆流仍归 BackendGuard）。
func (r *Resolver) notify(battleID string) {
	r.mu.Lock()
	onChange := r.onChange
	r.mu.Unlock()
	if onChange != nil {
		onChange(battleID)
	}
}

// sweep 回收空闲条目并取消其目录监听（正常结束的对局由目录删除事件即时回收，本函数是兜底）。
func (r *Resolver) sweep(now time.Time) {
	var stops []func()
	r.mu.Lock()
	for battleID, e := range r.owners {
		if now.Before(e.seen.Add(DefaultOwnerIdleTTL)) {
			continue
		}
		if e.cancel != nil {
			stops = append(stops, e.cancel)
		}
		delete(r.owners, battleID)
	}
	r.mu.Unlock()
	for _, stop := range stops {
		stop()
	}
}

// startSweeper 起空闲回收巡检（Close 时停止；事件驱动的正常回收不依赖它）。
func (r *Resolver) startSweeper() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(sweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				r.sweep(r.now())
			}
		}
	}()
}

// Close 停止全部监听并回收本地缓存（幂等；接入层停机时由装配层调用）。
// 每局目录监听与唯一的实例监听都必须在这里取消，否则停机后残留 watch 与 goroutine。
func (r *Resolver) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	stops := make([]func(), 0, len(r.owners)+1)
	for battleID, e := range r.owners {
		if e.cancel != nil {
			stops = append(stops, e.cancel)
		}
		delete(r.owners, battleID)
	}
	if r.frameWatch != nil {
		stops = append(stops, r.frameWatch)
		r.frameWatch = nil
	}
	r.snapshot = nil
	r.mu.Unlock()
	r.cancel()
	for _, stop := range stops {
		stop()
	}
	r.wg.Wait()
}

// 编译期断言：Resolver 同时满足接入层的 Resolver 与可选的「解析源头变化」通知接口。
var (
	_ edge.Resolver              = (*Resolver)(nil)
	_ edge.ResolveChangeNotifier = (*Resolver)(nil)
)
