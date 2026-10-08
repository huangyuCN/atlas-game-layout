package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// Registry 是 battle 节点的直连连接注册表：player_id → 当前连接（每玩家一条）+ 端点反查表，
// 并实现 biz.BattlePusher（推送/关闭）、biz.ConnPresence（在场复核）与 biz.SettleLedger（结算留档）。
// 登记时机是**帧槽验票通过**（JoinBattle 本身也是带票帧，故其到达即完成登记）；
// 断开事件由帧引擎连接生命周期钩子经 Bridge 转成 actor 消息（批次 6）。
type Registry struct {
	mu        sync.Mutex
	conns     map[string]Conn          // playerID → 当前连接
	endpoints map[Endpoint]endpointRef // 端点 → 玩家与对局（断开事件反查）
	ports     map[transport.Kind]Port  // 帧面类型 → 直连端口（装配期挂载）
	ended     *EndedBook               // 结束留档：懒激活前拒绝迟到 op + 重连补投
}

// endpointRef 是端点反查项：断开事件据此找到玩家与对局（对局用于把消息投给正确的 actor）。
type endpointRef struct {
	playerID string
	battleID string
}

// 静态保证：注册表即 battle 的直连推送端口与结算留档端口。
var (
	_ biz.BattlePusher = (*Registry)(nil)
	_ biz.SettleLedger = (*Registry)(nil)
)

// NewRegistry 构造空注册表（帧面端口在服务端构造完成后经 BindPort 挂载；
// 结束留档 TTL 取 DefaultEndedTTL）。
func NewRegistry() *Registry { return NewRegistryWithTTL(DefaultEndedTTL) }

// NewRegistryWithTTL 构造空注册表并指定结束留档 TTL（≤0 取 DefaultEndedTTL）。
func NewRegistryWithTTL(ttl time.Duration) *Registry {
	return &Registry{
		conns:     make(map[string]Conn),
		endpoints: make(map[Endpoint]endpointRef),
		ports:     make(map[transport.Kind]Port),
		ended:     NewEndedBook(ttl),
	}
}

// RecordEnded 实现 biz.SettleLedger：留档一局战斗的结算结果（墓碑 + 胜负 + 参战名单），
// 帧面据此在懒激活之前拒绝迟到的帧 op，并在玩家重连后补投结果（幂等）。
func (r *Registry) RecordEnded(battleID, winner string, players []string) {
	r.ended.Record(battleID, winner, players)
}

// Ended 判定该对局是否已结束（结束留档未过期即真）：帧面身份解析在**投递之前**问它，
// 免得 SpawnAuto 重建一个空名单实例（规格 §9.8：结算后不得再有悬挂连接与重建）。
func (r *Registry) Ended(battleID string) bool { return r.ended.Ended(battleID) }

// ReplayEnd 向指定连接补投留档的结算结果（重连后补投，规格：结算结果不得因丢包而永久丢失）：
// 幂等（同一条通知可重复到达）、有界（每玩家最多 MaxEndReplays 次）、名单外不投。
// **不关闭连接**：关闭只发生在结算那一次，迟到的 op 不得再次触发关连接。
func (r *Registry) ReplayEnd(playerID string, c Conn) bool {
	winner, ok := r.ended.Claim(c.BattleID, playerID)
	if !ok {
		return false
	}
	_ = r.pushTo(c, battlev1opclient.BattleServicePushOps.BattleEndNotify,
		&battlev1.BattleEndNotify{BattleId: c.BattleID, WinnerPlayerId: winner})
	return true
}

// BindPort 挂载一类帧面的直连端口（装配期调用；nil 端口忽略）。
func (r *Registry) BindPort(kind transport.Kind, p Port) {
	if p == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ports[kind] = p
}

// Register 登记玩家的当前连接（每次验票通过都会调用），并返回**连接是否变化**
// （首次登记或换连接接管为真；逐帧重复登记同一连接为假——调用方据此判定上线事件）：
// 同一玩家的新连接**接管**旧连接——旧连接被主动关闭并从表中移除（规格 §9.4 重连接管）。
// 旧连接的端点反查项**保留**：帧引擎随后必然回调一次断开事件，届时要靠它反查玩家
// （推送失败注销同理——不保留就丢掉了玩家真正掉线的最后一个信号）。
func (r *Registry) Register(playerID string, c Conn) bool {
	r.mu.Lock()
	old, existed := r.conns[playerID]
	changed := !existed || old != c
	r.conns[playerID] = c
	if changed { // 逐帧重复登记（同一连接）不动反查表：热路径上少一次写
		r.endpoints[c.Endpoint()] = endpointRef{playerID: playerID, battleID: c.BattleID}
	}
	r.mu.Unlock()
	if existed && old != c {
		_ = r.closeConn(old)
	}
	return changed
}

// Unregister 注销玩家的连接：仅当表中记录仍是该连接时才移除，
// 避免旧连接的迟到注销摘掉已接管的新连接。
// 端点反查项**不在这里清理**：真正的掉线信号是帧引擎随后必然回调的断开事件，
// 它还要靠反查项找到玩家（推送给已死连接失败只是掉线的征兆，摘早了就丢掉最后一个信号）。
func (r *Registry) Unregister(playerID string, c Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.conns[playerID]; ok && cur == c {
		delete(r.conns, playerID)
	}
}

// Online 实现 biz.ConnPresence：返回该玩家当前是否有存活直连
// （应用掉线事件前的复核口径，规格 §9.2 硬约束②）。
func (r *Registry) Online(playerID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.conns[playerID]
	return ok
}

// Disconnect 处理帧引擎的端点断开：反查端点所属玩家与对局并摘除该端点索引；
// 该端点若正是玩家的当前连接则一并摘除（玩家此后不在场）。
// 第三个返回值为假即未登记端点（匿名连接），调用方忽略该事件。
func (r *Registry) Disconnect(ep Endpoint) (playerID, battleID string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	owner, ok := r.endpoints[ep]
	if !ok {
		return "", "", false
	}
	delete(r.endpoints, ep)
	if cur, live := r.conns[owner.playerID]; live && cur.Endpoint() == ep {
		delete(r.conns, owner.playerID)
	}
	return owner.playerID, owner.battleID, true
}

// Count 返回当前登记的连接数（观测与测试用）。
func (r *Registry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.conns)
}

// CountBattle 返回指定对局当前登记的连接数（观测与测试用）。
func (r *Registry) CountBattle(battleID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.conns {
		if c.BattleID == battleID {
			n++
		}
	}
	return n
}

// CloseBattle 关闭并移除本局全部直连（结算后回收，规格 §9.8：禁止结算后悬挂连接）。
// 结算是对局的终点，故连端点反查项一并清理：帧引擎随后回调的断开事件不再反查到玩家，
// 不会向已停的 actor 投递无意义的掉线消息。
//
// 关闭前对每条仍未确认的连接重投 EndRetries 次结算通知：数据报面（KCP/UDP）没有重传，
// 单次推送丢包就等于玩家永远不知道结果（重连补投覆盖的是「玩家还会再来」的那部分）。
// 本方法每局只被调用一次（结算是单次的），故关闭也**确定性发生一次**。
func (r *Registry) CloseBattle(battleID string) {
	r.mu.Lock()
	victims := make([]Conn, 0, len(r.conns))
	for playerID, c := range r.conns {
		if c.BattleID == battleID {
			victims = append(victims, c)
			delete(r.conns, playerID)
			delete(r.endpoints, c.Endpoint())
		}
	}
	r.mu.Unlock()
	winner, recorded := r.ended.Winner(battleID)
	for _, c := range victims {
		r.repushEnd(c, winner, recorded)
		_ = r.closeConn(c)
	}
}

// repushEnd 向一条未确认连接有界重投结算通知（未留档即跳过：不发无胜者的空通知）。
func (r *Registry) repushEnd(c Conn, winner string, recorded bool) {
	if !recorded {
		return
	}
	msg := &battlev1.BattleEndNotify{BattleId: c.BattleID, WinnerPlayerId: winner}
	for i := 0; i < EndRetries; i++ {
		_ = r.pushTo(c, battlev1opclient.BattleServicePushOps.BattleEndNotify, msg)
	}
}

// PublishFrame 实现 biz.BattlePusher：把一帧以 FrameBroadcast 直发该玩家的当前连接。
func (r *Registry) PublishFrame(_ context.Context, playerID, battleID string, frame *locksteppb.LockstepFrame) error {
	return r.push(playerID, battlev1opclient.BattleServicePushOps.FrameBroadcast,
		&battlev1.FrameBroadcast{BattleId: battleID, Frame: frame})
}

// PublishEnd 实现 biz.BattlePusher：把战斗结束通知直发该玩家的当前连接。
func (r *Registry) PublishEnd(_ context.Context, playerID, battleID, winner string) error {
	return r.push(playerID, battlev1opclient.BattleServicePushOps.BattleEndNotify,
		&battlev1.BattleEndNotify{BattleId: battleID, WinnerPlayerId: winner})
}

// PublishOut 实现 biz.BattlePusher：把玩家出局通知直发该玩家的当前连接（掉线判负，规格 §9.3）。
func (r *Registry) PublishOut(_ context.Context, playerID, battleID, outPlayerID string, reason battlev1.PlayerOutReason) error {
	return r.push(playerID, battlev1opclient.BattleServicePushOps.PlayerOutNotify,
		&battlev1.PlayerOutNotify{BattleId: battleID, PlayerId: outPlayerID, Reason: reason})
}

// push 向玩家当前连接推送一条 Notify：未登记即跳过（返回 nil——直连未建立不是错误），
// 端口未挂载即报错（装配期不一致不该在请求期静默）；连接已死按失效注销。
func (r *Registry) push(playerID, operation string, msg any) error {
	r.mu.Lock()
	c, ok := r.conns[playerID]
	port := r.ports[c.Kind]
	r.mu.Unlock()
	if !ok {
		return nil
	}
	if port == nil {
		return fmt.Errorf("stream: 帧面 %s 未挂载推送端口（装配期漏挂）", c.Kind)
	}
	err := port.Push(c, operation, msg)
	if errors.Is(err, engine.ErrConnNotFound) || errors.Is(err, engine.ErrPeerNotFound) {
		r.Unregister(playerID, c) // 连接已死：按失效注销，避免表里堆积
	}
	return err
}

// pushTo 向**指定**连接推送一条 Notify：重连补投与关闭前重投走它（不经「当前连接」查表，
// 也不做失效注销——这两条路径的连接本就不在表内或马上要被移除）。
func (r *Registry) pushTo(c Conn, operation string, msg any) error {
	r.mu.Lock()
	port := r.ports[c.Kind]
	r.mu.Unlock()
	if port == nil {
		return fmt.Errorf("stream: 帧面 %s 未挂载推送端口（装配期漏挂）", c.Kind)
	}
	return port.Push(c, operation, msg)
}

// closeConn 关闭一条连接（端口未挂载即忽略——只登记不关闭的形态仍可用）。
func (r *Registry) closeConn(c Conn) error {
	r.mu.Lock()
	port := r.ports[c.Kind]
	r.mu.Unlock()
	if port == nil {
		return nil
	}
	return port.Close(c)
}
