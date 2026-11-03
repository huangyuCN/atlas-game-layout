// 下行增量编码（ver=3）的服务端实现：**全局单链**差分 + 逐连接发送基线。
//
// 设计见 docs/superpowers/specs/2026-11-03-downlink-delta-encoding.md（§3.2 字段、§3.5 单链、
// §4.3 恢复三层）。三条不变量由本文件守住：
//  1. 每 tick 至多两次编码：增量至多一次、全量至多一次（**严禁 per-client 差分**）；
//  2. 增量只表达相对上一 tick 的稀疏变化，合并结果必须与同帧全量编码逐字段等价；
//  3. 关键帧复用 ver=2 全量帧（FrameBroadcast），不新增第二种全量格式。

package stream

import (
	"bytes"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// VersionDelta 是帧广播增量载荷的帧头 ver（ver=3）：与 FramePayloadVersion 的 DELTA 一一对应。
// 帧协议常量表（atlas/transport/frame）只登记 ver=1/2，增量是**战斗域的载荷编码**，
// 版本常量因此留在本包，避免改动三语言帧协议产物（本期不改 TS/C#）。
const VersionDelta uint8 = 3

// DefaultKeyframeEvery 是周期关键帧的缺省间隔（tick）：10 Hz 下 30 s，字节开销约 0.3%。
const DefaultKeyframeEvery uint64 = 300

// DefaultKeyframeMinInterval 是每连接关键帧请求的最小间隔（§4.3：≥200 ms 一次，超频丢弃）。
const DefaultKeyframeMinInterval = 200 * time.Millisecond

// DeltaConfig 是增量编码的可调参数（0 值取缺省）。
type DeltaConfig struct {
	// KeyframeEvery 是周期关键帧间隔（tick），≤0 取 DefaultKeyframeEvery。
	KeyframeEvery uint64
	// KeyframeMinInterval 是每连接关键帧请求限流窗口，≤0 取 DefaultKeyframeMinInterval。
	KeyframeMinInterval time.Duration
}

// keyframeEvery 返回生效的周期关键帧间隔。
func (c DeltaConfig) keyframeEvery() uint64 {
	if c.KeyframeEvery == 0 {
		return DefaultKeyframeEvery
	}
	return c.KeyframeEvery
}

// keyframeInterval 返回生效的关键帧请求限流窗口。
func (c DeltaConfig) keyframeInterval() time.Duration {
	if c.KeyframeMinInterval <= 0 {
		return DefaultKeyframeMinInterval
	}
	return c.KeyframeMinInterval
}

// deltaLink 是全局单链的一环：上一 tick 的完整帧（只读，绝不改写）与它的帧号。
type deltaLink struct {
	frame *locksteppb.LockstepFrame
	seq   uint64
}

// clientState 是一条连接的发送基线（服务端自己的记账，不是对端 ACK）：
//   - lastSeq：我**发过**的最后一帧（≠ 他收到）——用于避免给没有基线的连接发增量；
//   - payloadVer：协商生效的载荷版本（JoinBattle 时写入）；
//   - keyframeRequested：客户端请求过关键帧（下一 tick 必给全量）；
//   - lastKeyframeReq：请求限流窗口的起点（每连接 ≥200ms 一次）。
type clientState struct {
	lastSeq           uint64
	payloadVer        battlev1.FramePayloadVersion
	keyframeRequested bool
	lastKeyframeReq   time.Time
}

// pushTarget 是一个目标的发送决策（ver + 全量/增量）。
type pushTarget struct {
	playerID string
	conn     Conn
	ver      uint8
	full     bool
}

// pushPlan 是一 tick 的发送计划：目标集合 + 是否需要编码增量/全量（各至多一次）。
type pushPlan struct {
	seq       uint64
	targets   []pushTarget
	needFull  bool
	needDelta bool
	delta     *battlev1.FrameBroadcastDelta
}

// sameInput 判定两条输入是否「逐字节相同」（**忽略 frame_id**：每 tick 都会被改写成当前帧，
// 无新输入者重复上一次输入正是增量的收益来源）。
func sameInput(a, b *locksteppb.LockstepInput) bool {
	if a.GetPlayerId() != b.GetPlayerId() {
		return false
	}
	if a.GetPredicted() != b.GetPredicted() {
		return false
	}
	if a.GetClientSendTimeUnixNano() != b.GetClientSendTimeUnixNano() {
		return false
	}
	return bytes.Equal(a.GetPayload(), b.GetPayload())
}

// inputsByPlayer 按 player_id 建索引（10 人量级，直接建表，不排序不拷贝 payload）。
func inputsByPlayer(inputs []*locksteppb.LockstepInput) map[string]*locksteppb.LockstepInput {
	out := make(map[string]*locksteppb.LockstepInput, len(inputs))
	for _, in := range inputs {
		out[in.GetPlayerId()] = in
	}
	return out
}

// diffFrames 生成「prev → cur」的增量：字段顺序固定（SERVER_TIME、INPUTS、SNAPSHOT），
// 未变化的字段不出现。prev 必须非空且 cur.frame_id = prev.frame_id + 1。
//
// state_hash 与 seq 同级：**每帧必带当帧值**（cur 的），不进 changed——锁步防失步靠它。
// 这里只做**复制**，绝不在 diff 里重算哈希：hash 依赖全量状态，属于帧聚合的产物。
func diffFrames(prev, cur *locksteppb.LockstepFrame) *battlev1.FrameBroadcastDelta {
	d := &battlev1.FrameBroadcastDelta{
		BaseSeq:   prev.GetFrameId(),
		Seq:       cur.GetFrameId(),
		StateHash: cur.GetStateHash(), // 当帧（不是基线）hash，直接复用聚合已算好的值
	}

	if !sameServerTime(prev.GetServerTime(), cur.GetServerTime()) {
		d.Changed = append(d.Changed, battlev1.FrameDeltaField_FRAME_DELTA_FIELD_SERVER_TIME)
		if ts := cur.GetServerTime(); ts != nil {
			d.ServerTimeUnixNano = ts.AsTime().UnixNano()
		}
	}

	prevBy := inputsByPlayer(prev.GetInputs())
	curBy := inputsByPlayer(cur.GetInputs())
	for _, in := range cur.GetInputs() {
		if old, ok := prevBy[in.GetPlayerId()]; ok && sameInput(old, in) {
			continue
		}
		d.SetInputs = append(d.SetInputs, in)
	}
	for id := range prevBy {
		if _, ok := curBy[id]; !ok {
			d.RemovedPlayerIds = append(d.RemovedPlayerIds, id)
		}
	}
	if len(d.SetInputs) > 0 || len(d.RemovedPlayerIds) > 0 {
		d.Changed = append(d.Changed, battlev1.FrameDeltaField_FRAME_DELTA_FIELD_INPUTS)
	}

	if !sameSnapshot(prev.GetSnapshot(), cur.GetSnapshot()) {
		d.Changed = append(d.Changed, battlev1.FrameDeltaField_FRAME_DELTA_FIELD_SNAPSHOT)
		d.Snapshot = cur.GetSnapshot() // nil = 本帧无快照（清空），这是唯一的表达方式
	}
	return d
}

// sameServerTime 比较两帧的服务端时间戳（按纳秒精度；两侧都为空也算相同）。
func sameServerTime(a, b *timestamppb.Timestamp) bool { return proto.Equal(a, b) }

// sameSnapshot 比较两帧快照（nil 与空消息视为相同，其余逐字段相等）。
func sameSnapshot(a, b *locksteppb.SnapshotMeta) bool {
	if a == nil || b == nil {
		return a.GetFrameId() == b.GetFrameId() && bytes.Equal(a.GetStateHash(), b.GetStateHash()) &&
			a.GetSizeBytes() == b.GetSizeBytes() && a.GetStorageKey() == b.GetStorageKey()
	}
	return proto.Equal(a, b)
}

// isDatagram 判定某帧面是否为裸数据报面（UDP）：该面**永不发增量**（单包必须自包含）。
func isDatagram(kind transport.Kind) bool { return kind == transport.KindUDP }

// NewRegistryDelta 构造「protobuf 全量 + ver=3 增量」注册表（增量开关打开时的唯一装配入口）。
//
// 开启前提是 protobuf 全量（ver=2）：关键帧就是 ver=2 的 FrameBroadcast，不另立全量格式。
// 不自洽即**装配期失败**（不是运行期降级）——让配置错误在启动时暴露。
func NewRegistryDelta(ttl time.Duration, cfg DeltaConfig) (*Registry, error) {
	r, err := NewRegistryProtobuf(ttl)
	if err != nil {
		return nil, err
	}
	r.delta = true
	r.keyEvery = cfg.keyframeEvery()
	r.keyInterval = cfg.keyframeInterval()
	return r, nil
}

// buildPlan 组装本 tick 的发送计划（持锁读连接与逐连接状态，不做任何编码）。
// 计划一旦确定，整 tick 只需 encode 增量/全量各至多一次。
func (r *Registry) buildPlan(battleID string, players []string, f *locksteppb.LockstepFrame) pushPlan {
	seq := f.GetFrameId()
	plan := pushPlan{seq: seq}
	r.mu.Lock()
	defer r.mu.Unlock()
	link := r.links[battleID]
	linkOK := link != nil && link.seq+1 == seq
	for _, playerID := range players {
		c, ok := r.conns[playerID]
		if !ok {
			continue // 没有直连就没目标（不编码、不记账）
		}
		ver, full := r.decide(c.Kind, r.states[playerID], seq, linkOK)
		if full {
			plan.needFull = true
			ver = r.pushVer
		} else {
			plan.needDelta = true
		}
		plan.targets = append(plan.targets, pushTarget{playerID: playerID, conn: c, ver: ver, full: full})
	}
	if plan.needDelta {
		plan.delta = diffFrames(link.frame, f)
	}
	return plan
}

// decide 决定一条连接本 tick 的 (帧头 ver, 是否全量)：
//   - 未开增量 / 裸 UDP 面 / 未协商到 ver=3 ⇒ 全量（udp 客户端零改动）；
//   - 已协商但服务端链断（新连接/上帧没发成）/ 客户端请求过关键帧 / 周期兜底 ⇒ 全量；
//   - 其余 ⇒ 增量。
func (r *Registry) decide(kind transport.Kind, st *clientState, seq uint64, linkOK bool) (uint8, bool) {
	if !r.delta || isDatagram(kind) || st == nil || st.payloadVer != battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_DELTA {
		return r.pushVer, true
	}
	periodic := r.keyEvery > 0 && seq%r.keyEvery == 0
	if !linkOK || st.lastSeq+1 != seq || st.keyframeRequested || periodic {
		return r.pushVer, true
	}
	return VersionDelta, false
}

// commitSend 记下「这一帧已经发给这条连接」：只在该连接仍是当前连接时记账
// （期间被新连接接管则不写，免得给接管者伪造基线）。
func (r *Registry) commitSend(playerID string, c Conn, seq uint64, full bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.conns[playerID]; !ok || cur != c {
		return
	}
	st := r.states[playerID]
	if st == nil {
		st = &clientState{}
		r.states[playerID] = st
	}
	st.lastSeq = seq
	if full {
		st.keyframeRequested = false
	}
}

// advanceLink 把全局单链推进到本 tick（存**只读引用**：该帧此后不再被改写）。
func (r *Registry) advanceLink(battleID string, f *locksteppb.LockstepFrame) {
	r.mu.Lock()
	r.links[battleID] = &deltaLink{frame: f, seq: f.GetFrameId()}
	r.mu.Unlock()
}

// NegotiatePayloadVersion 实现 biz.BattlePusher：按「服务端允许 ∩ 客户端声明」取最高版本，
// 并把结果记为该连接的生效版本（回执里下发）。未开增量 / 裸 UDP 面**永远**回落到全量版本。
func (r *Registry) NegotiatePayloadVersion(playerID string, accepted []battlev1.FramePayloadVersion) battlev1.FramePayloadVersion {
	r.mu.Lock()
	defer r.mu.Unlock()
	best := highestAllowed(accepted, r.allowedVersion())
	if best == battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_DELTA {
		c, ok := r.conns[playerID]
		if !ok || isDatagram(c.Kind) { // 裸数据报面永不发增量；未登记连接没有基线
			best = battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_PROTOBUF
		}
	}
	if best == battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_DELTA {
		r.stateLocked(playerID).payloadVer = best
	}
	return best
}

// allowedVersion 返回本节点允许下发的最高载荷版本（开关决定，运行期只读）。
func (r *Registry) allowedVersion() battlev1.FramePayloadVersion {
	if r.delta {
		return battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_DELTA
	}
	return versionOfVer(r.pushVer)
}

// RequestKeyframe 实现 biz.BattlePusher：记录该连接的关键帧请求（幂等 Tell）。
// 每条连接 ≥200ms 一次限流，超频直接丢弃并返回 false（不回错，避免客户端放大请求）。
func (r *Registry) RequestKeyframe(playerID string, _ uint64) bool {
	if !r.delta {
		return false // 未开增量的节点没有增量链，请求无意义（静默忽略）
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.conns[playerID]; !ok {
		return false
	}
	st := r.stateLocked(playerID)
	now := time.Now()
	if !st.lastKeyframeReq.IsZero() && now.Sub(st.lastKeyframeReq) < r.keyInterval {
		return false
	}
	st.lastKeyframeReq = now
	st.keyframeRequested = true
	return true
}

// stateLocked 取（或懒建）该连接的状态；调用方必须已持锁。
func (r *Registry) stateLocked(playerID string) *clientState {
	st := r.states[playerID]
	if st == nil {
		st = &clientState{}
		r.states[playerID] = st
	}
	return st
}

// resetState 丢弃该连接的发送基线（换连接/注销时必须重置：新连接没有基线，首帧必全量）。
func (r *Registry) resetState(playerID string) { delete(r.states, playerID) }

// highestAllowed 返回「客户端声明 ∩ 服务端允许（≤ allowed）」的最高版本：
// 缺省声明（空）只认 ver=1（老客户端行为零变化）；空交集回落 ver=1（有输出版本可解）。
func highestAllowed(accepted []battlev1.FramePayloadVersion, allowed battlev1.FramePayloadVersion) battlev1.FramePayloadVersion {
	if len(accepted) == 0 {
		return battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_PROTOJSON
	}
	best := battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_UNSPECIFIED
	for _, v := range accepted {
		if v > best && v <= allowed {
			best = v
		}
	}
	if best == battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_UNSPECIFIED {
		return battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_PROTOJSON
	}
	return best
}

// versionOfVer 把帧头 ver 映射为载荷版本枚举（两者必须一一对应，不得另立字面量）。
func versionOfVer(ver uint8) battlev1.FramePayloadVersion {
	if ver == VersionDelta {
		return battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_DELTA
	}
	if ver == frame.Version2 {
		return battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_PROTOBUF
	}
	return battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_PROTOJSON
}
