// 激活闸门（框架 cluster.ActivationGate 的实现）：把「跨节点留档」变成**任何节点**的
// 激活准入——已结束的对局不得被迟到帧 op / rpc 面调用 / 重投的激活请求复活。

package ledger

import (
	"context"
	"sync"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/metrics"
)

// 闸门指标名（口径见各处注释）。
const (
	// MetricActivationRefused 是「因已留档（终态）被拒绝激活」的累计次数（counter）。
	MetricActivationRefused = "battle_activation_refused_total"
	// MetricLookupFailed 是闸门查询留档失败的累计次数（counter）：查询失败按**放行**处理，
	// 该计数用于发现存储抖动（放行的代价见 Gate.AllowActivation 注释）。
	MetricLookupFailed = "battle_ledger_lookup_failed_total"
)

// gateLookupTimeout 是闸门单次查询留档的时限：闸门在**激活路径**上同步询问，不能无限等
// （存储慢时按查询失败处理＝放行，见 AllowActivation）。
const gateLookupTimeout = time.Second

// 负缓存窗口的缺省值与硬上界（诊断 §5 修法 1）。
//
// 为什么窗口必须很短：缓存的是「尚未留档」这个判定，而留档是随时可能写入的——窗口越长，
// 「窗口内刚结算的局又被激活一次」的放行区间就越宽。200ms 的选择依据：它只覆盖
// 「同一局在激活路径上的连续敲门」（重投/迟到 op 的突发窗口），远小于票据 TTL 与留档 TTL，
// 因此现实中不存在被它挡住或被它放行的合法请求。
const (
	// DefaultNegativeCacheTTL 是 miss 负缓存的缺省窗口。
	DefaultNegativeCacheTTL = 200 * time.Millisecond
	// MaxNegativeCacheTTL 是负缓存窗口的硬上界：配置只能收紧或关闭，放大它在语义上不成立。
	MaxNegativeCacheTTL = 200 * time.Millisecond
)

// GateOption 配置闸门（负缓存窗口与时钟）。
type GateOption func(*Gate)

// WithNegativeCacheTTL 设置 miss 负缓存窗口：≤0 关闭（逐次查存储），超过
// MaxNegativeCacheTTL 夹到上界（「≤200ms」是硬约定）。
func WithNegativeCacheTTL(d time.Duration) GateOption {
	return func(g *Gate) {
		switch {
		case d <= 0:
			g.negTTL = 0
		case d > MaxNegativeCacheTTL:
			g.negTTL = MaxNegativeCacheTTL
		default:
			g.negTTL = d
		}
	}
}

// WithGateClock 注入时钟（测试用，避免真等窗口）；nil 取 time.Now。
func WithGateClock(now func() time.Time) GateOption {
	return func(g *Gate) {
		if now != nil {
			g.now = now
		}
	}
}

// Gate 是 battle 的激活闸门：对本服务 actor 类型的 PID，在创建 cell 之前查共享留档，
// 命中即以稳定 reason（BATTLE_ENDED）拒绝激活；miss 判定进短负缓存（见 AllowActivation）。
type Gate struct {
	store     Store
	actorType string
	meter     metrics.Collector
	timeout   time.Duration
	negTTL    time.Duration // miss 负缓存窗口（0 = 关闭）
	now       func() time.Time

	// 两条计数器在装配期取定并预注册（见 NewGate），闸门路径只做一次 Add。
	refused      metrics.Counter
	lookupFailed metrics.Counter

	mu        sync.Mutex
	missUntil map[string]time.Time // battleID → miss 判定过期时刻（只缓存 miss）
}

// NewGate 构造激活闸门（store 为 nil 或 actorType 为空时退化为「一律放行」；
// meter 为 nil 时退化为 noop）。
func NewGate(store Store, actorType string, meter metrics.Collector, opts ...GateOption) *Gate {
	if meter == nil {
		meter = metrics.Noop()
	}
	g := &Gate{
		store: store, actorType: actorType, meter: meter,
		timeout: gateLookupTimeout, negTTL: DefaultNegativeCacheTTL, now: time.Now,
		missUntil: make(map[string]time.Time),
	}
	for _, o := range opts {
		o(g)
	}
	// 装配期预注册两条计数器（记 0）：空闲 battle 的 /metrics 上也要能看到这两个家族，
	// 否则「抓不到」会被读成「没有拒绝发生」——本轮排障正是踩在这条盲区上。
	g.refused = metrics.PreRegisterCounter(meter, MetricActivationRefused)
	g.lookupFailed = metrics.PreRegisterCounter(meter, MetricLookupFailed)
	return g
}

// negativeCacheTTL 返回生效的负缓存窗口（负缓存用例断言用）。
func (g *Gate) negativeCacheTTL() time.Duration { return g.negTTL }

// AllowActivation 实现 cluster.ActivationGate：返回 nil 放行，返回错误即拒绝本次激活
// （错误原样上抛给调用方，故用结构化错误给出稳定 reason）。
//
// 三条口径：
//   - 只对本服务的 actor 类型生效：别的 PID 不查留档（闸门挂在全局运行时上，不能误伤其他域）；
//   - 查询失败**失败开放**（放行 + 计数 + 告警）：留档是兜底防线，而闸门在每次激活都会问一次，
//     存储（etcd）抖动时失败关闭会把**所有新对局**的激活一起打死（可用性事故）；
//     放行的代价是已结束的对局可能在留档 TTL 窗口内被复活一次，而复活实例恢复快照后
//     立刻再次结算并自停（不产生状态分叉），属于可自愈的有界退化。
//   - miss 判定短缓存（窗口见 DefaultNegativeCacheTTL）：同一局在窗口内重复敲门只查一次存储；
//     **命中（已结算）绝不缓存**——已结算的判定必须每次实时重查，不能被本地缓存掩盖。
//     查询失败也不缓存（失败开放不得被缓存放大成窗口内「一直放行」）。
func (g *Gate) AllowActivation(ctx context.Context, pid types.PID) error {
	if g == nil || g.store == nil || g.actorType == "" || pid.Type() != g.actorType {
		return nil
	}
	uid := pid.UID()
	if g.missCached(uid) {
		return nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	entry, ok, err := g.store.Lookup(lookupCtx, uid)
	if err != nil {
		g.lookupFailed.Add(1)
		atlaslog.Warn("battle: 激活闸门查询留档失败（按放行处理）", "pid", pid.String(), "err", err)
		return nil
	}
	if !ok {
		g.cacheMiss(uid)
		return nil
	}
	g.refused.Add(1)
	return errorv1.ErrBattleEnded("对局 %s 已结束（跨节点留档：胜者 %q，拒绝复活）", uid, entry.Winner)
}
