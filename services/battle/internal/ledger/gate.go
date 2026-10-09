// 激活闸门（框架 cluster.ActivationGate 的实现）：把「跨节点留档」变成**任何节点**的
// 激活准入——已结束的对局不得被迟到帧 op / rpc 面调用 / 重投的激活请求复活。

package ledger

import (
	"context"
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

// Gate 是 battle 的激活闸门：对本服务 actor 类型的 PID，在创建 cell 之前查一次共享留档，
// 命中即以稳定 reason（BATTLE_ENDED）拒绝激活。
type Gate struct {
	store     Store
	actorType string
	meter     metrics.Collector
	timeout   time.Duration
}

// NewGate 构造激活闸门（store 为 nil 或 actorType 为空时退化为「一律放行」；
// meter 为 nil 时退化为 noop）。
func NewGate(store Store, actorType string, meter metrics.Collector) *Gate {
	if meter == nil {
		meter = metrics.Noop()
	}
	return &Gate{store: store, actorType: actorType, meter: meter, timeout: gateLookupTimeout}
}

// AllowActivation 实现 cluster.ActivationGate：返回 nil 放行，返回错误即拒绝本次激活
// （错误原样上抛给调用方，故用结构化错误给出稳定 reason）。
//
// 两条口径：
//   - 只对本服务的 actor 类型生效：别的 PID 不查留档（闸门挂在全局运行时上，不能误伤其他域）；
//   - 查询失败**失败开放**（放行 + 计数 + 告警）：留档是兜底防线，而闸门在每次激活都会问一次，
//     存储（etcd）抖动时失败关闭会把**所有新对局**的激活一起打死（可用性事故）；
//     放行的代价是已结束的对局可能在留档 TTL 窗口内被复活一次，而复活实例恢复快照后
//     立刻再次结算并自停（不产生状态分叉），属于可自愈的有界退化。
func (g *Gate) AllowActivation(ctx context.Context, pid types.PID) error {
	if g == nil || g.store == nil || g.actorType == "" || pid.Type() != g.actorType {
		return nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	entry, ok, err := g.store.Lookup(lookupCtx, pid.UID())
	if err != nil {
		g.counter(MetricLookupFailed).Add(1)
		atlaslog.Warn("battle: 激活闸门查询留档失败（按放行处理）", "pid", pid.String(), "err", err)
		return nil
	}
	if !ok {
		return nil
	}
	g.counter(MetricActivationRefused).Add(1)
	return errorv1.ErrBattleEnded("对局 %s 已结束（跨节点留档：胜者 %q，拒绝复活）", pid.UID(), entry.Winner)
}

// counter 按名字取计数器句柄（闸门路径低频，不做句柄缓存）。
func (g *Gate) counter(name string) metrics.Counter { return g.meter.Counter(name) }
