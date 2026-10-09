// 帧面观测口径（P1-4）：票据/身份校验失败按 reason 计数、帧 op 耗时直方图与失败按 reason 计数，
// 并在拒绝路径打一条带 `stream_id` 的关联日志。
//
// 口径与接入层呼应：接入层 `edge_ticket_rejected_total{reason=…}` 管 L4 验票与限流，
// battle `battle_frame_ticket_rejected_total{reason=…}` 管**到了帧面之后**的票据/身份拒绝；
// 两侧都按 reason 分序列，排障时按玩家与对局对齐。

package server

import (
	"context"
	"sync"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// 帧面指标名（口径对外可见：面板与告警按它们取数，改名即破坏下游）。
const (
	// MetricTicketRejected 是按 reason 计数的帧面票据/身份校验失败次数（counter）。
	MetricTicketRejected = "battle_frame_ticket_rejected_total"
	// MetricOpDuration 是帧 op 处理耗时（秒；histogram）。
	MetricOpDuration = "battle_frame_op_duration_seconds"
	// MetricOpErrors 是按 operation + reason 计数的帧 op 失败次数（counter）。
	MetricOpErrors = "battle_frame_op_errors_total"
	// MetricReplyOversize 是回执超过单包上限（16 KiB）被拦下的次数（counter，按 operation）。
	// 与接入层 edge_downlink_dropped_total{reason=oversize} 呼应：battle 侧拦住、接入层兜底。
	MetricReplyOversize = "battle_frame_reply_oversize_total"
)

// 标签键与兜底取值。
const (
	// labelReason 是拒绝/失败的原因标签键（与接入层的 LabelReason 同名同义）。
	labelReason = "reason"
	// labelOperation 是帧 op 全名的标签键。
	labelOperation = "operation"
	// reasonUnknown 是拿不到稳定 reason 时的兜底标签值：限基数，不把自由文本写进标签。
	reasonUnknown = "UNKNOWN"
)

// FrameMetrics 是帧面的观测出口：把票据拒绝、op 耗时/失败与超包回执写进指标采集器。
// collector 为 nil 时退化为 noop（装配侧不必判空）；句柄按标签缓存，热路径不反复分配。
type FrameMetrics struct {
	collector metrics.Collector

	mu       sync.Mutex
	ticket   map[string]metrics.Counter
	duration map[string]metrics.Histogram
	failures map[string]metrics.Counter
	oversize map[string]metrics.Counter
}

// NewFrameMetrics 构造帧面观测出口（collector 为 nil 时取 noop）。
func NewFrameMetrics(collector metrics.Collector) *FrameMetrics {
	if collector == nil {
		collector = metrics.Noop()
	}
	return &FrameMetrics{
		collector: collector,
		ticket:    make(map[string]metrics.Counter),
		duration:  make(map[string]metrics.Histogram),
		failures:  make(map[string]metrics.Counter),
		oversize:  make(map[string]metrics.Counter),
	}
}

// Hooks 返回帧 op 服务端的消费方副作用：身份解析失败（票据/身份类拒绝）按 reason 计数并留日志。
func (m *FrameMetrics) Hooks() frameops.Hooks {
	return frameops.Hooks{PostDeliver: m.postDeliver}
}

// Wrap 包装一个已注册的帧 op handler：观测处理耗时（成功与失败都观测）并在失败时按
// operation + reason 计数；回执与错误原样透传（观测不得改变协议行为）。
func (m *FrameMetrics) Wrap(operation string, h engine.MsgHandler) engine.MsgHandler {
	return func(ctx context.Context, dec engine.Decoder, enc engine.Encoder) ([]byte, error) {
		start := time.Now()
		out, err := h(ctx, dec, enc)
		m.durationOf(operation).Observe(time.Since(start).Seconds())
		if err != nil {
			m.failureOf(operation, reasonOf(err)).Add(1)
		}
		return out, err
	}
}

// postDeliver 实现 frameops.Hooks.PostDeliver：**身份未解析出来**的失败（票据缺失/非法/过期、
// 已结束对局）按 reason 计数，并打一条带 `stream_id` 与 operation 的关联日志；
// 身份已解析后的失败（目标不一致、投递失败）归 op 口径（Wrap），避免同一失败两处计数。
func (m *FrameMetrics) postDeliver(ctx context.Context, entry relay.RouteEntry, id frameops.Identity, err error) {
	if m == nil || err == nil || id != (frameops.Identity{}) {
		return
	}
	reason := reasonOf(err)
	m.ticketOf(reason).Add(1)
	atlaslog.Warn("battle: 帧面票据校验失败",
		"stream_id", streamIDOf(ctx), "operation", entry.Operation, "reason", reason,
		"player_id", id.PlayerID, "battle_id", id.BattleID)
}

// ticketOf 返回该 reason 的票据拒绝计数器（按 reason 缓存句柄）。
func (m *FrameMetrics) ticketOf(reason string) metrics.Counter {
	return handleOf(m, m.ticket, reason, func() metrics.Counter {
		return m.collector.Counter(MetricTicketRejected, labelReason, reason)
	})
}

// durationOf 返回该 op 的耗时直方图（按 operation 缓存句柄）。
func (m *FrameMetrics) durationOf(operation string) metrics.Histogram {
	return handleOf(m, m.duration, operation, func() metrics.Histogram {
		return m.collector.Histogram(MetricOpDuration, labelOperation, operation)
	})
}

// failureOf 返回该 op + reason 的失败计数器（按组合缓存句柄）。
func (m *FrameMetrics) failureOf(operation, reason string) metrics.Counter {
	return handleOf(m, m.failures, operation+"\x00"+reason, func() metrics.Counter {
		return m.collector.Counter(MetricOpErrors, labelOperation, operation, labelReason, reason)
	})
}

// oversizeOf 返回该 op 的超包回执计数器（按 operation 缓存句柄）。
func (m *FrameMetrics) oversizeOf(operation string) metrics.Counter {
	return handleOf(m, m.oversize, operation, func() metrics.Counter {
		return m.collector.Counter(MetricReplyOversize, labelOperation, operation)
	})
}

// handleOf 是四类指标句柄的取用口径：加锁 → 查缓存 → 未命中即创建。集中一处，
// 免得四份「查缓存」各自演化（句柄缓存是热路径上唯一的共享状态）。
func handleOf[T any](m *FrameMetrics, cache map[string]T, key string, create func() T) T {
	m.mu.Lock()
	defer m.mu.Unlock()
	handle, ok := cache[key]
	if !ok {
		handle = create()
		cache[key] = handle
	}
	return handle
}

// reasonOf 返回错误的稳定 reason（拿不到时归 reasonUnknown）：指标标签只用稳定 reason，
// 不落自由文本（基数可控）。
func reasonOf(err error) string {
	if reason := atlaserrors.Reason(err); reason != "" {
		return reason
	}
	return reasonUnknown
}

// streamIDOf 返回本次帧请求的 battle 侧直连流标识（流式面 `<kind>/<连接 ID>`，数据报面
// `<kind>/<对端键>`；取不到返回空串）。接入层的 `s-…` **不随帧请求下发**（帧头只有
// Seq/Version/Type/Length/Session/Request-Id），故两侧各记自己的流标识，排障时按
// player_id + battle_id 关联（P1-4③；跨层同一 stream_id 需要改线格式，列为待办）。
func streamIDOf(ctx context.Context) string {
	kind := transport.Kind("")
	if tr, ok := transport.FromServerContext(ctx); ok {
		kind = tr.Kind()
	}
	return stream.ConnStreamID(stream.Conn{
		Kind:   kind,
		ConnID: transport.ConnIDFromContext(ctx),
		Peer:   transport.PeerFromContext(ctx),
	})
}
