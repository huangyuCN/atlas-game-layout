package server

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// metricSeries 是一条指标序列（名字 + 标签键值对）的记录键。
type metricSeries string

// seriesOf 拼装序列键（标签按 [k,v,...] 顺序拼接，便于用例按名字+标签断言）。
func seriesOf(name string, labels []string) metricSeries {
	s := name
	for i := 0; i+1 < len(labels); i += 2 {
		s += "|" + labels[i] + "=" + labels[i+1]
	}
	return metricSeries(s)
}

// metricStub 是记录式指标假件：计数器按序列累加、直方图按序列记录观测值。
type metricStub struct {
	mu     sync.Mutex
	ctr    map[metricSeries]float64
	hist   map[metricSeries][]float64
	gauges map[metricSeries]float64
}

// newMetricStub 构造记录式指标假件。
func newMetricStub() *metricStub {
	return &metricStub{ctr: map[metricSeries]float64{}, hist: map[metricSeries][]float64{},
		gauges: map[metricSeries]float64{}}
}

// Counter 实现 metrics.Collector：返回按序列累加的计数器。
func (s *metricStub) Counter(name string, labels ...string) metrics.Counter {
	k := seriesOf(name, labels)
	return counterStub(func(d float64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.ctr[k] += d
	})
}

// Histogram 实现 metrics.Collector：返回按序列记录观测值的直方图。
func (s *metricStub) Histogram(name string, labels ...string) metrics.Histogram {
	k := seriesOf(name, labels)
	return histogramStub(func(v float64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.hist[k] = append(s.hist[k], v)
	})
}

// Gauge 实现 metrics.Collector：本包用例不观测仪表。
func (s *metricStub) Gauge(name string, labels ...string) metrics.Gauge {
	k := seriesOf(name, labels)
	return gaugeStub(func(v float64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.gauges[k] = v
	})
}

// count 返回某序列的计数累计值。
func (s *metricStub) count(name string, labels ...string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctr[seriesOf(name, labels)]
}

// observations 返回某序列的观测值快照。
func (s *metricStub) observations(name string, labels ...string) []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]float64(nil), s.hist[seriesOf(name, labels)]...)
}

// counterStub 是计数器函数适配（metrics.Counter 只有一个 Add 方法）。
type counterStub func(float64)

// Add 累加计数。
func (f counterStub) Add(delta float64) { f(delta) }

// histogramStub 是直方图函数适配。
type histogramStub func(float64)

// Observe 记录一次观测。
func (f histogramStub) Observe(v float64) { f(v) }

// gaugeStub 是仪表函数适配（本包用例不观测仪表）。
type gaugeStub func(float64)

// Set 记录当前值。
func (f gaugeStub) Set(v float64) { f(v) }

// Add 累加增量。
func (f gaugeStub) Add(v float64) { f(v) }

// Delete 删除序列（无操作）。
func (f gaugeStub) Delete() {}

// TestFrameMetricsCountsTicketFailureByReason 验证 P1-4①：票据/身份校验失败按 reason 计数，
// 且只覆盖**解析失败**（身份未解析出来）那一类——投递/校验失败另有 op 口径（见 Wrap 用例）。
func TestFrameMetricsCountsTicketFailureByReason(t *testing.T) {
	stub := newMetricStub()
	hooks := NewFrameMetrics(stub).Hooks()
	entry := mustEntry(t, battlev1opclient.BattleServiceProtocolOps.JoinBattle)
	hooks.PostDeliver(context.Background(), entry, frameops.Identity{}, errorv1.ErrBattleTicketExpired("票过期"))
	hooks.PostDeliver(context.Background(), entry, frameops.Identity{}, errorv1.ErrBattleTicketInvalid("票非法"))
	hooks.PostDeliver(context.Background(), entry, frameops.Identity{}, errorv1.ErrBattleTicketInvalid("票非法"))
	want := map[string]float64{
		errorv1.ReasonBattleTicketExpired(): 1,
		errorv1.ReasonBattleTicketInvalid(): 2,
	}
	for reason, n := range want {
		if got := stub.count(MetricTicketRejected, "reason", reason); got != n {
			t.Fatalf("票据拒绝计数[%s] = %v，期望 %v", reason, got, n)
		}
	}
	// 身份已解析（guard/投递失败）不计入票据口径；成功不计数。
	hooks.PostDeliver(context.Background(), entry, frameops.Identity{PlayerID: "p-1", BattleID: "b-1"},
		errors.New("投递失败"))
	hooks.PostDeliver(context.Background(), entry, frameops.Identity{PlayerID: "p-1", BattleID: "b-1"}, nil)
	total := stub.count(MetricTicketRejected, "reason", errorv1.ReasonBattleTicketExpired()) +
		stub.count(MetricTicketRejected, "reason", errorv1.ReasonBattleTicketInvalid())
	if total != 3 {
		t.Fatalf("票据拒绝总计数 = %v，期望 3（只统计解析失败）", total)
	}
}

// TestFrameMetricsWrapObservesDurationAndErrors 验证 P1-4②：帧 op 包装观测耗时直方图，
// 出错按 op + reason 计数；成功不计错误。
func TestFrameMetricsWrapObservesDurationAndErrors(t *testing.T) {
	stub := newMetricStub()
	m := NewFrameMetrics(stub)
	op := battlev1opclient.BattleServiceProtocolOps.SyncFrames
	slow := func(context.Context, engine.Decoder, engine.Encoder) ([]byte, error) {
		time.Sleep(2 * time.Millisecond)
		return nil, errorv1.ErrBattleNotFound("不在名单")
	}
	if _, err := m.Wrap(op, slow)(context.Background(), nil, nil); err == nil {
		t.Fatal("包装后的 handler 应原样返回错误")
	}
	ok := func(context.Context, engine.Decoder, engine.Encoder) ([]byte, error) { return []byte{1}, nil }
	if out, err := m.Wrap(op, ok)(context.Background(), nil, nil); err != nil || len(out) != 1 {
		t.Fatalf("包装后的 handler 应原样返回回执: out=%v err=%v", out, err)
	}
	obs := stub.observations(MetricOpDuration, "operation", op)
	if len(obs) != 2 || obs[0] < 0.002 {
		t.Fatalf("耗时观测 = %v，期望 2 次且首次 ≥2ms", obs)
	}
	if got := stub.count(MetricOpErrors, "operation", op, "reason", errorv1.ReasonBattleNotFound()); got != 1 {
		t.Fatalf("op 错误计数 = %v，期望 1", got)
	}
	// 兜底：非结构化错误（无 reason）归 UNKNOWN，不给标签基数留自由文本。
	m.Wrap(op, func(context.Context, engine.Decoder, engine.Encoder) ([]byte, error) {
		return nil, errors.New("裸错误")
	})(context.Background(), nil, nil)
	if got := stub.count(MetricOpErrors, "operation", op, "reason", reasonUnknown); got != 1 {
		t.Fatalf("裸错误计数 = %v，期望 1（reason=%s）", got, reasonUnknown)
	}
}

// TestNewFrameOpsWiresFrameMetrics 验证装配口径：注入的观测出口经 WithFrameMetrics 接到帧 op
// 服务端（票据拒绝真的会被打点），未注入时为 noop（测试装置不用改）。
func TestNewFrameOpsWiresFrameMetrics(t *testing.T) {
	stub := newMetricStub()
	ops, err := NewFrameOps(nil, testRecorder(), testTicketKey(), WithFrameMetrics(NewFrameMetrics(stub)))
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	_, err = ops.Handle(frameCtx(transport.KindKCP, 7, "", "$$$"),
		mustEntry(t, battlev1opclient.BattleServiceProtocolOps.JoinBattle),
		&battlev1.JoinBattleReq{BattleId: "b-test01"})
	if err == nil {
		t.Fatal("非法 base64 票据应被拒")
	}
	if got := stub.count(MetricTicketRejected, "reason", atlaserrors.Reason(err)); got != 1 {
		t.Fatalf("票据拒绝计数 = %v（reason=%s），期望 1", got, atlaserrors.Reason(err))
	}
}

// mustEntry 取一条真实路由条目（生成物原样）。
func mustEntry(t *testing.T, operation string) relay.RouteEntry {
	t.Helper()
	entry, ok := battlev1.BattleServiceRouteTable[operation]
	if !ok {
		t.Fatalf("路由表缺少 %s", operation)
	}
	return entry
}

// TestFrameMetricsTicketRejectionLogsStreamID 验证票据拒绝日志带 `stream_id` 关联字段（P1-4③）：
// 帧面拒绝是「哪条直连、哪个 op、什么 reason」的排障入口（接入层 stream_id 不下发，
// 故这里记 battle 侧直连流标识，两段按 player_id + battle_id 对齐）。
func TestFrameMetricsTicketRejectionLogsStreamID(t *testing.T) {
	old := atlaslog.GetLogger()
	var buf bytes.Buffer
	atlaslog.SetLogger(atlaslog.New(atlaslog.WithWriter(&buf)))
	t.Cleanup(func() { atlaslog.SetLogger(old) })

	NewFrameMetrics(metrics.Noop()).Hooks().PostDeliver(
		frameCtx(transport.KindKCP, 7, "", "slot"),
		mustEntry(t, battlev1opclient.BattleServiceProtocolOps.JoinBattle),
		frameops.Identity{}, errorv1.ErrBattleTicketExpired("票过期"))

	out := buf.String()
	for _, want := range []string{"stream_id=kcp/7", "reason=BATTLE_TICKET_EXPIRED", "operation=/battle.v1.BattleService/JoinBattle"} {
		if !strings.Contains(out, want) {
			t.Fatalf("票据拒绝日志缺少 %q：\n%s", want, out)
		}
	}
}
