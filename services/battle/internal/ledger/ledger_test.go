package ledger

import (
	"context"
	"strings"
	"testing"
	"time"

	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/metrics"
)

// battleActorType 返回闸门用例的 actor 类型段（与生成物的 battle actor 类型同源）。
func battleActorType(t *testing.T) string {
	t.Helper()
	return battlev1actor.BattleServiceActorType
}

// pidOf 返回 battle 类型的目标 PID。
func pidOf(t *testing.T, uid string) types.PID { return pidOfType(t, battleActorType(t), uid) }

// pidOfType 返回指定类型的目标 PID。
func pidOfType(t *testing.T, actorType, uid string) types.PID {
	t.Helper()
	pid, err := types.NewPID(actorType, uid)
	if err != nil {
		t.Fatalf("NewPID(%s:%s): %v", actorType, uid, err)
	}
	return pid
}

// counterStub 是计数器函数适配（metrics.Counter 只有一个 Add 方法）。
type counterStub func(float64)

// Add 累加计数。
func (f counterStub) Add(delta float64) { f(delta) }

// meterStub 是记录式指标假件（只记录计数器）。
type meterStub struct {
	counts map[string]float64
}

// newMeterStub 构造指标假件。
func newMeterStub() *meterStub { return &meterStub{counts: map[string]float64{}} }

// Counter 实现 metrics.Collector。
func (m *meterStub) Counter(name string, _ ...string) metrics.Counter {
	return counterStub(func(d float64) { m.counts[name] += d })
}

// Histogram 实现 metrics.Collector（本包用例不观测直方图）。
func (m *meterStub) Histogram(string, ...string) metrics.Histogram { return histStub{} }

// Gauge 实现 metrics.Collector（本包用例不观测仪表）。
func (m *meterStub) Gauge(string, ...string) metrics.Gauge { return gaugeStub{} }

// histStub 是空直方图。
type histStub struct{}

// Observe 丢弃观测值。
func (histStub) Observe(float64) {}

// gaugeStub 是空仪表。
type gaugeStub struct{}

// Set 丢弃设置值。
func (gaugeStub) Set(float64) {}

// Add 丢弃增量。
func (gaugeStub) Add(float64) {}

// Delete 丢弃删除。
func (gaugeStub) Delete() {}

// TestMemoryStoreTTL 验证进程内留档的读写与 TTL 过期语义：命中即真，过期即不存在，
// 同局重写幂等（TTL 重新起算）。
func TestMemoryStoreTTL(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now()
	store.now = func() time.Time { return now }
	ctx := context.Background()

	if _, ok, err := store.Lookup(ctx, "b-1"); ok || err != nil {
		t.Fatalf("未留档应返回未命中: ok=%v err=%v", ok, err)
	}
	if err := store.Record(ctx, "b-1", Entry{Winner: "p-a", Players: []string{"p-a", "p-b"}}, time.Minute); err != nil {
		t.Fatalf("Record: %v", err)
	}
	got, ok, err := store.Lookup(ctx, "b-1")
	if err != nil || !ok || got.Winner != "p-a" || len(got.Players) != 2 {
		t.Fatalf("留档内容不符: %+v ok=%v err=%v", got, ok, err)
	}
	// 同局重写：内容以最新为准，TTL 重新起算。
	if err := store.Record(ctx, "b-1", Entry{Winner: "p-b"}, time.Minute); err != nil {
		t.Fatalf("重复 Record: %v", err)
	}
	if got, _, _ := store.Lookup(ctx, "b-1"); got.Winner != "p-b" {
		t.Fatalf("重复留档未覆盖: %+v", got)
	}
	now = now.Add(2 * time.Minute)
	if _, ok, _ := store.Lookup(ctx, "b-1"); ok {
		t.Fatal("过期留档应视为未留档")
	}
}

// TestLeaseSeconds 验证租约秒数向上取整且不为 0（etcd 只接受正整数秒）。
func TestLeaseSeconds(t *testing.T) {
	cases := []struct {
		ttl  time.Duration
		want int64
	}{
		{time.Millisecond, 1},
		{time.Second, 1},
		{time.Second + time.Millisecond, 2},
		{135 * time.Second, 135},
	}
	for _, tc := range cases {
		if got := leaseSeconds(tc.ttl); got != tc.want {
			t.Fatalf("leaseSeconds(%s) = %d，期望 %d", tc.ttl, got, tc.want)
		}
	}
}

// TestMemoryStoreEmptyBattleIDIgnored 验证空 battle_id 不落档（不制造无主条目）。
func TestMemoryStoreEmptyBattleIDIgnored(t *testing.T) {
	store := NewMemoryStore()
	if err := store.Record(context.Background(), "", Entry{Winner: "p-a"}, time.Minute); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if _, ok, _ := store.Lookup(context.Background(), ""); ok {
		t.Fatal("空 battle_id 不应命中")
	}
}

// nopStore 是固定返回值的留档存储桩（闸门用例用）。
type nopStore struct {
	entry Entry
	ok    bool
	err   error
}

// Record 实现 Store：闸门用例不写入。
func (s *nopStore) Record(context.Context, string, Entry, time.Duration) error { return nil }

// Lookup 实现 Store：按桩的预置返回值回答。
func (s *nopStore) Lookup(context.Context, string) (Entry, bool, error) {
	return s.entry, s.ok, s.err
}

// TestGateRefusesEnded 验证闸门在留档命中时拒绝激活：错误带稳定 reason（BATTLE_ENDED，
// 客户端/调用方据此判定「该对局已结束」，不是传输故障），并计数。
func TestGateRefusesEnded(t *testing.T) {
	meter := newMeterStub()
	gate := NewGate(&nopStore{entry: Entry{Winner: "p-a"}, ok: true}, battleActorType(t), meter)
	err := gate.AllowActivation(context.Background(), pidOf(t, "b-1"))
	if err == nil || !strings.Contains(err.Error(), "已结束") {
		t.Fatalf("留档命中应拒绝激活: %v", err)
	}
	if meter.counts[MetricActivationRefused] != 1 {
		t.Fatalf("拒绝计数 = %v，期望 1", meter.counts[MetricActivationRefused])
	}
}

// TestGateAllowsUnknownAndForeign 验证闸门只对本服务 actor 类型的**已留档** PID 生效：
// 未留档放行，别的 actor 类型不查留档（闸门挂在全局运行时上，不得误伤其他域）。
func TestGateAllowsUnknownAndForeign(t *testing.T) {
	gate := NewGate(&nopStore{entry: Entry{Winner: "p-a"}, ok: true}, battleActorType(t), nil)
	if err := gate.AllowActivation(context.Background(), pidOfType(t, "other", "b-1")); err != nil {
		t.Fatalf("别的 actor 类型不应被闸门拦: %v", err)
	}
	allowGate := NewGate(&nopStore{}, battleActorType(t), nil)
	if err := allowGate.AllowActivation(context.Background(), pidOf(t, "b-1")); err != nil {
		t.Fatalf("未留档应放行: %v", err)
	}
	if err := NewGate(nil, battleActorType(t), nil).AllowActivation(context.Background(), pidOf(t, "b-1")); err != nil {
		t.Fatalf("未装配存储应放行（noop 闸门）: %v", err)
	}
}

// TestGateFailsOpenOnStoreError 验证存储不可用时**失败开放**（放行）并计数：
// 闸门在每次激活都会问一次，失败关闭会把所有新对局的激活一起打死（可用性事故）。
func TestGateFailsOpenOnStoreError(t *testing.T) {
	meter := newMeterStub()
	gate := NewGate(&nopStore{err: context.DeadlineExceeded}, battleActorType(t), meter)
	if err := gate.AllowActivation(context.Background(), pidOf(t, "b-1")); err != nil {
		t.Fatalf("查询失败应放行: %v", err)
	}
	if meter.counts[MetricLookupFailed] != 1 {
		t.Fatalf("查询失败计数 = %v，期望 1", meter.counts[MetricLookupFailed])
	}
}
