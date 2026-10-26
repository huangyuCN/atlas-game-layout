package ledger

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	otelmetrics "github.com/huangyuCN/atlas/contrib/metrics/otel"
)

// countingStore 包装 Store 并统计 Lookup 次数：负缓存用例的核心断言就是「打了几次存储」。
type countingStore struct {
	Store

	mu      sync.Mutex
	lookups int
}

// Lookup 计一次后转交内层存储。
func (s *countingStore) Lookup(ctx context.Context, battleID string) (Entry, bool, error) {
	s.mu.Lock()
	s.lookups++
	s.mu.Unlock()
	return s.Store.Lookup(ctx, battleID)
}

// count 返回累计查询次数。
func (s *countingStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookups
}

// clockBox 是可手动推进的时钟（负缓存窗口断言用，避免真等 200ms）。
type clockBox struct {
	mu sync.Mutex
	at time.Time
}

// newClockBox 以当前时刻为起点构造时钟。
func newClockBox() *clockBox { return &clockBox{at: time.Now()} }

// now 返回当前（可被测试推进的）时刻。
func (c *clockBox) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

// advance 推进时钟。
func (c *clockBox) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// newCachedGate 构造带计数存储与可控时钟的闸门（负缓存取缺省窗口）。
func newCachedGate(t *testing.T, store Store) (*Gate, *countingStore, *clockBox) {
	t.Helper()
	cs := &countingStore{Store: store}
	clk := newClockBox()
	gate := NewGate(cs, battleActorType(t), nil, WithGateClock(clk.now))
	return gate, cs, clk
}

// TestGateNegativeCacheCollapsesMisses 是负缓存的核心：
// 同一 battle_id 在窗口内被问 100 次，只允许打**一次**存储（旧实现 100 次）。
//
// 为什么必须压在本地：已结束局的迟到敲门会在每个 hello / 每次激活上重查 etcd，
// 突发窗口里这是一条纯粹的放大链路（诊断 §5 修法 1）。
func TestGateNegativeCacheCollapsesMisses(t *testing.T) {
	gate, cs, clk := newCachedGate(t, NewMemoryStore())
	pid := pidOf(t, "b-late")
	for i := 0; i < 100; i++ {
		if err := gate.AllowActivation(context.Background(), pid); err != nil {
			t.Fatalf("第 %d 次（未留档）应放行: %v", i, err)
		}
		clk.advance(time.Millisecond) // 100 次共推进 100ms，仍在 200ms 窗口内
	}
	if got := cs.count(); got != 1 {
		t.Fatalf("窗口内 100 次询问打了 %d 次存储，期望 1 次（负缓存未生效）", got)
	}
}

// TestGateNegativeCacheExpires 验证窗口到期后重新查存储：负缓存只是把「很短的窗口」压住，
// 不能让「尚未留档」这个判定永久陈旧（窗口一过就必须重新确认）。
func TestGateNegativeCacheExpires(t *testing.T) {
	gate, cs, clk := newCachedGate(t, NewMemoryStore())
	pid := pidOf(t, "b-late")
	if err := gate.AllowActivation(context.Background(), pid); err != nil {
		t.Fatalf("首次询问应放行: %v", err)
	}
	if got := cs.count(); got != 1 {
		t.Fatalf("首次询问后存储查询 %d 次，期望 1 次", got)
	}
	clk.advance(DefaultNegativeCacheTTL + time.Millisecond)
	if err := gate.AllowActivation(context.Background(), pid); err != nil {
		t.Fatalf("窗口过期后（仍未留档）应放行: %v", err)
	}
	if got := cs.count(); got != 2 {
		t.Fatalf("窗口过期后应重查存储（累计 2 次），实际 %d 次", got)
	}
}

// TestGateSettledVerdictNeverCached 验证**命中（已结算）不缓存**：
// 留档命中后每次询问都必须重新查存储并拒绝——已结算的判定永远不能被缓存掩盖，
// 否则留档 TTL 内的一次复活就会被本地判定放行（即时正确性优先于省查询）。
func TestGateSettledVerdictNeverCached(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.Record(ctx, "b-ended", Entry{Winner: "p-a"}, time.Minute); err != nil {
		t.Fatalf("Record: %v", err)
	}
	gate, cs, _ := newCachedGate(t, store)
	pid := pidOf(t, "b-ended")
	for i := 0; i < 100; i++ {
		if err := gate.AllowActivation(ctx, pid); err == nil {
			t.Fatalf("第 %d 次：已结算对局必须拒绝激活", i)
		}
	}
	if got := cs.count(); got != 100 {
		t.Fatalf("命中判定被缓存了：100 次询问只查了 %d 次存储（期望逐次重查 100 次）", got)
	}
}

// TestGateNegativeCacheDoesNotMaskLaterSettlement 验证缓存的 miss **不得越过结算**：
// 先在窗口内缓存一次 miss，随后该局结算（写入留档）；窗口结束后的下一次询问必须拒绝。
//
// 窗口内的极短放行是负缓存的已知代价（诊断 §5 修法 1：留档 TTL ≥ 票据 TTL，现实不存在
// 合法复活）；本用例钉住「代价有界」——它只活在窗口里，绝不延伸到窗口之外。
func TestGateNegativeCacheDoesNotMaskLaterSettlement(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	gate, _, clk := newCachedGate(t, store)
	pid := pidOf(t, "b-race")
	if err := gate.AllowActivation(ctx, pid); err != nil {
		t.Fatalf("结算前的询问应放行: %v", err)
	}
	if err := store.Record(ctx, "b-race", Entry{Winner: "p-a"}, time.Minute); err != nil {
		t.Fatalf("Record: %v", err)
	}
	clk.advance(DefaultNegativeCacheTTL + time.Millisecond)
	if err := gate.AllowActivation(ctx, pid); err == nil {
		t.Fatal("结算之后（窗口已过）必须拒绝激活——缓存的 miss 不得掩盖命中")
	}
}

// TestGateNegativeCacheWindowBound 验证窗口口径：
// 默认为 200ms 且**不可能**配到更大（「≤200ms」是硬约定，配置只能收紧或关闭）。
func TestGateNegativeCacheWindowBound(t *testing.T) {
	if DefaultNegativeCacheTTL != 200*time.Millisecond {
		t.Fatalf("负缓存缺省窗口 = %v，期望 200ms", DefaultNegativeCacheTTL)
	}
	gate := NewGate(NewMemoryStore(), battleActorType(t), nil, WithNegativeCacheTTL(time.Hour))
	if got := gate.negativeCacheTTL(); got != MaxNegativeCacheTTL {
		t.Fatalf("窗口应被夹到 %v，实际 %v", MaxNegativeCacheTTL, got)
	}
	if got := NewGate(NewMemoryStore(), battleActorType(t), nil, WithNegativeCacheTTL(0)).negativeCacheTTL(); got != 0 {
		t.Fatalf("显式配 0 应关闭负缓存，实际 %v", got)
	}
}

// TestGateNegativeCacheDisabledQueriesEveryTime 验证关闭负缓存后逐次查存储（显式 0 = 旧行为）。
func TestGateNegativeCacheDisabledQueriesEveryTime(t *testing.T) {
	cs := &countingStore{Store: NewMemoryStore()}
	gate := NewGate(cs, battleActorType(t), nil, WithNegativeCacheTTL(0))
	pid := pidOf(t, "b-late")
	for i := 0; i < 3; i++ {
		if err := gate.AllowActivation(context.Background(), pid); err != nil {
			t.Fatalf("第 %d 次应放行: %v", i, err)
		}
	}
	if got := cs.count(); got != 3 {
		t.Fatalf("负缓存关闭时应逐次查存储（3 次），实际 %d 次", got)
	}
}

// TestGateNegativeCacheDoesNotCacheStoreError 验证查询失败不进负缓存：
// 失败按放行处理（失败开放），若把失败也缓存，存储抖动就会在窗口内被放大成「一直放行」
// 且再也查不出恢复（下一跳还要重试）。
func TestGateNegativeCacheDoesNotCacheStoreError(t *testing.T) {
	store := &flakyStore{}
	gate := NewGate(store, battleActorType(t), nil)
	pid := pidOf(t, "b-1")
	if err := gate.AllowActivation(context.Background(), pid); err != nil {
		t.Fatalf("查询失败应放行: %v", err)
	}
	if err := gate.AllowActivation(context.Background(), pid); err != nil {
		t.Fatalf("第二次（存储已恢复）应放行: %v", err)
	}
	if got := store.count(); got != 2 {
		t.Fatalf("查询失败不得缓存：应查 2 次，实际 %d 次", got)
	}
}

// flakyStore 的第一次查询失败、之后成功且未留档。
type flakyStore struct {
	mu sync.Mutex
	n  int
}

// Record 实现 Store（本用例不写入）。
func (s *flakyStore) Record(context.Context, string, Entry, time.Duration) error { return nil }

// Lookup 实现 Store：首次返回错误，之后返回未命中。
func (s *flakyStore) Lookup(context.Context, string) (Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	if s.n == 1 {
		return Entry{}, false, context.DeadlineExceeded
	}
	return Entry{}, false, nil
}

// count 返回查询次数。
func (s *flakyStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// TestGateMetricsPreRegisteredAtAssembly 断言闸门装配后**零事件**抓 /metrics：
// battle_activation_refused_total 与 battle_ledger_lookup_failed_total 已存在且为 0，
// 且之后的事件照常累加（预注册只写 0，不钉死序列）。
//
// 旧实现在此红：两条计数器只在第一次 Add 时才建序列，空闲 battle 上抓不到任何 battle_*
// 家族——「抓不到」于是被读成「没发生过拒绝」（诊断 §5 修法 4）。
func TestGateMetricsPreRegisteredAtAssembly(t *testing.T) {
	exp, err := otelmetrics.New()
	if err != nil {
		t.Fatalf("构造 OTel 采集器失败: %v", err)
	}
	t.Cleanup(func() { _ = exp.Shutdown(context.Background()) })

	NewGate(&nopStore{}, battleActorType(t), exp.Collector())
	body := scrapeProm(t, exp.Handler())
	for _, name := range []string{MetricActivationRefused, MetricLookupFailed} {
		if !hasPromName(body, name) {
			t.Fatalf("闸门装配后（零事件）抓不到 %s；抓取全文:\n%s", name, body)
		}
		if got := promValue(body, name); got != 0 {
			t.Fatalf("%s 在零事件时应为 0，实际 %v", name, got)
		}
	}

	gate := NewGate(&nopStore{entry: Entry{Winner: "p-a"}, ok: true}, battleActorType(t), exp.Collector())
	if err := gate.AllowActivation(context.Background(), pidOf(t, "b-ended")); err == nil {
		t.Fatal("已留档应拒绝激活")
	}
	if got := promValue(scrapeProm(t, exp.Handler()), MetricActivationRefused); got != 1 {
		t.Fatalf("预注册后拒绝计数应累加到 1，实际 %v", got)
	}
}

// scrapeProm 抓一次 Prometheus 文本端点（直接调 Handler，不占真实端口）。
func scrapeProm(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("指标端点应 200，实际 %d", rec.Code)
	}
	return rec.Body.String()
}

// hasPromName 报告抓取全文里是否有该指标名的样本行（值可以是 0）。
func hasPromName(body, name string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") && len(strings.Fields(line)) == 2 {
			return true
		}
	}
	return false
}

// promValue 返回该指标名第一条样本的取值（缺失返回 -1）。
func promValue(body, name string) float64 {
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, name+" ") {
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			return -1
		}
		return v
	}
	return -1
}
