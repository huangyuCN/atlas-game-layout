// Package metricstest 提供测试用的指标采集器：把打点值记录到内存，供业务
// 打点单测按指标名/标签断言。仅测试依赖，不参与生产装配。
package metricstest

import (
	"strings"
	"sync"

	"github.com/huangyuCN/atlas/metrics"
)

// Recorder 是 metrics.Collector 的记录实现（并发安全）；
// 同时实现可选的 metrics.ObservableCollector，供拉取式仪表注册断言。
type Recorder struct {
	mu          sync.Mutex
	counters    map[string]float64
	histograms  map[string]int
	gauges      map[string]float64
	observables map[string]func() float64
}

// New 创建一个空的指标记录器。
func New() *Recorder {
	return &Recorder{
		counters:    make(map[string]float64),
		histograms:  make(map[string]int),
		gauges:      make(map[string]float64),
		observables: make(map[string]func() float64),
	}
}

// Counter 返回按 name+labels 分桶的计数句柄。
func (r *Recorder) Counter(name string, labels ...string) metrics.Counter {
	return counter{r: r, key: seriesKey(name, labels)}
}

// Histogram 返回按 name+labels 分桶的观测句柄。
func (r *Recorder) Histogram(name string, labels ...string) metrics.Histogram {
	return histogram{r: r, key: seriesKey(name, labels)}
}

// Gauge 返回按 name+labels 分桶的仪表句柄。
func (r *Recorder) Gauge(name string, labels ...string) metrics.Gauge {
	return gauge{r: r, key: seriesKey(name, labels)}
}

// ObservableGauge 实现 metrics.ObservableCollector：仅记录回调供断言。
func (r *Recorder) ObservableGauge(name string, fn func() float64, _ ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observables[name] = fn
}

// CounterValue 返回指定 name+labels 的累计值（未打点为 0）。
func (r *Recorder) CounterValue(name string, labels ...string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[seriesKey(name, labels)]
}

// HistogramCount 返回指定 name+labels 的观测次数（未观测为 0）。
func (r *Recorder) HistogramCount(name string, labels ...string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.histograms[seriesKey(name, labels)]
}

// GaugeValue 返回指定 name+labels 的当前值（未设置返回 0,false）。
func (r *Recorder) GaugeValue(name string, labels ...string) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.gauges[seriesKey(name, labels)]
	return v, ok
}

// Observable 返回指定名字已注册的拉取式回调（未注册返回 false）。
func (r *Recorder) Observable(name string) (func() float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn, ok := r.observables[name]
	return fn, ok
}

// seriesKey 把 name+labels 归一化为稳定的 map 键。
func seriesKey(name string, labels []string) string {
	if len(labels) == 0 {
		return name
	}
	return name + "{" + strings.Join(labels, ",") + "}"
}

type counter struct {
	r   *Recorder
	key string
}

// Add 累加计数。
func (c counter) Add(delta float64) {
	c.r.mu.Lock()
	defer c.r.mu.Unlock()
	c.r.counters[c.key] += delta
}

type histogram struct {
	r   *Recorder
	key string
}

// Observe 记录一次观测（仅计数，值不保留）。
func (h histogram) Observe(float64) {
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	h.r.histograms[h.key]++
}

type gauge struct {
	r   *Recorder
	key string
}

// Set 覆盖仪表值。
func (g gauge) Set(value float64) {
	g.r.mu.Lock()
	defer g.r.mu.Unlock()
	g.r.gauges[g.key] = value
}

// Add 累加仪表值。
func (g gauge) Add(delta float64) {
	g.r.mu.Lock()
	defer g.r.mu.Unlock()
	g.r.gauges[g.key] += delta
}

// Delete 移除仪表序列。
func (g gauge) Delete() {
	g.r.mu.Lock()
	defer g.r.mu.Unlock()
	delete(g.r.gauges, g.key)
}

var _ metrics.Collector = (*Recorder)(nil)
var _ metrics.ObservableCollector = (*Recorder)(nil)
