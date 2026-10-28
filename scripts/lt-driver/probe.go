// probe.go 把探针（SyncFrames）的**结果**与**延迟**分开统计。
//
// 为什么必须分开：只看分位数永远分不清「服务端慢」与「探针到点放弃」——
//   - 超时样本（等待时长远大于服务时延）若混进分位数，p95/p99 会被顶到「超时预算」附近；
//   - 若被静默丢弃，则「有 5% 探针没回来」这件事在报表里完全不可见。
//
// 因此每次探针都记三件事（发出时刻 / 收到时刻 / 结果分类与 err 类型），并在汇总里给两套分位：
// 「仅成功样本」（真实服务时延）与「含超时」（现在的观测口径）。
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// probeOutcome 是一次探针的结果分类（进 SUMMARY 的枚举口径，勿随意改名）。
type probeOutcome string

const (
	probeOK probeOutcome = "ok" // 收到回执
	// probeTimeout 是「到点没收到回执」：ctx 截止或读超时，等待时长≈调用预算。
	probeTimeout probeOutcome = "timeout"
	// probeError 是其它错误：连接断开、通道关闭、解码失败等（不是慢，是错）。
	probeError probeOutcome = "error"
)

// 报错类型标签（SUMMARY 里按标签计数，避免把长错误串铺进报表）。
const (
	errKindDeadline = "context deadline exceeded"
	errKindCanceled = "context canceled"
	errKindIOTimeout = "i/o timeout"
)

// probeRec 是一条探针记录：发出/收到时刻 + 等待时长 + 结果分类与报错类型。
type probeRec struct {
	Tag     string       `json:"tag"`
	Sent    time.Time    `json:"sent"`
	Recv    time.Time    `json:"recv"`
	MS      float64      `json:"ms"`
	Outcome probeOutcome `json:"outcome"`
	ErrKind string       `json:"err_kind,omitempty"`
}

// classifyProbe 把一次探针的错误归一为「结果分类 + 报错类型标签」。
//
// 超时与其它错误必须分开：只有超时才有「样本被丢弃/被拉高」的问题；
// 连接断开类错误意味着样本缺失的原因完全不同（该连接此后不再产生样本）。
func classifyProbe(err error) (probeOutcome, string) {
	if err == nil {
		return probeOK, ""
	}
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), strings.Contains(err.Error(), errKindDeadline):
		return probeTimeout, errKindDeadline
	case errors.As(err, &ne) && ne.Timeout():
		return probeTimeout, errKindIOTimeout
	case errors.Is(err, context.Canceled), strings.Contains(err.Error(), errKindCanceled):
		return probeError, errKindCanceled
	}
	return probeError, firstLine(err.Error())
}

// firstLine 取错误串首行并截断，避免把多行诊断塞进汇总键。
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 80 {
		s = s[:80] + "…"
	}
	return s
}

// probeSummary 是一组探针的结果摘要（成功率 + 两套分位 + 超时等待时长）。
type probeSummary struct {
	Total      int
	OK         int
	Timeout    int
	Errors     int
	SuccessPct float64
	ErrKinds   map[string]int
	OKLatency  latency // 仅成功样本：真实服务时延
	AllLatency latency // 含超时（超时按其实际等待时长计）：现在的观测口径
	TimeoutWait latency // 超时样本的等待时长分布（判是否卡在调用预算上）
}

// probeSet 并发安全地收集一组连接的全部探针记录。
type probeSet struct {
	mu       sync.Mutex
	ok       *samples
	all      *samples
	wait     *samples
	total    int
	okN      int
	timeoutN int
	errN     int
	errKinds map[string]int
	recs     []probeRec
	keep     bool
}

// newProbeSet 建一个探针记录集；keep 为真时保留逐条记录（-probe-log 落盘用）。
func newProbeSet(keep bool) *probeSet {
	return &probeSet{
		ok: &samples{}, all: &samples{}, wait: &samples{},
		errKinds: make(map[string]int), keep: keep,
	}
}

// addRec 记一条探针结果：计数分类 + 三套样本（仅成功 / 含超时 / 超时等待）。
func (s *probeSet) addRec(r probeRec) {
	if r.Tag == "" {
		r.Tag = "-"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total++
	d := time.Duration(r.MS * float64(time.Millisecond))
	switch r.Outcome {
	case probeOK:
		s.okN++
		s.ok.addAt(d, r.Recv)
		s.all.addAt(d, r.Recv)
	case probeTimeout:
		s.timeoutN++
		s.wait.addAt(d, r.Recv)
		s.all.addAt(d, r.Recv)
	default:
		s.errN++
	}
	if r.ErrKind != "" {
		s.errKinds[r.ErrKind]++
	}
	if s.keep && probeLogWanted() {
		s.recs = append(s.recs, r)
	}
}

// merge 合并另一组探针记录（汇总各连接用）。
func (s *probeSet) merge(other *probeSet) {
	if other == nil {
		return
	}
	other.mu.Lock()
	total, okN, timeoutN, errN := other.total, other.okN, other.timeoutN, other.errN
	kinds := make(map[string]int, len(other.errKinds))
	for k, v := range other.errKinds {
		kinds[k] += v
	}
	recs := append([]probeRec(nil), other.recs...)
	other.mu.Unlock()

	s.mu.Lock()
	s.total += total
	s.okN += okN
	s.timeoutN += timeoutN
	s.errN += errN
	for k, v := range kinds {
		s.errKinds[k] += v
	}
	s.recs = append(s.recs, recs...)
	s.mu.Unlock()
	s.ok.merge(other.ok)
	s.all.merge(other.all)
	s.wait.merge(other.wait)
}

// summary 计算成功率与两套分位数。
func (s *probeSet) summary() probeSummary {
	s.mu.Lock()
	out := probeSummary{
		Total: s.total, OK: s.okN, Timeout: s.timeoutN, Errors: s.errN,
		ErrKinds: make(map[string]int, len(s.errKinds)),
	}
	for k, v := range s.errKinds {
		out.ErrKinds[k] = v
	}
	s.mu.Unlock()
	if out.Total > 0 {
		out.SuccessPct = float64(out.OK) * 100 / float64(out.Total)
	}
	out.OKLatency = summarize(s.ok.sorted())
	out.AllLatency = summarize(s.all.sorted())
	out.TimeoutWait = summarize(s.wait.sorted())
	return out
}

// okWindow 返回打点时刻落在 [from, to] 内的成功样本（升序）；零值表示该侧不限。
func (s *probeSet) okWindow(from, to time.Time) []float64 { return s.ok.window(from, to) }

// dump 把逐条记录以 NDJSON 写入 w，返回写入条数（未开启保留时为 0）。
func (s *probeSet) dump(w io.Writer) (int, error) {
	s.mu.Lock()
	recs := append([]probeRec(nil), s.recs...)
	s.mu.Unlock()
	bw := bufio.NewWriterSize(w, 1<<16)
	enc := json.NewEncoder(bw)
	n := 0
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			return n, fmt.Errorf("写探针记录失败（第 %d 条）: %w", n+1, err)
		}
		n++
	}
	return n, bw.Flush()
}

// broadcastGap 返回广播的「应到帧数」与「缺口帧数」。
//
// 分母取服务端探针回执里的 current_frame（服务端权威帧号），分子取该连接实收的广播条数：
// 只有用服务端帧号当分母，才能发现「少收」——用实收条数当分母永远自证清白。
func broadcastGap(first, serverFrame, received uint64) (want, missing uint64) {
	if first == 0 || serverFrame < first {
		return received, 0
	}
	want = serverFrame - first + 1
	if received >= want {
		return want, 0
	}
	return want, want - received
}

// 逐条探针记录落盘的全局开关（由 -probe-log / -probe-log-every 设置；建连接前就导出）。
var (
	probeLogPath  string
	probeLogEvery atomic.Int64 // 每 N 条落一条（≤0 视为 1）
	probeLogSeq   atomic.Int64
)

// probeLogWanted 判断本条记录是否要落盘（抽样由 -probe-log-every 控制）。
func probeLogWanted() bool {
	if probeLogPath == "" {
		return false
	}
	every := probeLogEvery.Load()
	if every <= 1 {
		return true
	}
	return probeLogSeq.Add(1)%every == 0
}

// errKindText 把报错类型计数渲染为「标签=次数」串（按标签排序，便于跨轮 diff）。
func errKindText(m map[string]int) string {
	if len(m) == 0 {
		return "无"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

// dumpProbeLog 把多组探针记录合并写到 path（NDJSON，一行一条）。
// 关掉进程前调用；写入失败不影响已采集的汇总（只报错）。
func dumpProbeLog(path string, sets []*probeSet) (int, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	total := 0
	for _, s := range sets {
		if s == nil {
			continue
		}
		n, err := s.dump(f)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, f.Sync()
}
