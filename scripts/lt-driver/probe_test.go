// probe_test.go 覆盖探针「结果分类」与「广播缺口」两条口径：
// 这两条决定了 p99 到底是「服务端慢」还是「探针超时被当成延迟」，必须有单测钉住。
package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestClassifyProbe 验证错误归一：截止/读超时进 timeout，其余进 error，nil 为成功。
func TestClassifyProbe(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		want    probeOutcome
		wantErr string
	}{
		{name: "成功", err: nil, want: probeOK, wantErr: ""},
		{name: "上下文截止", err: context.DeadlineExceeded, want: probeTimeout, wantErr: "context deadline exceeded"},
		{name: "包装后的截止", err: fmt.Errorf("frame: %w", context.DeadlineExceeded), want: probeTimeout, wantErr: "context deadline exceeded"},
		{name: "取消", err: context.Canceled, want: probeError, wantErr: "context canceled"},
		{name: "连接关闭", err: errors.New("client: response channel closed"), want: probeError, wantErr: "client: response channel closed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, kind := classifyProbe(c.err)
			if got != c.want {
				t.Fatalf("分类 = %v，期望 %v", got, c.want)
			}
			if kind != c.wantErr {
				t.Fatalf("报错类型 = %q，期望 %q", kind, c.wantErr)
			}
		})
	}
}

// TestClassifyProbeNetTimeout 验证 net.Error.Timeout() 也归入超时（读超时类错误不带 ctx 语义）。
func TestClassifyProbeNetTimeout(t *testing.T) {
	got, kind := classifyProbe(timeoutErr{})
	if got != probeTimeout || kind != "i/o timeout" {
		t.Fatalf("分类 = %v/%q，期望 timeout/i-o timeout", got, kind)
	}
}

// timeoutErr 是只实现 Timeout() 的假错误（模拟 net.Error 读超时）。
type timeoutErr struct{}

// Error 实现 error 接口。
func (timeoutErr) Error() string { return "read tcp: i/o timeout" }

// Timeout 实现 net.Error 接口（非临时错误）。
func (timeoutErr) Timeout() bool { return true }

// Temporary 实现 net.Error 接口（net.Error 要求）。
func (timeoutErr) Temporary() bool { return true }

// TestProbeSetSummary 验证成功率与两套分位数：超时样本必须**只**进「含超时」口径。
func TestProbeSetSummary(t *testing.T) {
	s := newProbeSet(false)
	base := time.Now()
	add := func(ms int, outcome probeOutcome, errKind string) {
		s.addRec(probeRec{
			Tag: "p0", Sent: base, Recv: base.Add(time.Duration(ms) * time.Millisecond),
			MS: float64(ms), Outcome: outcome, ErrKind: errKind,
		})
	}
	add(10, probeOK, "")
	add(12, probeOK, "")
	add(210, probeOK, "") // 慢但成功：必须留在「仅成功」口径里
	add(2000, probeTimeout, "context deadline exceeded")
	add(2000, probeTimeout, "context deadline exceeded")
	add(5, probeError, "connection closed")

	sum := s.summary()
	if sum.Total != 6 || sum.OK != 3 || sum.Timeout != 2 || sum.Errors != 1 {
		t.Fatalf("计数 = total %d ok %d timeout %d error %d，期望 6/3/2/1",
			sum.Total, sum.OK, sum.Timeout, sum.Errors)
	}
	if got, want := sum.SuccessPct, 50.0; got != want {
		t.Fatalf("成功率 = %.2f%%，期望 %.2f%%", got, want)
	}
	// 分位口径沿用 stats.go 的 percentile（取 int(q*(n-1)) 的**下界**），故 3 个样本的 p99 不是最大值；
	// 这里校验样本数与最大值——判定「慢但成功」看的就是最大值是否真的在 210ms 量级。
	if sum.OKLatency.Count != 3 || sum.OKLatency.Max != 210 {
		t.Fatalf("仅成功样本 = n %d max %.1f，期望 n=3 max=210", sum.OKLatency.Count, sum.OKLatency.Max)
	}
	if sum.AllLatency.Count != 5 {
		t.Fatalf("含超时样本 n = %d，期望 5（成功 3 + 超时 2；其它错误不计）", sum.AllLatency.Count)
	}
	if sum.TimeoutWait.Count != 2 || sum.TimeoutWait.P50 != 2000 {
		t.Fatalf("超时等待 = n %d p50 %.1f，期望 n=2 p50=2000", sum.TimeoutWait.Count, sum.TimeoutWait.P50)
	}
	if sum.ErrKinds["context deadline exceeded"] != 2 || sum.ErrKinds["connection closed"] != 1 {
		t.Fatalf("报错类型 = %v，期望 deadline=2 closed=1", sum.ErrKinds)
	}
}

// TestProbeSetMergeAndWindow 验证多连接合并与按打点时刻切窗（建流窗口 vs 稳态）。
func TestProbeSetMergeAndWindow(t *testing.T) {
	a, b := newProbeSet(false), newProbeSet(false)
	t0 := time.Now()
	a.addRec(probeRec{Tag: "a", Sent: t0, Recv: t0.Add(5 * time.Millisecond), MS: 5, Outcome: probeOK})
	b.addRec(probeRec{Tag: "b", Sent: t0, Recv: t0.Add(250 * time.Millisecond), MS: 250, Outcome: probeOK})
	b.addRec(probeRec{Tag: "b", Sent: t0, Recv: t0.Add(2 * time.Second), MS: 2000, Outcome: probeTimeout, ErrKind: "context deadline exceeded"})

	a.merge(b)
	if got := a.summary(); got.Total != 3 || got.OK != 2 || got.Timeout != 1 {
		t.Fatalf("合并后 = total %d ok %d timeout %d，期望 3/2/1", got.Total, got.OK, got.Timeout)
	}
	// 窗口切在 t0+100ms：只剩 5ms 那条（250ms 的落点在窗口外）。
	win := a.okWindow(t0, t0.Add(100*time.Millisecond))
	if len(win) != 1 || win[0] != 5 {
		t.Fatalf("建流窗口样本 = %v，期望 [5]", win)
	}
}

// TestBroadcastGap 验证广播缺口：分母取服务端 current_frame（收没收到），不让少收被静默吞掉。
func TestBroadcastGap(t *testing.T) {
	cases := []struct {
		name     string
		first    uint64
		server   uint64
		received uint64
		want     uint64
	}{
		{name: "无缺口", first: 100, server: 199, received: 100, want: 0},
		{name: "缺 3 帧", first: 100, server: 199, received: 97, want: 3},
		{name: "收到多于服务端帧号（晚到补帧）", first: 100, server: 150, received: 60, want: 0},
		{name: "未开始（无样本）", first: 0, server: 0, received: 0, want: 0},
		{name: "只有服务端帧号没收到广播", first: 100, server: 199, received: 0, want: 100},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want, missing := broadcastGap(c.first, c.server, c.received)
			if missing != c.want {
				t.Fatalf("缺口 = %d（期望帧 %d），期望 %d", missing, want, c.want)
			}
		})
	}
}
