// queue.go 把「入池阶段」从成局窗口里切出来：入队（EnterMatchQueue）按可配并发扇出，
// 每票往返与失败数单独打点，并支持可选预灌池（先把 N 张票灌满池子再开匹配）。
//
// 为什么需要它：上一轮入场只有 307.6–579 玩家/秒，但服务端每 tick 池里只有 ~6 张票、
// fetch_size=100 从未填满、tick 仅 2.37ms——瓶颈在驱动的串行入队（单票 110–200ms RTT），
// 不在服务端成局侧。把 RTT 用并发盖过去，才能量出服务端真实入场容量。
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

// queuePhase 是一段入池阶段的时间窗与结果（并发入队）。
type queuePhase struct {
	from    time.Time
	to      time.Time
	entered int // 入池成功票数
	failed  int // 入池失败票数
}

// dur 返回阶段耗时（秒）。
func (q queuePhase) dur() float64 { return q.to.Sub(q.from).Seconds() }

// qps 返回阶段入池速率（票/秒，含失败票；零宽窗口返回 0）。
func (q queuePhase) qps() float64 {
	if d := q.dur(); d > 0 {
		return float64(q.entered+q.failed) / d
	}
	return 0
}

// enterPhase 以 conc 并发把 [lo,hi) 的玩家入池并返回阶段统计。
//
// 单票失败只计数不打断整批：要量的是容量，个别票失败应体现在队列失败数里，
// 而不是让整轮压测白跑（上一轮「一个错就整轮失败」的口径在这里显式放宽）。
func enterPhase(ctx context.Context, ps []*loadPlayer, lo, hi, conc int) queuePhase {
	ph := queuePhase{from: time.Now()}
	if hi = min(hi, len(ps)); hi <= lo {
		ph.to = ph.from
		return ph
	}
	var mu sync.Mutex
	ok, bad := 0, 0
	_ = parallelFor(hi-lo, conc, func(i int) error {
		if err := ps[lo+i].enterQueue(ctx); err != nil {
			mu.Lock()
			bad++
			mu.Unlock()
			fmt.Fprintf(os.Stderr, "[入池] %v\n", err)
			return nil
		}
		mu.Lock()
		ok++
		mu.Unlock()
		return nil
	})
	ph.to = time.Now()
	ph.entered, ph.failed = ok, bad
	return ph
}

// matchOutcome 是一轮成局窗口的全部阶段边界与结果（入池阶段 + 成局阶段）。
type matchOutcome struct {
	prefill queuePhase // 预灌池阶段（-prefill > 0 时非空）
	queue   queuePhase // 入池阶段（预灌之后的剩余票；预灌填满时为零宽）
	match   queuePhase // 成局阶段（等待成局通知的窗口）
	matched int        // 收到成局通知的玩家数
	battles int        // 按 match_id 去重后的局数
}

// settle 让预灌的票先在池子里积起来（池子不积，成局侧永远看不到满池）。
func settle(d time.Duration) {
	if d > 0 {
		time.Sleep(d)
	}
}

// attachPhases 把阶段埋点写进汇总：入池 QPS 与成局段玩家/秒分开给，
// 免得像上一轮那样把「入池慢」看成「成局慢」。
func (o matchOutcome) attachPhases(rep *report) {
	rep.PrefillWant = o.prefill.entered + o.prefill.failed
	rep.PrefillEntered, rep.PrefillFailed = o.prefill.entered, o.prefill.failed
	rep.PrefillS, rep.PrefillQPS = o.prefill.dur(), o.prefill.qps()
	rep.QueueEntered, rep.QueueFailed = o.queue.entered, o.queue.failed
	rep.QueueS, rep.QueueQPS = o.queue.dur(), o.queue.qps()
	rep.MatchS = o.match.dur()
	rep.Matched = o.matched
	if d := o.match.dur(); d > 0 {
		rep.MatchPlayersPerS = float64(o.matched) / d
	}
	rep.EntryWindowS = o.prefill.dur() + o.queue.dur() + o.match.dur()
	if rep.EntryWindowS > 0 {
		rep.EntryPlayersPerS = float64(o.matched) / rep.EntryWindowS
	}
}

// check 校验入池参数：并发度 1–2048（再大只会把压测机自己打满），预灌票数不得超过玩家数。
func (q queueCfg) check(players int) error {
	if q.conc < 1 || q.conc > 2048 {
		return fmt.Errorf("非法 -queue-concurrency=%d（允许 1–2048）", q.conc)
	}
	if q.prefill < 0 || q.prefill > players {
		return fmt.Errorf("非法 -prefill=%d（允许 0–%d，不得超过 -players）", q.prefill, players)
	}
	return nil
}

// queueRTT 汇总全部玩家的入池往返样本（每票一次 EnterMatchQueue 的 RTT）。
func queueRTT(ps []*loadPlayer) latency {
	sum := &samples{}
	for _, p := range ps {
		if p != nil {
			sum.merge(p.queue)
		}
	}
	return summarize(sum.sorted())
}
