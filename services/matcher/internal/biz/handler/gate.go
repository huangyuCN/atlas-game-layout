// 成局处置的有界并发闸门：入场（开局+出票）扇出的上限与背压都在这里兜住。

package handler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
)

// ErrAdmissionBusy 是等并发槽超时的哨兵错误：处置与「开局+出票失败」一致
// （回池 + 失败事件 + 打点），既不静默丢弃，也不无限排队。
var ErrAdmissionBusy = errors.New("成局处置等并发槽超时")

// admissionGate 是成局处置的有界扇出闸门：
//   - 上限：同时最多 cap(slots) 局在走「开局+出票」的跨进程 RPC；
//   - 背压：拿不到槽的成局在各自的 ticket 监听 goroutine 里阻塞等待——不丢事件、
//     不无界起新 goroutine、不堆中间队列；等槽超过 wait 预算即快速失败（有界等待）；
//   - 无缓冲队列：等待者数天然等于「已成局但未处置」的局数，不会因积压吃内存。
type admissionGate struct {
	slots chan struct{}
	wait  time.Duration
}

// newAdmissionGate 按策略构造闸门（零值取缺省，见 biz.DefaultAdmit*；0 永不解释为无限并发）。
func newAdmissionGate(policy biz.Admission) *admissionGate {
	limit := policy.Concurrency
	if limit <= 0 {
		limit = biz.DefaultAdmitConcurrency
	}
	wait := policy.Wait
	if wait <= 0 {
		wait = biz.DefaultAdmitWait
	}
	return &admissionGate{slots: make(chan struct{}, limit), wait: wait}
}

// limit 返回并发上限（测试与打点口径用）。
func (g *admissionGate) limit() int { return cap(g.slots) }

// do 在并发槽内执行 fn；等槽超过 wait 预算返回 ErrAdmissionBusy（调用方按失败处置）。
func (g *admissionGate) do(ctx context.Context, fn func(context.Context) error) error {
	waitCtx, cancel := context.WithTimeout(ctx, g.wait)
	defer cancel()
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	case <-waitCtx.Done():
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: 等槽被取消: %w", ErrAdmissionBusy, err)
		}
		return fmt.Errorf("%w: 等槽超过 %s", ErrAdmissionBusy, g.wait)
	}
	return fn(ctx)
}
