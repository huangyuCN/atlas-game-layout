package handler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas/matchmaker"
)

// TestAdmissionGateBoundsConcurrency 验证闸门有界：同时执行的 fn 数不超过上限，
// 且确实并行（峰值 > 1，不是串行）。
func TestAdmissionGateBoundsConcurrency(t *testing.T) {
	const limit = 3
	g := newAdmissionGate(biz.Admission{Concurrency: limit, Wait: 2 * time.Second})
	if g.limit() != limit {
		t.Fatalf("并发上限 = %d, 期望 %d", g.limit(), limit)
	}
	var (
		mu       sync.Mutex
		cur, max int
		wg       sync.WaitGroup
	)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := g.do(context.Background(), func(context.Context) error {
				mu.Lock()
				cur++
				if cur > max {
					max = cur
				}
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				mu.Lock()
				cur--
				mu.Unlock()
				return nil
			})
			if err != nil {
				t.Errorf("闸门执行失败: %v", err)
			}
		}()
	}
	wg.Wait()
	if max > limit {
		t.Fatalf("并发峰值 = %d, 超过上限 %d（无界并发）", max, limit)
	}
	if max < 2 {
		t.Fatalf("并发峰值 = %d, 期望 > 1（应在多处并行扇出，而非一处串行）", max)
	}
}

// TestAdmissionGateBusyFailsFast 验证背压有界：槽被占满时等槽超过预算即快速失败
// （ErrAdmissionBusy），而不是无限排队或直接无界并发。
func TestAdmissionGateBusyFailsFast(t *testing.T) {
	g := newAdmissionGate(biz.Admission{Concurrency: 1, Wait: 150 * time.Millisecond})
	release, started := make(chan struct{}), make(chan struct{})
	go func() {
		_ = g.do(context.Background(), func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	begin := time.Now()
	err := g.do(context.Background(), func(context.Context) error {
		t.Error("等槽超时后不应执行被拒绝的处置")
		return nil
	})
	elapsed := time.Since(begin)
	close(release)
	if !errors.Is(err, ErrAdmissionBusy) {
		t.Fatalf("等槽超时错误 = %v, 期望 ErrAdmissionBusy", err)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("等槽耗时 = %v, 期望接近预算 150ms（不得立即失败也不得无限等待）", elapsed)
	}
}

// TestAdmissionFanoutBounded 验证成局处置按有界并发扇出：入队并发上限 3 时，
// 同时进行的「开局+出票」调用峰值恰为 3（其余在背压里等槽），不是每局一 goroutine 无界打满。
func TestAdmissionFanoutBounded(t *testing.T) {
	const limit = 3
	h, svc, _, sink := newTestHandlerWith(testHandlerOpts{
		admission: biz.Admission{Concurrency: limit, Timeout: 2 * time.Second, Wait: 2 * time.Second},
	})
	sink.block = make(chan struct{})
	for i := 0; i < 8; i++ {
		playerID := fmt.Sprintf("p-%d", i)
		ticketID := queueOne(t, h, playerID)
		m := &matchmaker.Match{
			ID:      fmt.Sprintf("m-%d", i),
			Tickets: []matchmaker.Ticket{{ID: ticketID, Players: []matchmaker.Player{{ID: playerID}}}},
		}
		svc.emit(ticketID, matchmaker.TicketEvent{
			TicketID: ticketID, State: matchmaker.TicketCompleted, Match: m,
		})
	}
	waitFor(t, "入场调用达到并发上限", func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return len(sink.admitted) >= limit
	})
	time.Sleep(50 * time.Millisecond) // 让其余局在背压里等槽
	if peak := sink.peak(); peak != limit {
		t.Fatalf("入场并发峰值 = %d, 期望恰为上限 %d（有界 + 确实并行）", peak, limit)
	}
	close(sink.block) // 放行全部处置
	waitFor(t, "全部成局处置完成", func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return len(sink.published) == 8
	})
	if peak := sink.peak(); peak > limit {
		t.Fatalf("放行后并发峰值 = %d, 超过上限 %d", peak, limit)
	}
}

// TestAdmissionFailureRequeueBounded 验证回池有上限：同一票血统最多回池 maxRequeue 次，
// 之后只发失败回执、不再重建票据（battle 持续不可用时不产生「成局→失败→回池」自激）。
func TestAdmissionFailureRequeueBounded(t *testing.T) {
	h, svc, _, sink := newTestHandler()
	sink.admitErr = errors.New("battle 不可用")
	ticketID := queueOne(t, h, "p-1")
	for round := 0; round <= maxRequeue; round++ {
		failRound(t, svc, sink, fmt.Sprintf("m-%d", round), ticketID, round+1)
		if round < maxRequeue {
			ticketID = lastTicketID(svc) // 回池后的新票（票上的回池计数已累加）
		}
	}
	if got, want := startCount(svc), 1+maxRequeue; got != want {
		t.Fatalf("入队次数 = %d, 期望 %d（初始 1 + 回池上限 %d）", got, want, maxRequeue)
	}
}

// failRound 让给定票成局并等待 wantFailed 条失败回执（本轮失败处置完成）。
func failRound(t *testing.T, svc *fakeService, sink *fakeSink, matchID, ticketID string, wantFailed int) {
	t.Helper()
	tk := storedTicket(t, svc, ticketID)
	m := &matchmaker.Match{ID: matchID, Tickets: []matchmaker.Ticket{tk}}
	svc.emit(ticketID, matchmaker.TicketEvent{TicketID: ticketID, State: matchmaker.TicketCompleted, Match: m})
	waitFor(t, "本轮失败回执", func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return len(sink.failed) >= wantFailed
	})
}

// storedTicket 返回引擎侧登记的票据快照：成局事件里的票由引擎给出，扩展随票携带。
func storedTicket(t *testing.T, svc *fakeService, ticketID string) matchmaker.Ticket {
	t.Helper()
	svc.mu.Lock()
	defer svc.mu.Unlock()
	tk, ok := svc.tickets[ticketID]
	if !ok {
		t.Fatalf("票 %s 未在引擎登记", ticketID)
	}
	return tk
}

// lastTicketID 返回引擎最近一次入队生成的票 ID（回池轮次推进用）。
func lastTicketID(svc *fakeService) string {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	return fmtID(svc.nextID)
}

// startCount 返回入队调用次数（回池断言用）。
func startCount(svc *fakeService) int {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	return len(svc.starts)
}
