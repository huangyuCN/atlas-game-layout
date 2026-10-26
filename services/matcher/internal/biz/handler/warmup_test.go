package handler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/metricstest"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas/matchmaker"
)

// fakeWarmer 记录预热调用（可注入错误与阻塞）。
type fakeWarmer struct {
	mu    sync.Mutex
	calls []startCall
	err   error
	block chan struct{}
	done  chan struct{}
}

// Warm 实现 biz.BattleWarmer。
func (w *fakeWarmer) Warm(_ context.Context, battleID, matchID string, playerIDs []string) error {
	w.mu.Lock()
	w.calls = append(w.calls, startCall{battleID: battleID, matchID: matchID, playerIDs: playerIDs})
	block := w.block
	err := w.err
	w.mu.Unlock()
	if w.done != nil {
		defer close(w.done)
	}
	if block != nil {
		<-block
	}
	return err
}

// called 返回已记录的预热调用（快照）。
func (w *fakeWarmer) called() []startCall {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]startCall(nil), w.calls...)
}

// waitCalled 等待预热被触发（异步触发，最多等 1s）。
func (w *fakeWarmer) waitCalled(t *testing.T) []startCall {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if calls := w.called(); len(calls) > 0 {
			return calls
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("预热未被触发")
	return nil
}

// TestAdmitMatchWarmsBattleActor 验证成局处置会**触发**预热，且预热携带同一 battle_id / match_id / 名册
// （预热与入场必须打同一对局，否则预热的是另一局）。
func TestAdmitMatchWarmsBattleActor(t *testing.T) {
	warmer := &fakeWarmer{}
	h, _, _, _ := newTestHandlerWith(testHandlerOpts{warmer: warmer})
	ev := matchmaker.TicketEvent{Match: &matchmaker.Match{ID: "m-1", Tickets: []matchmaker.Ticket{testTicket("t-1", "p-1", "p-2")}}}
	if !h.admitMatch(context.Background(), ev, "b-1", []string{"p-1", "p-2"}) {
		t.Fatal("成局处置应成功")
	}
	calls := warmer.waitCalled(t)
	if calls[0].battleID != "b-1" || calls[0].matchID != "m-1" {
		t.Fatalf("预热参数 = %+v，期望 b-1/m-1", calls[0])
	}
	if len(calls[0].playerIDs) != 2 {
		t.Fatalf("预热名册 = %v，期望 2 人", calls[0].playerIDs)
	}
}

// TestAdmitMatchWarmFailureDoesNotBlock 验证预热失败**不阻塞**主流程：
// 入场照常成功、成局事件照常发布（预热只是加速手段，不得成为新的失败源）。
func TestAdmitMatchWarmFailureDoesNotBlock(t *testing.T) {
	warmer := &fakeWarmer{err: errors.New("battle 预热失败")}
	h, _, _, sink := newTestHandlerWith(testHandlerOpts{warmer: warmer})
	ev := matchmaker.TicketEvent{Match: &matchmaker.Match{ID: "m-2", Tickets: []matchmaker.Ticket{testTicket("t-2", "p-3", "p-4")}}}
	if !h.admitMatch(context.Background(), ev, "b-2", []string{"p-3", "p-4"}) {
		t.Fatal("预热失败不应让成局处置失败")
	}
	warmer.waitCalled(t)
	if len(sink.published) == 0 {
		t.Fatal("预热失败不应阻断成局事件发布")
	}
}

// TestAdmitMatchWarmPendingDoesNotDelay 验证预热卡住时也不拖慢入场：
// 预热在独立 goroutine 内跑，入场不等待它的结果。
func TestAdmitMatchWarmPendingDoesNotDelay(t *testing.T) {
	warmer := &fakeWarmer{block: make(chan struct{})}
	defer close(warmer.block)
	h, _, _, _ := newTestHandlerWith(testHandlerOpts{warmer: warmer})
	ev := matchmaker.TicketEvent{Match: &matchmaker.Match{ID: "m-3", Tickets: []matchmaker.Ticket{testTicket("t-3", "p-5", "p-6")}}}
	begin := time.Now()
	if !h.admitMatch(context.Background(), ev, "b-3", []string{"p-5", "p-6"}) {
		t.Fatal("成局处置应成功")
	}
	if elapsed := time.Since(begin); elapsed > 500*time.Millisecond {
		t.Fatalf("入场耗时 %v：预热卡住拖慢了主流程", elapsed)
	}
	warmer.waitCalled(t)
}

// TestAdmissionWarmMetricOnFailure 验证预热失败可观测（打点）。
func TestAdmissionWarmMetricOnFailure(t *testing.T) {
	meter := metricstest.New()
	warmer := &fakeWarmer{err: errors.New("boom")}
	h, _, _, _ := newTestHandlerWith(testHandlerOpts{warmer: warmer, meter: meter})
	ev := matchmaker.TicketEvent{Match: &matchmaker.Match{ID: "m-4", Tickets: []matchmaker.Ticket{testTicket("t-4", "p-7", "p-8")}}}
	if !h.admitMatch(context.Background(), ev, "b-4", []string{"p-7", "p-8"}) {
		t.Fatal("成局处置应成功")
	}
	warmer.waitCalled(t)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if meter.CounterValue(metricWarmupFailed) > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("预热失败未打点（%s）", metricWarmupFailed)
}

// testTicket 构造测试票据。
func testTicket(id string, players ...string) matchmaker.Ticket {
	list := make([]matchmaker.Player, 0, len(players))
	for _, p := range players {
		list = append(list, matchmaker.Player{ID: p})
	}
	return matchmaker.Ticket{ID: id, Players: list}
}

// 保证 biz 引用不被优化掉（接口形态由 biz 定义）。
var _ biz.BattleWarmer = (*fakeWarmer)(nil)

// 保证 matcherv1 引用（失败事件 reason 类型）。
var _ = matcherv1.MatchFailReason_MATCH_FAIL_REASON_FAILED
