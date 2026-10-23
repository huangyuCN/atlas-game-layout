package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/metricstest"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas/matchmaker"
	"google.golang.org/protobuf/proto"
)

// stubTickets 返回入场回执桩（接入层面→地址列表 + 逐人票据；票据内容对 matcher 不透明）。
func stubTickets() *battlev1.IssueEntryTicketReply {
	return &battlev1.IssueEntryTicketReply{
		Endpoints: []*battlev1.EdgeEndpoint{
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.test:7100"},
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: "edge.test:7101"},
		},
		Tickets: []*battlev1.BattleTicketEntry{
			{PlayerId: "p-1", Ticket: []byte("cipher-p-1")},
			{PlayerId: "p-2", Ticket: []byte("cipher-p-2")},
		},
		ExpiresAtUnixMs: 1_760_000_000_000,
	}
}

// queueOne 入队单个玩家并返回其 ticket ID（成局链路的起点）。
func queueOne(t *testing.T, h *MatcherHandler, playerID string) string {
	t.Helper()
	reply, err := h.QueueMatch(context.Background(), &matcherv1.QueueMatchRequest{
		PlayerId: playerID,
		Player:   &commonv1.PlayerSummary{PlayerId: playerID, Level: 10},
	})
	if err != nil {
		t.Fatalf("入队 %s: %v", playerID, err)
	}
	return reply.GetTicketId()
}

// waitFor 轮询等待条件成立（后台成局监听 goroutine 的断言用）。
func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("等待超时: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// twoPlayerMatch 构造双人成局结果（ticketID 为触发监听的那张票）。
func twoPlayerMatch(ticketID string) *matchmaker.Match {
	return &matchmaker.Match{
		ID: "m-1",
		Tickets: []matchmaker.Ticket{
			{ID: ticketID, Players: []matchmaker.Player{{ID: "p-1"}}},
			{ID: "t-b", Players: []matchmaker.Player{{ID: "p-2"}}},
		},
	}
}

// TestWatchCompletedIssuesTickets 验证成局链路顺序「入场（开局+出票，一次调用）→ 发布成局事件」，
// 且成局事件携带 battle 回执原样的 endpoint 与逐人票据（matcher 只搬运，不重组）。
func TestWatchCompletedIssuesTickets(t *testing.T) {
	h, svc, _, sink := newTestHandler()
	sink.tickets = stubTickets()
	ticketID := queueOne(t, h, "p-1")
	svc.emit(ticketID, matchmaker.TicketEvent{
		TicketID: ticketID,
		State:    matchmaker.TicketCompleted,
		Match:    twoPlayerMatch(ticketID),
	})
	waitFor(t, "成局事件发布", func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return len(sink.published) >= 1
	})

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if got := sink.seq; len(got) != 2 || got[0] != "admit" || got[1] != "publish" {
		t.Fatalf("调用顺序 = %v, 期望 [admit publish]（开局与出票合并成一次调用）", got)
	}
	if len(sink.admitted) != 1 || sink.admitted[0].battleID != sink.published[0].battleID {
		t.Fatalf("入场目标 = %+v, 期望与成局事件的 battle 一致 %q", sink.admitted, sink.published[0].battleID)
	}
	if !proto.Equal(sink.published[0].tickets, sink.tickets) {
		t.Fatalf("事件票据回执 = %v, 期望与 battle 回执逐字段一致 %v", sink.published[0].tickets, sink.tickets)
	}
}

// TestWatchCompletedSkipsPublishWhenAdmitFails 验证入场失败时不发成局事件：
// 事件里没有票与接入层地址，下发只会让客户端连不上（失败回执另发，见下个用例）。
func TestWatchCompletedSkipsPublishWhenAdmitFails(t *testing.T) {
	h, svc, _, sink := newTestHandler()
	sink.admitErr = errors.New("battle 未就绪")
	ticketID := queueOne(t, h, "p-1")
	svc.emit(ticketID, matchmaker.TicketEvent{
		TicketID: ticketID,
		State:    matchmaker.TicketCompleted,
		Match:    twoPlayerMatch(ticketID),
	})
	waitFor(t, "入场调用", func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return len(sink.admitted) >= 1
	})
	time.Sleep(50 * time.Millisecond) // 让后续步骤充分执行

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.published) != 0 {
		t.Fatalf("入场失败仍发布了成局事件 %d 次（应为 0）", len(sink.published))
	}
}

// TestWatchCompletedAdmissionFailureIsExplicit 验证入场失败不再被静默吞掉：
// 不发成局事件、给全名单（每张票一条）发失败事件、玩家按原名册回池、打点计数 +1。
// 旧实现 `_ = h.sink.Start(...)` 吞错 ⇒ 本用例的失败事件数恒 0（真红）。
func TestWatchCompletedAdmissionFailureIsExplicit(t *testing.T) {
	meter := metricstest.New()
	h, svc, mapper, sink := newTestHandlerWith(testHandlerOpts{meter: meter})
	sink.admitErr = errors.New("battle 未就绪")
	ticketID := queueOne(t, h, "p-1")
	svc.emit(ticketID, matchmaker.TicketEvent{
		TicketID: ticketID,
		State:    matchmaker.TicketCompleted,
		Match:    twoPlayerMatch(ticketID),
	})
	waitFor(t, "失败事件发布", func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return len(sink.failed) >= 2
	})

	sink.mu.Lock()
	published, failed := len(sink.published), append([]failCall(nil), sink.failed...)
	sink.mu.Unlock()
	if published != 0 {
		t.Fatalf("入场失败仍发布了成局事件 %d 次（应为 0）", published)
	}
	reasons := make(map[string]matcherv1.MatchFailReason, len(failed))
	for _, f := range failed {
		reasons[f.ticketID] = f.reason
	}
	if len(reasons) != 2 {
		t.Fatalf("失败事件票数 = %d, 期望 2（全名单每张票一条）: %v", len(reasons), failed)
	}
	for _, want := range []string{"t-a", "t-b"} {
		if got, ok := reasons[want]; !ok || got != matcherv1.MatchFailReason_MATCH_FAIL_REASON_FAILED {
			t.Fatalf("票 %s 的失败原因 = %v（存在=%v）, 期望 MATCH_FAIL_REASON_FAILED", want, got, ok)
		}
	}
	if got := meter.CounterValue(metricAdmissionFailed, "stage", "admit"); got != 1 {
		t.Fatalf("入场失败打点 = %v, 期望 1（stage=admit）", got)
	}
	// 回池：原始两张票各重新入池一次（初始 1 次入队 + 2 次回池 = 3）。
	svc.mu.Lock()
	starts := append([]matchmaker.StartRequest(nil), svc.starts...)
	svc.mu.Unlock()
	if len(starts) != 3 {
		t.Fatalf("入队调用次数 = %d, 期望 3（初始 1 + 回池 2）: %+v", len(starts), starts)
	}
	for _, req := range starts[1:] {
		if req.Matchmaker != biz.DefaultMatchmakerName || len(req.Players) != 1 {
			t.Fatalf("回池请求 = %+v, 期望默认规则集 + 原票玩家", req)
		}
	}
	// 映射跟随新票：p-1 不再指向旧票 t-a；未成立的成局关联不得残留。
	ctx := context.Background()
	if got, _ := mapper.Get(ctx, "p-1"); got == "" || got == "t-a" {
		t.Fatalf("回池后玩家映射 = %q, 期望指向新票（非 t-a）", got)
	}
	if got, _ := mapper.GetBattle(ctx, "p-1"); got != "" {
		t.Fatalf("入场失败后 battle 关联 = %q, 期望为空（不得残留假成局）", got)
	}
}
