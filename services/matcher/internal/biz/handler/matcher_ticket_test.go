package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas/matchmaker"
	"google.golang.org/protobuf/proto"
)

// stubTickets 返回取票回执桩（接入层面→地址列表 + 逐人票据；票据内容对 matcher 不透明）。
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

// TestWatchCompletedIssuesTickets 验证成局链路顺序「开局 → 取票 → 发布成局事件」，
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
	if got := sink.seq; len(got) != 3 || got[0] != "start" || got[1] != "issue" || got[2] != "publish" {
		t.Fatalf("调用顺序 = %v, 期望 [start issue publish]（票必须先于成局事件取得）", got)
	}
	if len(sink.issued) != 1 || sink.issued[0] != sink.published[0].battleID {
		t.Fatalf("取票目标 = %v, 期望开局对局 %q", sink.issued, sink.published[0].battleID)
	}
	if !proto.Equal(sink.published[0].tickets, sink.tickets) {
		t.Fatalf("事件票据回执 = %v, 期望与 battle 回执逐字段一致 %v", sink.published[0].tickets, sink.tickets)
	}
}

// TestWatchCompletedSkipsPublishWhenIssueFails 验证取票失败时不发成局事件：
// 事件里没有票与接入层地址，下发只会让客户端连不上（客户端可经查询接口恢复）。
func TestWatchCompletedSkipsPublishWhenIssueFails(t *testing.T) {
	h, svc, _, sink := newTestHandler()
	sink.issueErr = errors.New("battle 未就绪")
	ticketID := queueOne(t, h, "p-1")
	svc.emit(ticketID, matchmaker.TicketEvent{
		TicketID: ticketID,
		State:    matchmaker.TicketCompleted,
		Match:    twoPlayerMatch(ticketID),
	})
	waitFor(t, "取票调用", func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return len(sink.issued) >= 1
	})
	time.Sleep(50 * time.Millisecond) // 让后续步骤充分执行

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.published) != 0 {
		t.Fatalf("取票失败仍发布了成局事件 %d 次（应为 0）", len(sink.published))
	}
}
