// party_test 验证灵活组队域的 grpc 实现（真实 memory 引擎 + fake 记录器）。

package handler

import (
	"context"
	"testing"
	"time"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas/contrib/matchmaker/memory"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// newPartyHandler 构造 party 域测试环境（与单人域共享 fake 记录器）。
func newPartyHandler(t *testing.T) (*MatcherHandler, *fakeService, *fakeSink, *fakePartyMapper) {
	t.Helper()
	_ = t
	t.Helper()
	svc := newFakeService()
	mapper := newFakeMapper()
	sink := &fakeSink{}
	partyMapper := newFakePartyMapper()
	h := NewMatcherHandler(MatcherDeps{
		Svc:         svc,
		Party:       memory.NewPartyWithCapacity(svc, biz.DefaultPartyCapacity),
		Mapper:      mapper,
		PartyMapper: partyMapper,
		Sink:        sink,
		Deduper:     newFakeDeduper(),
		Roster:      sink,
	})
	return h, svc, sink, partyMapper
}

// TestPartyLifecycle 验证建队/加入/名册/离开/解散全生命周期与名册事件。
func TestPartyLifecycle(t *testing.T) {
	ctx := context.Background()
	h, svc, sink, _ := newPartyHandler(t)
	_ = svc

	// 队长建队。
	cp, err := h.CreateParty(ctx, &matcherv1.CreatePartyRequest{
		PlayerId: "p-leader",
		Player:   &commonv1.PlayerSummary{PlayerId: "p-leader", Level: 10},
	})
	if err != nil {
		t.Fatalf("CreateParty: %v", err)
	}
	partyID := cp.GetPartyId()
	if partyID == "" {
		t.Fatal("建队回执缺 party_id")
	}

	// 成员加入。
	if _, err := h.JoinParty(ctx, &matcherv1.JoinPartyRequest{
		PartyId:  partyID,
		PlayerId: "p-m1",
		Player:   &commonv1.PlayerSummary{PlayerId: "p-m1", Level: 12},
	}); err != nil {
		t.Fatalf("JoinParty: %v", err)
	}

	// 名册快照：队长 + 成员，等级为服务端权威值。
	info, err := h.DescribeParty(ctx, &matcherv1.DescribePartyRequest{PartyId: partyID})
	if err != nil {
		t.Fatalf("DescribeParty: %v", err)
	}
	if info.GetLeaderId() != "p-leader" || len(info.GetMembers()) != 2 {
		t.Fatalf("名册不符: %+v", info)
	}

	// 名册事件：created + join。
	sink.rostersCheck(t, []matcherv1.PartyRosterReason{
		matcherv1.PartyRosterReason_PARTY_ROSTER_REASON_CREATED,
		matcherv1.PartyRosterReason_PARTY_ROSTER_REASON_JOIN,
	})

	// 成员离开。
	if _, err := h.LeaveParty(ctx, &matcherv1.LeavePartyRequest{PartyId: partyID, PlayerId: "p-m1"}); err != nil {
		t.Fatalf("LeaveParty: %v", err)
	}
	// 已不在队重复离开：明确拒绝。
	if _, err := h.LeaveParty(ctx, &matcherv1.LeavePartyRequest{PartyId: partyID, PlayerId: "p-m1"}); atlaserrors.Reason(err) != "NOT_IN_PARTY" {
		t.Fatalf("重复离开应 NOT_IN_PARTY, got %v", err)
	}
}

// TestPartyQueueAndLeaveCancelsTicket 验证整队入队 → 成员离开联动取消整队票。
func TestPartyQueueAndLeaveCancelsTicket(t *testing.T) {
	ctx := context.Background()
	h, _, sink, partyMapper := newPartyHandler(t)

	cp, err := h.CreateParty(ctx, &matcherv1.CreatePartyRequest{
		PlayerId: "p-leader",
		Player:   &commonv1.PlayerSummary{PlayerId: "p-leader", Level: 10},
	})
	if err != nil {
		t.Fatalf("CreateParty: %v", err)
	}
	partyID := cp.GetPartyId()
	if _, err := h.JoinParty(ctx, &matcherv1.JoinPartyRequest{
		PartyId: partyID, PlayerId: "p-m1",
		Player: &commonv1.PlayerSummary{PlayerId: "p-m1", Level: 10},
	}); err != nil {
		t.Fatalf("JoinParty: %v", err)
	}

	// 整队入队。
	qp, err := h.QueueParty(ctx, &matcherv1.QueuePartyRequest{PartyId: partyID, Ruleset: "casual"})
	if err != nil {
		t.Fatalf("QueueParty: %v", err)
	}
	ticketID := qp.GetTicketId()
	if got, _ := partyMapper.GetPartyTicket(ctx, partyID); got != ticketID {
		t.Fatalf("队伍票映射不符: %q", got)
	}

	// 成员离开：联动取消整队票。
	if _, err := h.LeaveParty(ctx, &matcherv1.LeavePartyRequest{PartyId: partyID, PlayerId: "p-m1"}); err != nil {
		t.Fatalf("LeaveParty: %v", err)
	}
	if got, _ := partyMapper.GetPartyTicket(ctx, partyID); got != "" {
		t.Fatalf("取消后队伍票映射残留: %q", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		sink.mu.Lock()
		n := len(sink.failed)
		sink.mu.Unlock()
		if n >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("取消后失败事件未发布")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// rostersCheck 断言名册事件序列（并发安全）。
func (s *fakeSink) rostersCheck(t *testing.T, wantReasons []matcherv1.PartyRosterReason) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.rosters) != len(wantReasons) {
		t.Fatalf("名册事件数 = %d, want %d", len(s.rosters), len(wantReasons))
	}
	for i, r := range s.rosters {
		if r.reason != wantReasons[i] {
			t.Fatalf("名册事件[%d] = %s, want %s", i, r.reason, wantReasons[i])
		}
	}
}
