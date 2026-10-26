package handler

import (
	"context"
	"fmt"
	"testing"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/placement"
	"github.com/huangyuCN/atlas/matchmaker"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector/filter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 本文件守「入场失败换点重试一次」的语义：
// 首次 Admit 因**传输层**失败（实例刚下线/端口不通）时换下一个候选重试一次；
// 只换一次（不成风暴），两次都失败走既有失败路径，业务错误不重试。

// retryEvent 构造一局成局事件（两名玩家、一张票）。
func retryEvent(matchID string) matchmaker.TicketEvent {
	return matchmaker.TicketEvent{Match: &matchmaker.Match{
		ID:      matchID,
		Tickets: []matchmaker.Ticket{testTicket("t-"+matchID, "p-1", "p-2")},
	}}
}

// newRetryTestHandler 装配「两台本机 battle + 注入入场失败序列」的 handler。
func newRetryTestHandler(t *testing.T, errs ...error) (*MatcherHandler, *fakeSink) {
	t.Helper()
	sink := &fakeSink{admitErrs: errs}
	planner := placement.New(twoLocalBattles(), placement.Options{Local: filter.PreferLocal("127.0.0.1")})
	h, _, _, _ := newTestHandlerWith(testHandlerOpts{placer: planner, sink: sink})
	return h, sink
}

// admitCalls 返回入场调用次数（换点重试的次数断言看这里）。
func (s *fakeSink) admitCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.targets)
}

// targetList 返回各次入场时 ctx 上的选点目标（重试必须换一台）。
func (s *fakeSink) targetList() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...)
}

// publishedCount 返回成局事件发布次数。
func (s *fakeSink) publishedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.published)
}

// failedCount 返回失败事件发布次数。
func (s *fakeSink) failedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.failed)
}

// TestAdmitRetriesOnAnotherBattleWhenTransportFails 是本修复的核心断言：
// 首次入场因传输层失败时，换**另一台**候选重试一次并成功——本局照常成局（发布成局事件），
// 不走失败/回池路径。
//
// 旧实现在此红：Pin 钉死一台，Admit 失败即视为本局作废（钉到刚下线/端口冲突的进程就整局失败）。
func TestAdmitRetriesOnAnotherBattleWhenTransportFails(t *testing.T) {
	h, sink := newRetryTestHandler(t, status.Error(codes.Unavailable, "connection refused"))
	if !h.admitMatch(context.Background(), retryEvent("m-1"), "b-1", []string{"p-1", "p-2"}) {
		t.Fatal("首次传输层失败后应换点重试并成功，实际走了失败路径")
	}
	targets := sink.targetList()
	if len(targets) != 2 {
		t.Fatalf("应入场 2 次（首次 + 换点重试一次），实际 %d 次: %v", len(targets), targets)
	}
	if targets[1] == "" || targets[1] == targets[0] {
		t.Fatalf("重试必须换到另一台 battle：首次 %q，重试 %q", targets[0], targets[1])
	}
	if got := sink.publishedCount(); got != 1 {
		t.Fatalf("重试成功后应发布 1 次成局事件，实际 %d 次", got)
	}
	if got := sink.failedCount(); got != 0 {
		t.Fatalf("重试成功不得发失败事件，实际 %d 次", got)
	}
}

// TestAdmitRetriesOnlyOnceWhenAllBattlesFail 验证「只重试一次」：
// 两台候选都失败时入场总共 2 次，随后按既有失败路径处置（失败事件 + 回池），不无限重试。
func TestAdmitRetriesOnlyOnceWhenAllBattlesFail(t *testing.T) {
	down := status.Error(codes.Unavailable, "dial tcp: connect: connection refused")
	h, sink := newRetryTestHandler(t, down, down)
	if h.admitMatch(context.Background(), retryEvent("m-1"), "b-1", []string{"p-1", "p-2"}) {
		t.Fatal("两台都失败时必须按失败路径处置")
	}
	if got := sink.admitCalls(); got != 2 {
		t.Fatalf("只允许换点重试一次（共 2 次入场调用），实际 %d 次", got)
	}
	if got := sink.failedCount(); got != 1 {
		t.Fatalf("失败处置应逐票发失败事件（1 张票），实际 %d 次", got)
	}
	if got := sink.publishedCount(); got != 0 {
		t.Fatalf("全失败不得发布成局事件，实际 %d 次", got)
	}
}

// TestAdmitDoesNotRetryBusinessError 验证业务错误不重试：换一台 battle 业务判定也不会变
// （对局已结束、参数非法、名额不足都在 battle 侧判定），重试只是白跑一跳。
func TestAdmitDoesNotRetryBusinessError(t *testing.T) {
	h, sink := newRetryTestHandler(t, errorv1.ErrBattleEnded("对局 %s 已结束", "b-1"))
	if h.admitMatch(context.Background(), retryEvent("m-1"), "b-1", []string{"p-1", "p-2"}) {
		t.Fatal("业务错误必须按失败路径处置")
	}
	if got := sink.admitCalls(); got != 1 {
		t.Fatalf("业务错误不应换点重试（入场 1 次），实际 %d 次", got)
	}
	if got := sink.failedCount(); got != 1 {
		t.Fatalf("业务错误应逐票发失败事件（1 张票），实际 %d 次", got)
	}
}

// TestAdmitDoesNotRetryNonTransportErrors 验证非传输类失败同样不重试：
// 预算到期（DeadlineExceeded）没有时间给第二跳，换点只会把失败拖长。
func TestAdmitDoesNotRetryNonTransportErrors(t *testing.T) {
	h, sink := newRetryTestHandler(t, status.Error(codes.DeadlineExceeded, "budget exhausted"))
	if h.admitMatch(context.Background(), retryEvent("m-1"), "b-1", []string{"p-1", "p-2"}) {
		t.Fatal("到期必须按失败路径处置")
	}
	if got := sink.admitCalls(); got != 1 {
		t.Fatalf("到期不应换点重试（入场 1 次），实际 %d 次", got)
	}
}

// TestAdmitDoesNotRetryWithoutAlternative 验证只有一台候选时不换点：
// 换到同一台等于原样重试，没有价值（失败按既有路径处置即可）。
func TestAdmitDoesNotRetryWithoutAlternative(t *testing.T) {
	disc := &handlerDiscovery{ins: []*registry.ServiceInstance{
		{ID: "battle-1", Name: "battle", Endpoints: []string{"grpc://127.0.0.1:9301"}},
	}}
	sink := &fakeSink{admitErrs: []error{status.Error(codes.Unavailable, "down")}}
	planner := placement.New(disc, placement.Options{Local: filter.PreferLocal("127.0.0.1")})
	h, _, _, _ := newTestHandlerWith(testHandlerOpts{placer: planner, sink: sink})
	if h.admitMatch(context.Background(), retryEvent("m-1"), "b-1", []string{"p-1", "p-2"}) {
		t.Fatal("单台候选失败时必须按失败路径处置")
	}
	if got := sink.admitCalls(); got != 1 {
		t.Fatalf("没有其它候选时不应重试（入场 1 次），实际 %d 次", got)
	}
}

// TestAdmitRetryKeepsSameBattle 验证换点重试仍打**同一局**：battle_id/match_id 与名单不变
// （换点只换落点，不能把对局换个身份——否则等于凭空造了第二局）。
func TestAdmitRetryKeepsSameBattle(t *testing.T) {
	h, sink := newRetryTestHandler(t, status.Error(codes.Unavailable, "down"))
	if !h.admitMatch(context.Background(), retryEvent("m-9"), "b-9", []string{"p-1", "p-2"}) {
		t.Fatal("换点重试应成功")
	}
	calls := sink.admittedSnapshot()
	if len(calls) != 2 {
		t.Fatalf("应入场 2 次，实际 %d 次", len(calls))
	}
	for i, c := range calls {
		if c.battleID != "b-9" || c.matchID != "m-9" || fmt.Sprint(c.playerIDs) != "[p-1 p-2]" {
			t.Fatalf("第 %d 次入场身份漂移: battle=%s match=%s players=%v", i, c.battleID, c.matchID, c.playerIDs)
		}
	}
}

// admittedSnapshot 返回入场调用快照（身份漂移断言用）。
func (s *fakeSink) admittedSnapshot() []startCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]startCall(nil), s.admitted...)
}
