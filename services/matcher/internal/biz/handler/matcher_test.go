package handler

import (
	"context"
	"sync"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas/contrib/matchmaker/memory"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/matchmaker"
	"github.com/huangyuCN/atlas/metrics"
)

// fakeService 是撮合运行时 API 的假实现。
type fakeService struct {
	mu      sync.Mutex
	tickets map[string]matchmaker.Ticket
	nextID  int
	events  map[string]chan matchmaker.TicketEvent
	cancels []string
	// starts 记录入队请求（回池用例断言「玩家被重新入池」）。
	starts []matchmaker.StartRequest
}

func newFakeService() *fakeService {
	return &fakeService{
		tickets: make(map[string]matchmaker.Ticket),
		events:  make(map[string]chan matchmaker.TicketEvent),
	}
}

func (s *fakeService) StartMatchmaking(_ context.Context, req matchmaker.StartRequest) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	id := fmtID(s.nextID)
	s.starts = append(s.starts, req)
	s.tickets[id] = matchmaker.Ticket{
		ID: id, Players: req.Players, Matchmaker: req.Matchmaker,
		PartyID: req.PartyID, Extensions: req.Extensions,
	}
	s.events[id] = make(chan matchmaker.TicketEvent, 4)
	return id, nil
}

func (s *fakeService) Cancel(_ context.Context, ticketID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancels = append(s.cancels, ticketID)
	delete(s.tickets, ticketID)
	// 与真实引擎一致：取消产生终态事件并关闭事件流。
	if ch, ok := s.events[ticketID]; ok {
		ch <- matchmaker.TicketEvent{TicketID: ticketID, State: matchmaker.TicketCancelled}
		close(ch)
	}
	return nil
}

func (s *fakeService) AcceptMatch(context.Context, string, string) error { return nil }
func (s *fakeService) RejectMatch(context.Context, string, string) error { return nil }

func (s *fakeService) Describe(_ context.Context, ticketID string) (matchmaker.Ticket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tickets[ticketID]
	if !ok {
		return matchmaker.Ticket{}, matchmaker.ErrTicketNotFound
	}
	return t, nil
}

func (s *fakeService) Watch(_ context.Context, ticketID string) (<-chan matchmaker.TicketEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.events[ticketID]
	if !ok {
		return nil, matchmaker.ErrTicketNotFound
	}
	return ch, nil
}

func (s *fakeService) OpenBackfill(context.Context, matchmaker.OpenBackfillRequest) error { return nil }
func (s *fakeService) CreateBackfillTicket(context.Context, matchmaker.BackfillRequest) (string, error) {
	return "", nil
}
func (s *fakeService) CloseBackfill(context.Context, string) error { return nil }

// emit 投递事件（终态后关闭通道模拟后端行为）。
func (s *fakeService) emit(ticketID string, ev matchmaker.TicketEvent) {
	s.mu.Lock()
	ch := s.events[ticketID]
	s.mu.Unlock()
	if ch == nil {
		return
	}
	ch <- ev
	if ev.State.IsTerminal() {
		close(ch)
	}
}

func fmtID(n int) string { return "t-" + string(rune('a'+n-1)) }

// fakeMapper 是玩家→ticket 映射的内存实现。
type fakeMapper struct {
	mu      sync.Mutex
	ids     map[string]string
	matches map[string]string
	battles map[string]string
}

func newFakeMapper() *fakeMapper {
	return &fakeMapper{
		ids:     make(map[string]string),
		matches: make(map[string]string),
		battles: make(map[string]string),
	}
}

func (m *fakeMapper) Get(_ context.Context, playerID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ids[playerID], nil
}
func (m *fakeMapper) Set(_ context.Context, playerID, ticketID string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ids[playerID] = ticketID
	return nil
}
func (m *fakeMapper) Del(_ context.Context, playerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.ids, playerID)
	return nil
}
func (m *fakeMapper) SetMatch(_ context.Context, playerID, matchID string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.matches[playerID] = matchID
	return nil
}
func (m *fakeMapper) GetMatch(_ context.Context, playerID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.matches[playerID], nil
}

func (m *fakeMapper) DelMatch(_ context.Context, playerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.matches, playerID)
	return nil
}

func (m *fakeMapper) SetBattle(_ context.Context, playerID, battleID string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.battles == nil {
		m.battles = make(map[string]string)
	}
	m.battles[playerID] = battleID
	return nil
}

func (m *fakeMapper) GetBattle(_ context.Context, playerID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.battles[playerID], nil
}

func (m *fakeMapper) DelBattle(_ context.Context, playerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.battles, playerID)
	return nil
}

// fakeSink 是成局观察方的记录实现（入场调用/成局事件/失败事件分开记录，seq 记录调用顺序；
// 同时统计入场调用的并发峰值，供有界扇出用例断言）。
type fakeSink struct {
	mu        sync.Mutex
	seq       []string
	published []startCall
	admitted  []startCall
	failed    []failCall
	rosters   []rosterCall
	// tickets 是入场回执桩（nil 用最小回执）；admitErr 非空时入场失败。
	tickets  *battlev1.IssueEntryTicketReply
	admitErr error
	// block 非空时入场调用阻塞到通道关闭（有界并发与背压用例用）。
	block chan struct{}
	// cur/max 是入场调用的当前并发数与峰值（有界断言：峰值 ≤ 并发上限）。
	cur, max int
}

type startCall struct {
	battleID, matchID string
	playerIDs         []string
	tickets           *battlev1.IssueEntryTicketReply
}

type failCall struct {
	ticketID string
	reason   matcherv1.MatchFailReason
}

func (s *fakeSink) PublishStarted(_ context.Context, battleID, matchID string, playerIDs []string, tickets *battlev1.IssueEntryTicketReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq = append(s.seq, "publish")
	s.published = append(s.published, startCall{battleID: battleID, matchID: matchID, playerIDs: playerIDs, tickets: tickets})
	return nil
}

func (s *fakeSink) PublishFailed(_ context.Context, ticketID string, _ []string, reason matcherv1.MatchFailReason) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = append(s.failed, failCall{ticketID: ticketID, reason: reason})
	return nil
}

// Admit 实现 biz.BattleAdmitter：记录入场调用并回放桩回执（同时统计并发峰值）。
// 每局只调 1 次——「2 跳变 1 跳」在计数维度就是 admitted 的长度。
func (s *fakeSink) Admit(_ context.Context, battleID, matchID string, playerIDs []string) (*battlev1.IssueEntryTicketReply, error) {
	s.mu.Lock()
	s.seq = append(s.seq, "admit")
	s.admitted = append(s.admitted, startCall{battleID: battleID, matchID: matchID, playerIDs: playerIDs})
	s.cur++
	if s.cur > s.max {
		s.max = s.cur
	}
	err, block, tickets := s.admitErr, s.block, s.tickets
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.cur--
		s.mu.Unlock()
	}()
	if block != nil {
		<-block
	}
	if err != nil {
		return nil, err
	}
	if tickets != nil {
		return tickets, nil
	}
	// 最小回执：按名单逐人给一张不透明票（matcher 不解析票内容，只搬运）。
	reply := &battlev1.IssueEntryTicketReply{Endpoints: []*battlev1.EdgeEndpoint{
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.test:7100"},
	}}
	for _, playerID := range playerIDs {
		reply.Tickets = append(reply.Tickets, &battlev1.BattleTicketEntry{
			PlayerId: playerID, Ticket: []byte("cipher-" + playerID),
		})
	}
	return reply, nil
}

// peak 返回入场调用的并发峰值（有界扇出断言用）。
func (s *fakeSink) peak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max
}

// PublishRoster 实现 biz.PartyRosterPublisher：名册变更事件记录（party 域测试断言用）。
func (s *fakeSink) PublishRoster(_ context.Context, partyID, leaderID string, playerIDs []string, reason matcherv1.PartyRosterReason) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rosters = append(s.rosters, rosterCall{partyID: partyID, leaderID: leaderID, playerIDs: playerIDs, reason: reason})
	return nil
}

type rosterCall struct {
	partyID, leaderID string
	reason            matcherv1.PartyRosterReason
	playerIDs         []string
}

// fakeDeduper 是结算去重的内存实现。
type fakeDeduper struct {
	mu      sync.Mutex
	settled map[string]bool
}

func newFakeDeduper() *fakeDeduper { return &fakeDeduper{settled: make(map[string]bool)} }

func (d *fakeDeduper) TrySettle(_ context.Context, matchID string, _ time.Duration) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled[matchID] {
		return false, nil
	}
	d.settled[matchID] = true
	return true, nil
}

// fakePartyMapper 是队伍→整队票映射的内存实现。
type fakePartyMapper struct {
	mu      sync.Mutex
	tickets map[string]string
}

func newFakePartyMapper() *fakePartyMapper {
	return &fakePartyMapper{tickets: make(map[string]string)}
}

func (m *fakePartyMapper) GetPartyTicket(_ context.Context, partyID string) (string, error) {
	return m.tickets[partyID], nil
}

func (m *fakePartyMapper) SetPartyTicket(_ context.Context, partyID, ticketID string, _ time.Duration) error {
	m.tickets[partyID] = ticketID
	return nil
}

func (m *fakePartyMapper) DelPartyTicket(_ context.Context, partyID string) error {
	delete(m.tickets, partyID)
	return nil
}

// testHandlerOpts 是测试装配的可选项（有界并发与打点断言用；零值即默认装配）。
type testHandlerOpts struct {
	admission biz.Admission
	meter     metrics.Collector
	warmer    biz.BattleWarmer
	warmup    biz.Warmup
}

// newTestHandler 构造测试用 handler（party 用 memory 引擎真实实现）。
func newTestHandler() (*MatcherHandler, *fakeService, *fakeMapper, *fakeSink) {
	return newTestHandlerWith(testHandlerOpts{})
}

// newTestHandlerWith 以给定选项构造测试用 handler。
func newTestHandlerWith(o testHandlerOpts) (*MatcherHandler, *fakeService, *fakeMapper, *fakeSink) {
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
		Warmer:      o.warmer,
		Warmup:      o.warmup,
		Deduper:     newFakeDeduper(),
		Roster:      sink,
		Admission:   o.admission,
		Metrics:     o.meter,
	})
	return h, svc, mapper, sink
}

// TestQueue 验证入队与重复入队拒绝。
func TestQueue(t *testing.T) {
	ctx := context.Background()
	h, _, mapper, _ := newTestHandler()

	reply, err := h.QueueMatch(ctx, &matcherv1.QueueMatchRequest{
		PlayerId: "p-1",
		Player:   &commonv1.PlayerSummary{PlayerId: "p-1", Level: 10},
	})
	if err != nil {
		t.Fatalf("Queue: %v", err)
	}
	if reply.GetTicketId() == "" {
		t.Fatal("入队回执缺少 ticket_id")
	}
	if got, _ := mapper.Get(ctx, "p-1"); got != reply.GetTicketId() {
		t.Fatalf("映射 = %q, want %q", got, reply.GetTicketId())
	}
	// 重复入队拒绝。
	if _, err := h.QueueMatch(ctx, &matcherv1.QueueMatchRequest{PlayerId: "p-1", Player: &commonv1.PlayerSummary{PlayerId: "p-1", Level: 10}}); err == nil {
		t.Fatal("重复入队应失败")
	}
}

// TestCancel 验证取消与无映射幂等。
func TestCancel(t *testing.T) {
	ctx := context.Background()
	h, svc, mapper, _ := newTestHandler()
	if _, err := h.QueueMatch(ctx, &matcherv1.QueueMatchRequest{PlayerId: "p-1", Player: &commonv1.PlayerSummary{PlayerId: "p-1", Level: 10}}); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	reply, err := h.CancelMatch(ctx, &matcherv1.CancelMatchRequest{PlayerId: "p-1"})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if !reply.GetCanceled() {
		t.Fatal("取消回执 canceled=false")
	}
	if got, _ := mapper.Get(ctx, "p-1"); got != "" {
		t.Fatalf("取消后映射残留: %q", got)
	}
	// 无映射：幂等。
	reply, err = h.CancelMatch(ctx, &matcherv1.CancelMatchRequest{PlayerId: "p-none"})
	if err != nil || reply.GetCanceled() {
		t.Fatalf("无映射取消: reply=%+v err=%v", reply, err)
	}
	if len(svc.cancels) != 1 {
		t.Fatalf("底层取消次数 = %d, want 1", len(svc.cancels))
	}
}

// TestQueryStateMapping 验证状态枚举映射。
func TestQueryStateMapping(t *testing.T) {
	ctx := context.Background()
	h, svc, _, _ := newTestHandler()
	if _, err := h.QueueMatch(ctx, &matcherv1.QueueMatchRequest{PlayerId: "p-1", Player: &commonv1.PlayerSummary{PlayerId: "p-1", Level: 10}}); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	// 排队中。
	reply, err := h.QueryMatch(ctx, &matcherv1.QueryMatchRequest{PlayerId: "p-1"})
	if err != nil || reply.GetState() != matcherv1.MatchState_MATCH_STATE_WAITING {
		t.Fatalf("排队状态: reply=%+v err=%v", reply, err)
	}
	// 成局终态。
	svc.tickets["t-a"] = matchmaker.Ticket{ID: "t-a", State: matchmaker.TicketCompleted}
	reply, err = h.QueryMatch(ctx, &matcherv1.QueryMatchRequest{PlayerId: "p-1"})
	if err != nil || reply.GetState() != matcherv1.MatchState_MATCH_STATE_MATCHED {
		t.Fatalf("成局状态: reply=%+v err=%v", reply, err)
	}
	// 无映射。
	reply, err = h.QueryMatch(ctx, &matcherv1.QueryMatchRequest{PlayerId: "p-none"})
	if err != nil || reply.GetState() != matcherv1.MatchState_MATCH_STATE_NONE {
		t.Fatalf("无匹配状态: reply=%+v err=%v", reply, err)
	}
}

// TestQueueUnknownRuleset 验证未知规则集白名单拒绝。
func TestQueueUnknownRuleset(t *testing.T) {
	ctx := context.Background()
	h, _, _, _ := newTestHandler()
	_, err := h.QueueMatch(ctx, &matcherv1.QueueMatchRequest{
		PlayerId: "p-1",
		Player:   &commonv1.PlayerSummary{PlayerId: "p-1", Level: 10},
		Ruleset:  "ranked",
	})
	if atlaserrors.Reason(err) != "INVALID_PARAMS" {
		t.Fatalf("未知规则集应拒绝 INVALID_PARAMS, got %v", err)
	}
}

// TestWatchCompleted 验证成局事件链路：发布 + 开局调用 + 对局关联。
func TestWatchCompleted(t *testing.T) {
	ctx := context.Background()
	h, svc, mapper, sink := newTestHandler()
	if _, err := h.QueueMatch(ctx, &matcherv1.QueueMatchRequest{PlayerId: "p-1", Player: &commonv1.PlayerSummary{PlayerId: "p-1", Level: 10}}); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	m := &matchmaker.Match{
		ID: "m-1",
		Tickets: []matchmaker.Ticket{
			{ID: "t-a", Players: []matchmaker.Player{{ID: "p-1"}}},
			{ID: "t-b", Players: []matchmaker.Player{{ID: "p-2"}}},
		},
	}
	svc.emit("t-a", matchmaker.TicketEvent{TicketID: "t-a", State: matchmaker.TicketCompleted, Match: m})
	// 同局第二张 ticket 的 Completed（去重后应被跳过）。
	svc.emit("t-b", matchmaker.TicketEvent{TicketID: "t-b", State: matchmaker.TicketCompleted, Match: m})
	// 等待后台监听处理。
	deadline := time.Now().Add(2 * time.Second)
	for {
		sink.mu.Lock()
		n := len(sink.admitted)
		sink.mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("成局观察未触发")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // 让第二个事件充分处理
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.published) != 1 {
		t.Fatalf("成局事件发布次数 = %d, want 1（去重失效）", len(sink.published))
	}
	if len(sink.admitted) != 1 {
		t.Fatalf("入场调用次数 = %d, want 1（每局 1 跳 + 同局去重）", len(sink.admitted))
	}
	call := sink.admitted[0]
	if call.matchID != "m-1" || len(call.playerIDs) != 2 {
		t.Fatalf("入场调用不符: %+v", call)
	}
	if got, _ := mapper.GetMatch(ctx, "p-1"); got != "m-1" {
		t.Fatalf("对局关联 = %q, want m-1", got)
	}
	// battle 关联：开局推送丢失后重登恢复用（stub 的 StartMatchmaking 自增票据，
	// 成局票据为 t-a/t-b，battle_id 由 fake 记录即可验证写入发生）。
	bid, _ := mapper.GetBattle(ctx, "p-1")
	if bid == "" {
		t.Fatal("battle 关联未写入")
	}
}

// TestWatchFailed 验证失败终态：发布失败事件 + 清理映射。
func TestWatchFailed(t *testing.T) {
	ctx := context.Background()
	h, svc, mapper, sink := newTestHandler()
	if _, err := h.QueueMatch(ctx, &matcherv1.QueueMatchRequest{PlayerId: "p-1", Player: &commonv1.PlayerSummary{PlayerId: "p-1", Level: 10}}); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	svc.emit("t-a", matchmaker.TicketEvent{TicketID: "t-a", State: matchmaker.TicketTimedOut, Reason: "timeout"})
	deadline := time.Now().Add(2 * time.Second)
	for {
		sink.mu.Lock()
		n := len(sink.failed)
		sink.mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("失败观察未触发")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, _ := mapper.Get(ctx, "p-1"); got != "" {
		t.Fatalf("失败后映射残留: %q", got)
	}
}

// 静态保证 fake 实现接口。
var (
	_ matchmaker.Service       = (*fakeService)(nil)
	_ biz.MatchEventSink       = (*fakeSink)(nil)
	_ biz.PartyRosterPublisher = (*fakeSink)(nil)
)
