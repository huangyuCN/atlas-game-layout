package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	matcherassemble "github.com/huangyuCN/atlas-game-layout/services/matcher/assemble"
	"github.com/huangyuCN/atlas/matchmaker"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	natsgo "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
)

// recordSink 是成局观察方的记录实现（真实 nats 发布 + 记录，双通道可观测）。
type recordSink struct {
	nc *natsgo.Conn

	mu        sync.Mutex
	published []startedCall
	started   []startedCall
}

type startedCall struct {
	battleID, matchID string
	playerIDs         []string
}

func (s *recordSink) PublishStarted(ctx context.Context, battleID, matchID string, playerIDs []string) error {
	// 真实 nats 发布（断言事件到达总线）。
	payload, err := protojson.Marshal(&matcherv1.MatchStartedEvent{
		MatchId: matchID, BattleId: battleID, PlayerIds: playerIDs,
	})
	if err == nil {
		_ = nats.Publish(ctx, s.nc, consts.MatchStartedTopic(), payload)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published = append(s.published, startedCall{battleID: battleID, matchID: matchID, playerIDs: playerIDs})
	return nil
}

func (s *recordSink) PublishFailed(context.Context, string, []string, string) error { return nil }

func (s *recordSink) Start(_ context.Context, battleID, matchID string, playerIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = append(s.started, startedCall{battleID: battleID, matchID: matchID, playerIDs: playerIDs})
	return nil
}

// TestE2EMatcherQueue 验证 M6 验收：双 ticket 入队 →
// 成局事件（nats）与开局调用（sink）可观测 + matchmaker 队列状态断言。
func TestE2EMatcherQueue(t *testing.T) {
	if reason := probeCore(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// nats 订阅成局事件。
	nc, err := nats.Connect(nats.Options{URL: itNatsURL, Name: "e2e-matcher-observer"})
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	defer nc.Close()
	startedEvent := make(chan *matcherv1.MatchStartedEvent, 4)
	if _, err := nats.Subscribe(nc, consts.MatchStartedTopic(), func(_ string, data []byte) {
		var ev matcherv1.MatchStartedEvent
		if err := protojson.Unmarshal(data, &ev); err == nil {
			startedEvent <- &ev
		}
	}); err != nil {
		t.Fatalf("订阅成局事件: %v", err)
	}

	// matcher 装配（注入记录 sink 断言开局调用）。
	sink := &recordSink{nc: nc}
	m, err := matcherassemble.New(ctx, matcherassemble.Options{
		NodeID:        "matcher-it",
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		RedisAddr:     itRedisAddr,
		SinkOverride:  sink,
	})
	if err != nil {
		t.Fatalf("matcher 装配: %v", err)
	}
	defer m.Stop(context.Background())

	gcli, err := atlasgrpc.DialInsecure(ctx, atlasgrpc.WithEndpoint(m.GRPCURL))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer gcli.Close()
	svc := matcherv1.NewMatcherClient(gcli)

	// 双 ticket 入队（等级相近）。
	queue := func(id string, level int32) string {
		reply, err := svc.QueueMatch(ctx, &matcherv1.QueueMatchRequest{
			PlayerId: id,
			Player:   &commonv1.PlayerSummary{PlayerId: id, Level: level},
		})
		if err != nil {
			t.Fatalf("入队 %s: %v", id, err)
		}
		return reply.GetTicketId()
	}
	p1 := testAccount(t, "p-m1")
	p2 := testAccount(t, "p-m2")
	t1 := queue(p1, 10)
	t2 := queue(p2, 12)

	// 断言成局事件（nats）。
	select {
	case ev := <-startedEvent:
		if ev.GetMatchId() == "" || ev.GetBattleId() == "" || len(ev.GetPlayerIds()) != 2 {
			t.Fatalf("成局事件不符: %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("未收到成局事件")
	}

	// 断言开局调用可观测且恰好一次（成局去重：同局双 ticket 只触发一次）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		sink.mu.Lock()
		n := len(sink.started)
		sink.mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("开局调用未触发")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 留出第二个 ticket 事件的处理窗口，断言无重复。
	time.Sleep(300 * time.Millisecond)
	sink.mu.Lock()
	startedN, publishedN := len(sink.started), len(sink.published)
	call := sink.started[0]
	sink.mu.Unlock()
	if startedN != 1 || publishedN != 1 {
		t.Fatalf("成局应恰好一次：开局 %d 次、发布 %d 次（去重失效）", startedN, publishedN)
	}
	if call.matchID == "" || len(call.playerIDs) != 2 {
		t.Fatalf("开局调用不符: %+v", call)
	}
	// 事件总线同样只收到一条。
	select {
	case ev := <-startedEvent:
		t.Fatalf("收到重复成局事件: %+v", ev)
	case <-time.After(300 * time.Millisecond):
	}

	// 断言查询状态 matched（双方）。
	for _, id := range []string{p1, p2} {
		q, err := svc.QueryMatch(ctx, &matcherv1.QueryMatchRequest{PlayerId: id})
		if err != nil {
			t.Fatalf("查询 %s: %v", id, err)
		}
		if q.GetState() != matcherv1.MatchState_MATCH_STATE_MATCHED || q.GetMatchId() == "" {
			t.Fatalf("%s 状态 = %+v, want MATCHED", id, q)
		}
	}

	// 断言 matchmaker 队列状态（后端终态）。
	for _, tid := range []string{t1, t2} {
		ticket, err := m.Service.Describe(ctx, tid)
		if err != nil {
			t.Fatalf("Describe %s: %v", tid, err)
		}
		if ticket.State != matchmaker.TicketCompleted {
			t.Fatalf("ticket %s 状态 = %s, want completed", tid, ticket.State)
		}
	}
}
