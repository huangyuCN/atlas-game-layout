package infra

import (
	"context"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	"github.com/huangyuCN/atlas/namespace"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// stubBattleClient 是 battle internal 面类型化客户端的桩：不开 gRPC，
// 记录入场调用请求并回放预置回执（「一次调用即出票」的用例）。
type stubBattleClient struct {
	admitReq *battlev1.AdmitBattleReq
	// admitCalls/issueCalls 统计两条路径的调用次数：入场链路只能调 Admit（每局 1 次），
	// 旧的两跳出票方法 IssueEntryTicket 必须零调用。
	admitCalls int
	issueCalls int
	reply      *battlev1.IssueEntryTicketReply
	err        error
}

func (s *stubBattleClient) Create(_ context.Context, req *battlev1.CreateBattleRequest) (*battlev1.CreateBattleReply, error) {
	return &battlev1.CreateBattleReply{BattleId: req.GetBattleId()}, nil
}

func (s *stubBattleClient) GetState(context.Context, *battlev1.GetStateReq) (*battlev1.GetStateReply, error) {
	return &battlev1.GetStateReply{}, nil
}

// Admit 实现入场（开局+出票合并）调用桩。
func (s *stubBattleClient) Admit(_ context.Context, req *battlev1.AdmitBattleReq) (*battlev1.IssueEntryTicketReply, error) {
	s.admitReq = req
	s.admitCalls++
	if s.err != nil {
		return nil, s.err
	}
	return s.reply, nil
}

// IssueEntryTicket 保留为「旧两跳路径」的计数桩（入场链路不得再调它）。
func (s *stubBattleClient) IssueEntryTicket(_ context.Context, _ *battlev1.IssueEntryTicketReq) (*battlev1.IssueEntryTicketReply, error) {
	s.issueCalls++
	return s.reply, nil
}

// testTicketKey 返回 32 字节测试密钥（与 battle 侧同形）。
func testTicketKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

// stubReply 构造出票回执桩：三面接入层地址 + 两张真票据（可被 ticket.Decode 解出）。
func stubReply(t *testing.T) *battlev1.IssueEntryTicketReply {
	t.Helper()
	issued := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	reply := &battlev1.IssueEntryTicketReply{
		Endpoints: []*battlev1.EdgeEndpoint{
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.test:7100"},
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: "edge.test:7101"},
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UDP, Address: "edge.test:7102"},
		},
		ExpiresAtUnixMs: issued.Add(2 * time.Minute).UnixMilli(),
	}
	for _, playerID := range []string{"p-1", "p-2"} {
		raw, err := ticket.Encode(ticket.Ticket{
			Version:   ticket.Version1,
			KID:       1,
			PlayerID:  playerID,
			BattleID:  "b-1",
			IssuedAt:  issued,
			ExpiresAt: issued.Add(2 * time.Minute),
		}, testTicketKey())
		if err != nil {
			t.Fatalf("构造票据桩: %v", err)
		}
		reply.Tickets = append(reply.Tickets, &battlev1.BattleTicketEntry{PlayerId: playerID, Ticket: raw})
	}
	return reply
}

// newTestServer 起内存 NATS Server 并返回客户端地址。
func newTestServer(t *testing.T) string {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1})
	if err != nil {
		t.Fatalf("启动 nats-server 失败: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server 未就绪")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// testTopics 构造本次用例独占的 topic 命名空间（发布方与订阅方同源）。
func testTopics(t *testing.T) consts.Topics {
	t.Helper()
	derived, err := namespace.Derive("matcher-infra-test")
	if err != nil {
		t.Fatalf("namespace.Derive: %v", err)
	}
	topics, err := consts.NewTopics(derived)
	if err != nil {
		t.Fatalf("consts.NewTopics: %v", err)
	}
	return topics
}

// TestSinkCarriesBattleTicketsIntoStartedEvent 验证 matcher 入场链路：
// 按 battle_id 调**一次** Admit（开局+出票合并，桩 client）→ 回执原样进成局事件的
// endpoint 与逐人票据，且每张票都能解出对应玩家与本局 battle_id。
// 附「2 跳 → 1 跳」的计数证明：Admit 恰 1 次、IssueEntryTicket 0 次。
func TestSinkCarriesBattleTicketsIntoStartedEvent(t *testing.T) {
	nc, err := pkgnats.Connect(pkgnats.Options{URL: newTestServer(t), Name: "matcher-infra-test"})
	if err != nil {
		t.Fatalf("nats 连接: %v", err)
	}
	t.Cleanup(nc.Close)
	topics := testTopics(t)
	received := make(chan []byte, 1)
	if _, err := pkgnats.Subscribe(nc, topics.MatchStarted(), func(_ string, data []byte) {
		received <- data
	}); err != nil {
		t.Fatalf("订阅成局事件: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // 订阅注册有异步窗口

	stub := &stubBattleClient{reply: stubReply(t)}
	sink := NewSink(pkgnats.NewPublisher(nc, topics), stub)
	ctx := context.Background()
	playerIDs := []string{"p-1", "p-2"}
	tickets, err := sink.Admit(ctx, "b-1", "m-1", playerIDs)
	if err != nil {
		t.Fatalf("入场（开局+出票）: %v", err)
	}
	// 每局 RPC 跳数 2 → 1：只调 Admit 一次，旧的两跳方法零调用。
	if stub.admitCalls != 1 {
		t.Fatalf("battle 调用次数 = %d, 期望 1（开局+出票合并）", stub.admitCalls)
	}
	if stub.issueCalls != 0 {
		t.Fatalf("入场链路仍调用了 IssueEntryTicket %d 次（应为 0）", stub.issueCalls)
	}
	if got := stub.admitReq.GetBattleId(); got != "b-1" {
		t.Fatalf("入场目标 battle_id = %q, 期望 b-1（客体寻址）", got)
	}
	if got := stub.admitReq.GetMatchId(); got != "m-1" {
		t.Fatalf("入场 match_id = %q, 期望 m-1", got)
	}
	if !proto.Equal(tickets, stub.reply) {
		t.Fatalf("入场回执 = %v, 期望原样透传 %v", tickets, stub.reply)
	}
	if err := sink.PublishStarted(ctx, "b-1", "m-1", playerIDs, tickets); err != nil {
		t.Fatalf("发布成局事件: %v", err)
	}

	select {
	case data := <-received:
		var ev matcherv1.MatchStartedEvent
		if err := protojson.Unmarshal(data, &ev); err != nil {
			t.Fatalf("解码成局事件: %v", err)
		}
		if got := ev.GetBattleEndpoints(); len(got) != len(stub.reply.GetEndpoints()) {
			t.Fatalf("事件 battle_endpoints 面数 = %d, 期望 battle 回执 %d 面", len(got), len(stub.reply.GetEndpoints()))
		} else {
			for i, want := range stub.reply.GetEndpoints() {
				if got[i].GetTransport() != want.GetTransport() || got[i].GetAddress() != want.GetAddress() {
					t.Errorf("事件 battle_endpoints[%d] = %s/%s, 期望回执 %s/%s（matcher 只搬运）", i,
						got[i].GetTransport(), got[i].GetAddress(), want.GetTransport(), want.GetAddress())
				}
			}
		}
		if got := len(ev.GetBattleTickets()); got != 2 {
			t.Fatalf("事件票据张数 = %d, 期望 2", got)
		}
		for _, entry := range ev.GetBattleTickets() {
			tk, derr := ticket.Decode(entry.GetTicket(), testTicketKey())
			if derr != nil {
				t.Fatalf("事件中玩家 %s 的票无法解出: %v", entry.GetPlayerId(), derr)
			}
			if tk.PlayerID != entry.GetPlayerId() || tk.BattleID != "b-1" {
				t.Errorf("票内身份 = %s/%s, 期望 %s/b-1", tk.PlayerID, tk.BattleID, entry.GetPlayerId())
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待成局事件超时")
	}
}

// TestMatchmakerRegistry_DefaultTickInterval 验证池扫描周期的缺省值：
// 10ms（原 50ms 把成局速率钉死在 20 局/秒 = 200 玩家/秒），且等待阈值缺省不变。
func TestMatchmakerRegistry_DefaultTickInterval(t *testing.T) {
	if DefaultTickInterval != 10*time.Millisecond {
		t.Fatalf("DefaultTickInterval = %v, 期望 10ms", DefaultTickInterval)
	}
	mm, ok := MatchmakerRegistry(0, 0)[biz.DefaultMatchmakerName]
	if !ok {
		t.Fatal("注册表缺缺省规则集")
	}
	if mm.TickInterval != DefaultTickInterval {
		t.Fatalf("缺省 TickInterval = %v, 期望 %v", mm.TickInterval, DefaultTickInterval)
	}
	if mm.RequestTimeout != DefaultRequestTimeout {
		t.Fatalf("缺省 RequestTimeout = %v, 期望 %v", mm.RequestTimeout, DefaultRequestTimeout)
	}
	if err := mm.Validate(); err != nil {
		t.Fatalf("缺省配置应合法: %v", err)
	}
}

// TestMatchmakerRegistry_TickIntervalConfigurable 验证扫描周期可配置（显式值原样生效）。
func TestMatchmakerRegistry_TickIntervalConfigurable(t *testing.T) {
	mm := MatchmakerRegistry(90*time.Second, 5*time.Millisecond)[biz.DefaultMatchmakerName]
	if mm.TickInterval != 5*time.Millisecond {
		t.Fatalf("TickInterval = %v, 期望 5ms（可配置）", mm.TickInterval)
	}
	if mm.RequestTimeout != 90*time.Second {
		t.Fatalf("RequestTimeout = %v, 期望 90s", mm.RequestTimeout)
	}
	if err := mm.Validate(); err != nil {
		t.Fatalf("显式配置应合法: %v", err)
	}
}
