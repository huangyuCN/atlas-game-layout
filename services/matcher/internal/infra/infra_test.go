package infra

import (
	"context"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	"github.com/huangyuCN/atlas/namespace"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// stubBattleClient 是 battle internal 面类型化客户端的桩：不开 gRPC，
// 记录调用请求并回放预置出票回执（"可用桩 client"的取票链路用例）。
type stubBattleClient struct {
	createReq *battlev1.CreateBattleRequest
	issueReq  *battlev1.IssueEntryTicketReq
	reply     *battlev1.IssueEntryTicketReply
	err       error
}

func (s *stubBattleClient) Create(_ context.Context, req *battlev1.CreateBattleRequest) (*battlev1.CreateBattleReply, error) {
	s.createReq = req
	return &battlev1.CreateBattleReply{BattleId: req.GetBattleId()}, nil
}

func (s *stubBattleClient) GetState(context.Context, *battlev1.GetStateReq) (*battlev1.GetStateReply, error) {
	return &battlev1.GetStateReply{}, nil
}

func (s *stubBattleClient) IssueEntryTicket(_ context.Context, req *battlev1.IssueEntryTicketReq) (*battlev1.IssueEntryTicketReply, error) {
	s.issueReq = req
	if s.err != nil {
		return nil, s.err
	}
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

// TestSinkCarriesBattleTicketsIntoStartedEvent 验证 matcher 取票链路：
// 按 battle_id 调 IssueEntryTicket（桩 client）→ 成局事件带上 battle 回执的
// endpoint 与逐人票据，且每张票都能解出对应玩家与本局 battle_id。
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
	if err := sink.Start(ctx, "b-1", "m-1", playerIDs); err != nil {
		t.Fatalf("开局: %v", err)
	}
	if got := stub.createReq.GetBattleId(); got != "b-1" {
		t.Fatalf("开局目标 battle_id = %q, 期望 b-1", got)
	}
	issued, err := sink.IssueEntryTicket(ctx, "b-1")
	if err != nil {
		t.Fatalf("取票: %v", err)
	}
	if got := stub.issueReq.GetBattleId(); got != "b-1" {
		t.Fatalf("取票目标 battle_id = %q, 期望 b-1（客体寻址）", got)
	}
	if !proto.Equal(issued, stub.reply) {
		t.Fatalf("取票回执 = %v, 期望原样透传 %v", issued, stub.reply)
	}
	if err := sink.PublishStarted(ctx, "b-1", "m-1", playerIDs, issued); err != nil {
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
