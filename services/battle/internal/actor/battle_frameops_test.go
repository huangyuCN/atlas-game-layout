package actor

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/server"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
)

// frameTestKey 返回 32 字节票据测试密钥（字节 0x01..0x20）。
func frameTestKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

// frameTransport 是帧请求上下文（KCP 面 + 连接 ID + 帧请求头）。
type frameTransport struct {
	conn uint64
	hdr  map[string]string
}

// Kind 返回帧面类型（KCP）。
func (f *frameTransport) Kind() transport.Kind { return transport.KindKCP }

// Endpoint 返回测试端点占位。
func (f *frameTransport) Endpoint() string { return "test://battle-frame" }

// Operation 返回空 operation。
func (f *frameTransport) Operation() string { return "" }

// RequestHeader 返回帧请求头。
func (f *frameTransport) RequestHeader() transport.Header { return frameHeader(f.hdr) }

// ReplyHeader 返回 nil。
func (f *frameTransport) ReplyHeader() transport.Header { return nil }

// ConnID 返回连接 ID。
func (f *frameTransport) ConnID() uint64 { return f.conn }

// frameHeader 是帧请求头的 map 实现。
type frameHeader map[string]string

// Get 读取键值。
func (h frameHeader) Get(key string) string { return h[key] }

// Set 写入键值。
func (h frameHeader) Set(key, value string) { h[key] = value }

// Add 追加键值。
func (h frameHeader) Add(key, value string) { h[key] = value }

// Delete 删除键。
func (h frameHeader) Delete(key string) { delete(h, key) }

// Keys 返回键名列表。
func (h frameHeader) Keys() []string {
	out := make([]string, 0, len(h))
	for k := range h {
		out = append(out, k)
	}
	return out
}

// Values 返回键的全部取值。
func (h frameHeader) Values(key string) []string {
	if v, ok := h[key]; ok {
		return []string{v}
	}
	return nil
}

// frameSlotCtx 构造带会话槽的帧请求上下文（slot 为空即不带票）。
func frameSlotCtx(connID uint64, slot string) context.Context {
	hdr := map[string]string{}
	if slot != "" {
		hdr[frame.RequestHeaderKeySession] = slot
	}
	return transport.NewServerContext(context.Background(), &frameTransport{conn: connID, hdr: hdr})
}

// ticketSlot 用给定密钥签票并编码为帧会话槽取值（base64url 无填充）。
func ticketSlot(t *testing.T, key []byte, playerID, battleID string, ttl time.Duration) string {
	t.Helper()
	now := time.Now()
	raw, err := ticket.Encode(ticket.Ticket{
		Version:   ticket.Version1,
		KID:       1,
		PlayerID:  playerID,
		BattleID:  battleID,
		IssuedAt:  now,
		ExpiresAt: now.Add(ttl),
	}, key)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// entryOf 取路由条目（帧 handler 的注册期入口，与 frameroute 注册口径一致）。
func entryOf(t *testing.T, operation string) relay.RouteEntry {
	t.Helper()
	entry, ok := battlev1.BattleServiceRouteTable[operation]
	if !ok {
		t.Fatalf("路由表缺少 %s", operation)
	}
	return entry
}

// TestFrameOpsLocalDeliverChain 验证帧 op 真实链路：帧请求（带票）→ frameops.Handle →
// LocalDeliverer（本地 actor 投递）→ BattleActor.JoinBattle 方法体 → 回执返回。
func TestFrameOpsLocalDeliverChain(t *testing.T) {
	key := frameTestKey()
	cfg := testBattleConfig()
	cfg.TicketKey, cfg.TicketTTL, cfg.EdgeEndpoints = key, time.Minute, testEdgeEndpoints()
	env := newBattleEnvWith(t, cfg)
	reg := stream.NewRegistry()
	ops, err := server.NewFrameOps(localRT{env.rt}, stream.NewBridge(reg, nil), key)
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	if _, err := env.ask(context.Background(), &battlev1.CreateBattleRequest{MatchId: "m-1", PlayerIds: []string{"p-a", "p-b"}}); err != nil {
		t.Fatalf("create: %v", err)
	}

	slot := ticketSlot(t, key, "p-a", "b-test01", time.Minute)
	rep, err := ops.Handle(frameSlotCtx(7, slot), entryOf(t, "/battle.v1.BattleService/JoinBattle"),
		&battlev1.JoinBattleReq{BattleId: "b-test01"})
	if err != nil {
		t.Fatalf("帧 JoinBattle: %v", err)
	}
	join, ok := rep.(*battlev1.JoinBattleReply)
	if !ok || join.GetMeta().GetSessionId() != "b-test01" {
		t.Fatalf("帧 JoinBattle 回执不符: %T %+v", rep, rep)
	}
	// 验票通过即登记直连连接（重连接管见 stream 包用例）。
	if reg.Count() != 1 || reg.CountBattle("b-test01") != 1 {
		t.Fatalf("帧连接未登记: count=%d battle=%d", reg.Count(), reg.CountBattle("b-test01"))
	}
}

// TestFrameOpsRejectsBadTicket 验证帧槽验票失败的三类负例与两个 reason 的区分：
// 无槽/被篡改 → BATTLE_TICKET_INVALID；过期 → BATTLE_TICKET_EXPIRED；失败不登记连接。
func TestFrameOpsRejectsBadTicket(t *testing.T) {
	key := frameTestKey()
	cfg := testBattleConfig()
	cfg.TicketKey, cfg.TicketTTL, cfg.EdgeEndpoints = key, time.Minute, testEdgeEndpoints()
	env := newBattleEnvWith(t, cfg)
	reg := stream.NewRegistry()
	ops, err := server.NewFrameOps(localRT{env.rt}, stream.NewBridge(reg, nil), key)
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}

	req := &battlev1.JoinBattleReq{BattleId: "b-test01"}
	entry := entryOf(t, "/battle.v1.BattleService/JoinBattle")
	cases := []struct {
		name string
		slot string
		want func(error) bool
	}{
		{"无槽", "", errorv1.IsBattleTicketInvalid},
		{"过期票", ticketSlot(t, key, "p-a", "b-test01", -time.Second), errorv1.IsBattleTicketExpired},
		{"被篡改", tamperSlot(t, ticketSlot(t, key, "p-a", "b-test01", time.Minute)), errorv1.IsBattleTicketInvalid},
		{"非法 base64", "$$$", errorv1.IsBattleTicketInvalid},
	}
	for _, tc := range cases {
		_, err := ops.Handle(frameSlotCtx(7, tc.slot), entry, req)
		if err == nil || !tc.want(err) {
			t.Fatalf("%s：错误 = %v（reason=%s）", tc.name, err, atlaserrors.Reason(err))
		}
	}
	if reg.Count() != 0 {
		t.Fatalf("验票失败仍登记了连接: count=%d", reg.Count())
	}
	if got := frameops.RequestIDOf(frameSlotCtx(7, "")); got != "" {
		t.Fatalf("无请求 ID 头时 RequestIDOf = %q, 期望空串", got)
	}
}

// tamperSlot 翻转票据密文的最后一字节后再编码（认证必然失败）。
func tamperSlot(t *testing.T, slot string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(slot)
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}
	raw[len(raw)-1] ^= 0xff
	return base64.RawURLEncoding.EncodeToString(raw)
}
