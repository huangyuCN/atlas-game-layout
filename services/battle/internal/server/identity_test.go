package server

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
)

// testTicketKey 返回 32 字节测试密钥（字节 0x01..0x20，与 battle 配置样例同形）。
func testTicketKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return key
}

// fakeTransport 是帧请求上下文（kind + 连接 ID/peer + 帧请求头）。
type fakeTransport struct {
	kind transport.Kind
	conn uint64
	peer string
	hdr  map[string]string
}

// Kind 返回帧面类型。
func (f *fakeTransport) Kind() transport.Kind { return f.kind }

// Endpoint 返回测试端点占位。
func (f *fakeTransport) Endpoint() string { return "test://battle-frame" }

// Operation 返回空 operation（身份解析不依赖它）。
func (f *fakeTransport) Operation() string { return "" }

// RequestHeader 返回帧请求头。
func (f *fakeTransport) RequestHeader() transport.Header { return fakeHeader(f.hdr) }

// ReplyHeader 返回 nil（帧传输无独立响应头语义）。
func (f *fakeTransport) ReplyHeader() transport.Header { return nil }

// ConnID 返回连接 ID（流式面推送寻址）。
func (f *fakeTransport) ConnID() uint64 { return f.conn }

// Peer 返回对端键（数据报面推送寻址）。
func (f *fakeTransport) Peer() string { return f.peer }

// fakeHeader 是帧请求头的 map 实现。
type fakeHeader map[string]string

// Get 读取键值。
func (h fakeHeader) Get(key string) string { return h[key] }

// Set 写入键值。
func (h fakeHeader) Set(key, value string) { h[key] = value }

// Add 追加键值（单值形态与 Set 同义）。
func (h fakeHeader) Add(key, value string) { h[key] = value }

// Delete 删除键。
func (h fakeHeader) Delete(key string) { delete(h, key) }

// Keys 返回全部键名。
func (h fakeHeader) Keys() []string {
	out := make([]string, 0, len(h))
	for k := range h {
		out = append(out, k)
	}
	return out
}

// Values 返回键的全部取值。
func (h fakeHeader) Values(key string) []string {
	if v, ok := h[key]; ok {
		return []string{v}
	}
	return nil
}

// frameCtx 构造帧请求上下文（slot 为空即无会话槽）。
func frameCtx(kind transport.Kind, connID uint64, peer, slot string) context.Context {
	hdr := map[string]string{}
	if slot != "" {
		hdr[frame.RequestHeaderKeySession] = slot
	}
	tr := &fakeTransport{kind: kind, conn: connID, peer: peer, hdr: hdr}
	return transport.NewServerContext(context.Background(), tr)
}

// encodeTicket 用测试密钥签一张票据（ttl 为有效期，负值即已过期）。
func encodeTicket(t *testing.T, playerID, battleID string, ttl time.Duration) []byte {
	t.Helper()
	now := time.Now()
	raw, err := ticket.Encode(ticket.Ticket{
		Version:   ticket.Version1,
		KID:       1,
		PlayerID:  playerID,
		BattleID:  battleID,
		IssuedAt:  now,
		ExpiresAt: now.Add(ttl),
	}, testTicketKey())
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return raw
}

// b64 返回 base64url（无填充）编码的票面值（帧会话槽取值约定）。
func b64(raw []byte) string { return base64.RawURLEncoding.EncodeToString(raw) }

// TestTicketIdentityResolve 验证合法票：解出玩家/对局身份并登记该连接。
func TestTicketIdentityResolve(t *testing.T) {
	reg := stream.NewRegistry()
	resolver := NewTicketIdentity(testTicketKey(), stream.NewBridge(reg, nil))
	ctx := frameCtx(transport.KindKCP, 7, "", b64(encodeTicket(t, "p-a", "b-1", time.Minute)))

	id, err := resolver.Resolve(ctx, relay.RouteEntry{Operation: "/battle.v1.BattleService/JoinBattle"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if id != (frameops.Identity{PlayerID: "p-a", BattleID: "b-1"}) {
		t.Fatalf("身份 = %+v, 期望 {p-a b-1}", id)
	}
	if reg.Count() != 1 || reg.CountBattle("b-1") != 1 {
		t.Fatalf("连接未登记: count=%d battle=%d", reg.Count(), reg.CountBattle("b-1"))
	}
}

// TestTicketIdentityResolveUDPRegistersPeer 验证数据报面按 peer 键登记（UDP 无连接语义）。
func TestTicketIdentityResolveUDPRegistersPeer(t *testing.T) {
	reg := stream.NewRegistry()
	resolver := NewTicketIdentity(testTicketKey(), stream.NewBridge(reg, nil))
	ctx := frameCtx(transport.KindUDP, 0, "1.2.3.4:9", b64(encodeTicket(t, "p-a", "b-1", time.Minute)))

	if _, err := resolver.Resolve(ctx, relay.RouteEntry{}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if reg.Count() != 1 {
		t.Fatalf("UDP 连接未登记: count=%d", reg.Count())
	}
}

// TestTicketIdentityRejectsMissingSlot 验证无会话槽 → BATTLE_TICKET_INVALID 且不登记。
func TestTicketIdentityRejectsMissingSlot(t *testing.T) {
	reg := stream.NewRegistry()
	resolver := NewTicketIdentity(testTicketKey(), stream.NewBridge(reg, nil))

	_, err := resolver.Resolve(frameCtx(transport.KindKCP, 7, "", ""), relay.RouteEntry{})
	if !errorv1.IsBattleTicketInvalid(err) {
		t.Fatalf("无槽错误 = %v（reason=%s）, 期望 BATTLE_TICKET_INVALID", err, atlaserrors.Reason(err))
	}
	if atlaserrors.Reason(err) != errorv1.ReasonBattleTicketInvalid() {
		t.Fatalf("reason = %q, 期望 %q", atlaserrors.Reason(err), errorv1.ReasonBattleTicketInvalid())
	}
	if reg.Count() != 0 {
		t.Fatalf("验票失败仍登记了连接: count=%d", reg.Count())
	}
}

// TestTicketIdentityRejectsBadTicket 验证坏票（非法 base64 / 被篡改）→ BATTLE_TICKET_INVALID。
func TestTicketIdentityRejectsBadTicket(t *testing.T) {
	reg := stream.NewRegistry()
	resolver := NewTicketIdentity(testTicketKey(), stream.NewBridge(reg, nil))

	cases := map[string]string{
		"非法 base64": "不是-base64url!",
		"被篡改":       b64(tamper(encodeTicket(t, "p-a", "b-1", time.Minute))),
		"截断":        b64(encodeTicket(t, "p-a", "b-1", time.Minute)[:8]),
	}
	for name, slot := range cases {
		_, err := resolver.Resolve(frameCtx(transport.KindKCP, 7, "", slot), relay.RouteEntry{})
		if !errorv1.IsBattleTicketInvalid(err) {
			t.Fatalf("%s：错误 = %v（reason=%s）, 期望 BATTLE_TICKET_INVALID", name, err, atlaserrors.Reason(err))
		}
	}
	if reg.Count() != 0 {
		t.Fatalf("验票失败仍登记了连接: count=%d", reg.Count())
	}
}

// TestTicketIdentityRejectsExpired 验证过期票 → BATTLE_TICKET_EXPIRED（与坏票区分，SDK 据此决定是否重取票）。
func TestTicketIdentityRejectsExpired(t *testing.T) {
	reg := stream.NewRegistry()
	resolver := NewTicketIdentity(testTicketKey(), stream.NewBridge(reg, nil))
	slot := b64(encodeTicket(t, "p-a", "b-1", -time.Second))

	_, err := resolver.Resolve(frameCtx(transport.KindKCP, 7, "", slot), relay.RouteEntry{})
	if !errorv1.IsBattleTicketExpired(err) {
		t.Fatalf("过期票错误 = %v（reason=%s）, 期望 BATTLE_TICKET_EXPIRED", err, atlaserrors.Reason(err))
	}
	if errorv1.IsBattleTicketInvalid(err) {
		t.Fatal("过期票不应判为 BATTLE_TICKET_INVALID（两个 reason 语义必须分开）")
	}
	if reg.Count() != 0 {
		t.Fatalf("验票失败仍登记了连接: count=%d", reg.Count())
	}
}

// tamper 翻转密文最后一个字节（认证必然失败）。
func tamper(raw []byte) []byte {
	out := append([]byte(nil), raw...)
	out[len(out)-1] ^= 0xff
	return out
}
