package actor

import (
	"context"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/server"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"google.golang.org/protobuf/proto"
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

// framePort 是帧面直连端口的内存实现：记录推送的 operation 序列、结算载荷与关闭的连接
// （链路用例据此断言「结算只关一次、迟到 op 只补投不再关」）。
type framePort struct {
	mu     sync.Mutex
	pushed []string // 推送的 operation 序列
	ends   []string // 结算通知的胜者序列
	closed []stream.Conn
}

// Push 实现 stream.Port：记录一次推送。
func (p *framePort) Push(_ stream.Conn, operation string, msg any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pushed = append(p.pushed, operation)
	if n, ok := msg.(*battlev1.BattleEndNotify); ok {
		p.ends = append(p.ends, n.GetWinnerPlayerId())
	}
	return nil
}

// Close 实现 stream.Port：记录一次关闭。
func (p *framePort) Close(c stream.Conn) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = append(p.closed, c)
	return nil
}

// counts 返回推送/结算/关闭的计数快照。
func (p *framePort) counts() (pushed, ends, closed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pushed), len(p.ends), len(p.closed)
}

// frameJoin 经帧面投递一次 JoinBattle（带票帧：验票通过即登记直连）。
func frameJoin(t *testing.T, ops *frameops.Handler, slot string, conn uint64) {
	t.Helper()
	_, err := ops.Handle(frameSlotCtx(conn, slot),
		entryOf(t, battlev1opclient.BattleServiceProtocolOps.JoinBattle),
		&battlev1.JoinBattleReq{BattleId: "b-test01"})
	if err != nil {
		t.Fatalf("帧入局失败: %v", err)
	}
}

// frameInput 经帧面投递一次帧输入（Tell：成功即无回执、无错误）。
func frameInput(t *testing.T, ops *frameops.Handler, slot string, conn, frame uint64, step byte) {
	t.Helper()
	_, err := ops.Handle(frameSlotCtx(conn, slot),
		entryOf(t, battlev1opclient.BattleServiceProtocolOps.SendFrameInput),
		&battlev1.FrameInputReq{BattleId: "b-test01",
			Input: &locksteppb.LockstepInput{FrameId: frame, Payload: []byte{step}}})
	if err != nil {
		t.Fatalf("帧输入 %d 失败: %v", frame, err)
	}
}

// assertOpRejected 断言一次迟到 op 拿到**稳定 reason** 的业务拒绝（BATTLE_ENDED，语义＝该对局已结束），
// 而不是传输错误或超时——SDK 据此停止发送，而不是重试到超时。
func assertOpRejected(t *testing.T, ops *frameops.Handler, operation, slot string, conn uint64, req proto.Message) {
	t.Helper()
	_, err := ops.Handle(frameSlotCtx(conn, slot), entryOf(t, operation), req)
	if !errorv1.IsBattleEnded(err) {
		t.Fatalf("迟到 op %s 错误 = %v（reason=%s），期望 BATTLE_ENDED", operation, err, atlaserrors.Reason(err))
	}
	if got := atlaserrors.Reason(err); got != errorv1.ReasonBattleEnded() {
		t.Fatalf("迟到 op %s reason = %q，期望 %q", operation, got, errorv1.ReasonBattleEnded())
	}
}

// settledFrameEnv 是一条「已结算」的帧面链路夹具：真 actor 运行时 + 真直连注册表 + 假帧面端口。
type settledFrameEnv struct {
	env   *battleEnv
	ops   *frameops.Handler
	port  *framePort
	slots map[string]string // playerID → 帧会话槽（票据密文）
	conns map[string]uint64 // playerID → 直连连接 ID
}

// newSettledFrameEnv 起一条帧面链路并打到结算（返回时 actor 已自停、留档已写、直连已被关闭）：
// 推送与留档都走真实注册表，故关闭次数、补投次数与拒绝判定都是链路真实行为。
func newSettledFrameEnv(t *testing.T) *settledFrameEnv {
	t.Helper()
	key := frameTestKey()
	cfg := testBattleConfig()
	cfg.TicketKey, cfg.TicketTTL, cfg.EdgeEndpoints = key, time.Minute, testEdgeEndpoints()
	cfg.TrackLen, cfg.MaxFrames, cfg.SnapshotEvery = 2, 10, 2 // 短赛道：3 帧内分出胜负并带快照
	reg := stream.NewRegistry()
	port := new(framePort)
	reg.BindPort(transport.KindKCP, port)
	env := newBattleEnvDeps(t, cfg, battleDeps{Pusher: reg, Ledger: reg})
	ops, err := server.NewFrameOps(localRT{env.rt}, stream.NewBridge(reg, nil), key)
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	if _, err := env.ask(context.Background(),
		&battlev1.CreateBattleRequest{MatchId: "m-1", PlayerIds: []string{"p-a", "p-b"}}); err != nil {
		t.Fatalf("create: %v", err)
	}
	fx := &settledFrameEnv{env: env, ops: ops, port: port,
		slots: map[string]string{
			"p-a": ticketSlot(t, key, "p-a", "b-test01", time.Minute),
			"p-b": ticketSlot(t, key, "p-b", "b-test01", time.Minute),
		},
		conns: map[string]uint64{"p-a": 7, "p-b": 8},
	}
	for _, p := range []string{"p-a", "p-b"} {
		frameJoin(t, ops, fx.slots[p], fx.conns[p])
	}
	for i := uint64(1); i <= 3; i++ {
		frameInput(t, ops, fx.slots["p-a"], fx.conns["p-a"], i, 1)
		frameInput(t, ops, fx.slots["p-b"], fx.conns["p-b"], i, 0)
	}
	waitSettled(t, env)
	assertActorStopped(t, env)
	return fx
}

// assertLateOpsRejected 连续发送迟到 op（帧输入 + 保活）并要求每条都被稳定 reason 拒绝。
func (fx *settledFrameEnv) assertLateOpsRejected(t *testing.T) {
	t.Helper()
	for i := 0; i < 3; i++ {
		assertOpRejected(t, fx.ops, battlev1opclient.BattleServiceProtocolOps.SendFrameInput,
			fx.slots["p-a"], fx.conns["p-a"],
			&battlev1.FrameInputReq{BattleId: "b-test01",
				Input: &locksteppb.LockstepInput{FrameId: uint64(20 + i), Payload: []byte{1}}})
		assertOpRejected(t, fx.ops, battlev1opclient.BattleServiceProtocolOps.Ping,
			fx.slots["p-a"], fx.conns["p-a"], &battlev1.PingReq{BattleId: "b-test01"})
	}
}

// TestFrameOpsLateOpsRejectedAfterSettle 验证验收暴露的主缺陷已收口：结算后客户端继续发帧 op →
// 帧面在**懒激活之前**以稳定 reason 拒绝、不重建战斗 actor、直连只被结算关一次，
// 且留档结果补投到重连后的连接上（结算通知不再因单次丢包而永久丢失）。
func TestFrameOpsLateOpsRejectedAfterSettle(t *testing.T) {
	fx := newSettledFrameEnv(t)
	_, endsSettled, closesSettled := fx.port.counts()
	if endsSettled == 0 {
		t.Fatal("结算未下发结束通知")
	}
	if closesSettled != len(fx.conns) {
		t.Fatalf("结算关闭的直连数 = %d，期望 %d（每玩家一条，只关一次）", closesSettled, len(fx.conns))
	}
	// 迟到 op：结算后客户端在同一张票上继续发帧输入与保活（验收现场每轮 9–13 次重建的输入源）。
	fx.assertLateOpsRejected(t)
	_, endsLate, closesLate := fx.port.counts()
	if closesLate != closesSettled {
		t.Fatalf("迟到 op 再次关闭了直连：关闭数 %d → %d（结算只关一次）", closesSettled, closesLate)
	}
	if endsLate <= endsSettled {
		t.Fatal("重连后未补投结算结果（错过推送的玩家仍然不知道结果）")
	}
	if _, ok := fx.env.rt.Stats(fx.env.pid); ok {
		t.Fatal("迟到 op 经 SpawnAuto 重建了战斗 actor（空名单实例会立刻再次结算）")
	}
}
