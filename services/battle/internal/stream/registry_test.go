package stream

import (
	"context"
	"errors"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/encoding"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// pushRecord 是一次推送的观测记录。
type pushRecord struct {
	conn      Conn
	operation string
	msg       any
}

// rawRecord 是一次**已编码载荷**推送的观测记录（version = 端口收到的载荷编码版本）。
type rawRecord struct {
	conn      Conn
	operation string
	payload   []byte
	version   uint8
}

// fakePort 是记录推送与关闭的帧面端口：
// 推送记为 push（编码下发）/ raw（复用同一份载荷）；failErr 非空时按 failConn 失败
// （failConn = 0 表示所有连接都失败）。
type fakePort struct {
	pushed   []pushRecord
	raw      []rawRecord
	closed   []Conn
	failErr  error
	failConn uint64
}

// Push 实现 Port：记录编码下发（failErr 命中时返回该错误）。
func (p *fakePort) Push(c Conn, operation string, msg any) error {
	if err := p.failure(c); err != nil {
		return err
	}
	p.pushed = append(p.pushed, pushRecord{conn: c, operation: operation, msg: msg})
	return nil
}

// PushRaw 实现 Port：记录已编码载荷下发（含载荷编码版本；failErr 命中时返回该错误）。
func (p *fakePort) PushRaw(c Conn, operation string, payload []byte, ver uint8) error {
	if err := p.failure(c); err != nil {
		return err
	}
	p.raw = append(p.raw, rawRecord{conn: c, operation: operation, payload: payload, version: ver})
	return nil
}

// Close 实现 Port：记录关闭。
func (p *fakePort) Close(c Conn) error {
	p.closed = append(p.closed, c)
	return nil
}

// failure 返回该连接应报的推送错误（nil = 正常推送）；failConn 只让指定连接失败，
// 用于验证「单目标写失败不影响其余目标」。
func (p *fakePort) failure(c Conn) error {
	if p.failErr == nil {
		return nil
	}
	if p.failConn != 0 && c.ConnID != p.failConn {
		return nil
	}
	return p.failErr
}

// kcpConn 返回一条 KCP 面的连接句柄。
func kcpConn(connID uint64, battleID string) Conn {
	return Conn{Kind: transport.KindKCP, ConnID: connID, BattleID: battleID}
}

// newTestRegistry 返回挂了 KCP/UDP 两个假端口的注册表。
func newTestRegistry() (*Registry, *fakePort, *fakePort) {
	reg := NewRegistry()
	kcpPort, udpPort := &fakePort{}, &fakePort{}
	reg.BindPort(transport.KindKCP, kcpPort)
	reg.BindPort(transport.KindUDP, udpPort)
	return reg, kcpPort, udpPort
}

// decodePayload 用 json codec（protojson）解码一条推送载荷——与客户端收到的字节同源。
func decodePayload(payload []byte, v any) error {
	codec := encoding.GetCodec("json")
	if codec == nil {
		return errCodecMissing
	}
	return codec.Unmarshal(payload, v)
}

// TestRegistryRegisterUnregister 验证登记后推送落对应连接、注销后不再推送。
func TestRegistryRegisterUnregister(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	conn := kcpConn(7, "b-1")
	reg.Register("p-a", conn)
	if reg.Count() != 1 {
		t.Fatalf("登记后连接数 = %d, 期望 1", reg.Count())
	}

	frame := &locksteppb.LockstepFrame{FrameId: 3}
	if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, frame); err != nil {
		t.Fatalf("PublishFrames: %v", err)
	}
	if len(kcpPort.raw) != 1 {
		t.Fatalf("推送次数 = %d, 期望 1", len(kcpPort.raw))
	}
	got := kcpPort.raw[0]
	if got.conn != conn || got.operation != battlev1opclient.BattleServicePushOps.FrameBroadcast {
		t.Fatalf("推送记录不符: %+v", got)
	}
	var bc battlev1.FrameBroadcast
	if err := decodePayload(got.payload, &bc); err != nil {
		t.Fatalf("帧广播载荷解码失败: %v", err)
	}
	if bc.GetBattleId() != "b-1" || bc.GetFrame().GetFrameId() != 3 {
		t.Fatalf("帧广播载荷不符: %+v", &bc)
	}

	reg.Unregister("p-a", conn)
	if reg.Count() != 0 {
		t.Fatalf("注销后连接数 = %d, 期望 0", reg.Count())
	}
	if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, frame); err != nil {
		t.Fatalf("未登记玩家的推送不应报错: %v", err)
	}
	if len(kcpPort.raw) != 1 {
		t.Fatalf("注销后仍收到推送: %d", len(kcpPort.raw))
	}
}

// TestRegistryReconnectTakeover 验证重连接管：新连接登记后旧连接被关闭且不再收到推送。
func TestRegistryReconnectTakeover(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	old, fresh := kcpConn(7, "b-1"), kcpConn(8, "b-1")
	reg.Register("p-a", old)
	reg.Register("p-a", fresh)

	if reg.Count() != 1 {
		t.Fatalf("接管后连接数 = %d, 期望 1（同玩家只保留一条）", reg.Count())
	}
	if len(kcpPort.closed) != 1 || kcpPort.closed[0] != old {
		t.Fatalf("旧连接未被关闭: %+v", kcpPort.closed)
	}
	if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, &locksteppb.LockstepFrame{FrameId: 1}); err != nil {
		t.Fatalf("PublishFrames: %v", err)
	}
	if len(kcpPort.raw) != 1 || kcpPort.raw[0].conn != fresh {
		t.Fatalf("推送未落新连接: %+v", kcpPort.raw)
	}
	// 旧连接的迟到注销不得摘掉新连接。
	reg.Unregister("p-a", old)
	if reg.Count() != 1 {
		t.Fatalf("迟到注销摘掉了新连接: 连接数 = %d", reg.Count())
	}
}

// TestRegistryPushFailureUnregisters 验证连接已死（推送报 ErrConnNotFound）时按失效注销。
func TestRegistryPushFailureUnregisters(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	reg.Register("p-a", kcpConn(7, "b-1"))
	kcpPort.failErr = engine.ErrConnNotFound

	err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, &locksteppb.LockstepFrame{FrameId: 1})
	if !errors.Is(err, engine.ErrConnNotFound) {
		t.Fatalf("推送错误 = %v, 期望 ErrConnNotFound", err)
	}
	if reg.Count() != 0 {
		t.Fatalf("失效连接未注销: 连接数 = %d", reg.Count())
	}
}

// TestRegistryUDPPushByPeer 验证数据报面按 peer 键推送（UDP 无连接语义）。
func TestRegistryUDPPushByPeer(t *testing.T) {
	reg, _, udpPort := newTestRegistry()
	conn := Conn{Kind: transport.KindUDP, Peer: "1.2.3.4:9", BattleID: "b-1"}
	reg.Register("p-a", conn)
	if err := reg.PublishEnd(context.Background(), "p-a", "b-1", "p-a"); err != nil {
		t.Fatalf("PublishEnd: %v", err)
	}
	if len(udpPort.pushed) != 1 {
		t.Fatalf("UDP 推送次数 = %d, 期望 1", len(udpPort.pushed))
	}
	got := udpPort.pushed[0]
	if got.conn != conn || got.operation != battlev1opclient.BattleServicePushOps.BattleEndNotify {
		t.Fatalf("结束通知推送不符: %+v", got)
	}
	if end, ok := got.msg.(*battlev1.BattleEndNotify); !ok || end.GetWinnerPlayerId() != "p-a" {
		t.Fatalf("结束通知载荷不符: %+v", got.msg)
	}
}

// TestRegistryCloseBattle 验证结算后关闭本局全部直连（规格 §9.8）：只关本局、其余对局不受影响。
func TestRegistryCloseBattle(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	reg.Register("p-a", kcpConn(7, "b-1"))
	reg.Register("p-b", kcpConn(8, "b-1"))
	reg.Register("p-c", kcpConn(9, "b-2"))

	reg.CloseBattle("b-1")
	if reg.Count() != 1 || reg.CountBattle("b-1") != 0 {
		t.Fatalf("关闭后连接数 = %d（b-1 剩 %d）, 期望 1/0", reg.Count(), reg.CountBattle("b-1"))
	}
	if len(kcpPort.closed) != 2 {
		t.Fatalf("关闭记录数 = %d, 期望 2", len(kcpPort.closed))
	}
	if err := reg.PublishFrames(context.Background(), "b-1", []string{"p-a"}, &locksteppb.LockstepFrame{FrameId: 1}); err != nil {
		t.Fatalf("已关闭对局的推送不应报错: %v", err)
	}
	if len(kcpPort.raw) != 0 {
		t.Fatalf("已关闭对局仍收到推送: %+v", kcpPort.raw)
	}
	reg.CloseBattle("b-2")
	if reg.Count() != 0 {
		t.Fatalf("全部关闭后连接数 = %d, 期望 0", reg.Count())
	}
}
