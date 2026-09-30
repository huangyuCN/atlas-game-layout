// Package stream 维护 battle 侧「player_id → 直连连接」注册表、连接生命周期桥与直连推送端口：
// 验票通过的帧连接在此登记（重连时新连接接管旧连接），战斗域通知（帧广播、战斗结束、出局）
// 经帧引擎推送原语直发客户端（规格 §1.1）；结算后按对局关闭全部直连（规格 §9.8）。
// 帧引擎的连接建立/断开事件经 Bridge 转成 actor 消息（掉线计时与重连回座，规格 §9.1/§9.2）。
package stream

import (
	"strconv"

	"github.com/huangyuCN/atlas/transport"
)

// Endpoint 是一条直连端点在帧引擎里的标识（与帧引擎 ConnEvent.ID 同源，规格 §9.1）：
// 流式面（KCP/WS）为连接 ID 的十进制串，数据报面（UDP）为对端键。
// 断开事件按它反查玩家——两端标识口径不一致就永远对不上号。
type Endpoint struct {
	// Kind 是帧面类型（transport.KindKCP / KindWebSocket / KindUDP）。
	Kind transport.Kind
	// ID 是端点标识（流式面 = connID 十进制串；数据报面 = 对端键）。
	ID string
}

// Conn 是一条直连连接的寻址句柄：流式面（KCP/WS）按连接 ID 寻址，数据报面（UDP）按对端键寻址。
type Conn struct {
	// Kind 是帧面类型（transport.KindKCP / KindWebSocket / KindUDP）。
	Kind transport.Kind
	// ConnID 是流式连接 ID（KCP/WS；由帧引擎分配，经请求上下文取得）。
	ConnID uint64
	// Peer 是数据报对端键（UDP；由帧引擎按源地址维护）。
	Peer string
	// BattleID 是所属对局（结算后按对局关闭，规格 §9.8）。
	BattleID string
}

// Endpoint 返回该连接在帧引擎里的端点标识（掉线事件反查与重复登记去重的口径）。
func (c Conn) Endpoint() Endpoint {
	if c.Peer != "" {
		return Endpoint{Kind: c.Kind, ID: c.Peer}
	}
	return Endpoint{Kind: c.Kind, ID: strconv.FormatUint(c.ConnID, 10)}
}

// Port 是一类帧面的直连端口：推送 + 关闭（适配见 ConnPort / DatagramPort）。
type Port interface {
	// Push 向该连接推送一条 Notify 消息。
	Push(c Conn, operation string, msg any) error
	// Close 关闭该连接（数据报面为移除对端）。
	Close(c Conn) error
}

// ConnServer 是连接式帧传输的直连能力（*kcp.Server 与 *websocket.Server 均实现）。
type ConnServer interface {
	// Push 按连接 ID 推送一条 Notify 消息。
	Push(connID uint64, operation string, msg any) error
	// CloseConn 主动关闭指定连接。
	CloseConn(connID uint64) error
}

// ConnPort 适配连接式帧面（KCP/WS：按连接 ID 寻址）。
type ConnPort struct {
	// Srv 是连接式帧传输服务端。
	Srv ConnServer
}

// Push 实现 Port：按连接 ID 推送。
func (p ConnPort) Push(c Conn, operation string, msg any) error {
	return p.Srv.Push(c.ConnID, operation, msg)
}

// Close 实现 Port：按连接 ID 关闭。
func (p ConnPort) Close(c Conn) error { return p.Srv.CloseConn(c.ConnID) }

// DatagramServer 是数据报帧传输的直连能力（*udp.Server 实现）。
type DatagramServer interface {
	// PushTo 按对端键推送一条 Notify 数据报。
	PushTo(peer string, operation string, msg any) error
	// DropPeer 主动移除指定对端。
	DropPeer(peer string) error
}

// DatagramPort 适配数据报帧面（UDP：按 peer 键寻址）。
type DatagramPort struct {
	// Srv 是数据报帧传输服务端。
	Srv DatagramServer
}

// Push 实现 Port：按对端键推送。
func (p DatagramPort) Push(c Conn, operation string, msg any) error {
	return p.Srv.PushTo(c.Peer, operation, msg)
}

// Close 实现 Port：移除对端（UDP 无连接语义）。
func (p DatagramPort) Close(c Conn) error { return p.Srv.DropPeer(c.Peer) }
