// kcpconn.go 提供「经接入层 KCP 面」的帧连接载体：接入层仍是 UDP 数据报面（首包 hello 换
// flow-id、此后双向带 8 字节前缀），KCP 会话建在「带前缀的 PacketConn」之上，端到端跑在
// 客户端与 battle 的 KCP 帧面之间（接入层只转发字节、不解析 KCP）。
//
// 与 edgeconn.go 的关系：握手（udpHello）与帧编解码（frame.ReadInto/Encode）复用同一套原语，
// 唯一差别是承载从「一报一帧」换成「KCP 会话上的字节流」；帧头/长度等线格式完全不变。
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/encoding"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"github.com/huangyuCN/atlas/transport/frame/engine"
	kcpgo "github.com/xtaci/kcp-go/v5"
)

// battle 帧面 KCP 缺省参数：battle/config.yaml 的 kcp 段未显式配置任何会话参数时，
// 框架 transport/kcp 的缺省值（session_config.go defaultKcpSessionConfig）。
// 两端必须同档，否则窗口/MTU/重传策略不一致会让测量失真。
const (
	kcpNoDelay  = 0   // 0=常规 RTO 退避（battle 缺省；fast_profile 才置 1）
	kcpInterval = 40  // KCP 内部 update/flush 间隔（ms）
	kcpResend   = 0   // 快速重传阈值（0=关闭）
	kcpNC       = 0   // 拥塞控制（0=启用）
	kcpSndWnd   = 128 // 发送窗口（包）
	kcpRcvWnd   = 128 // 接收窗口（包）
	kcpMTU      = 1400
)

// edgeKCPConn 是一条经接入层 KCP 面的连接（flow-id 已就绪，KCP 会话跑在其上）。
type edgeKCPConn struct {
	sess *kcpgo.UDPSession
}

// edgeKCPAdapter 实现 engine.ClientAdapter[*edgeKCPConn]：拨号 = 接入层 hello + 建 KCP 会话。
type edgeKCPAdapter struct {
	addr   string      // 接入层 KCP 面地址（host:port）
	ticket []byte      // 票据密文（hello 段载荷）
	hello  helloPolicy // hello 握手重试策略（单次超时 / 尝试次数 / 退避，见 edgeconn.go）
}

// Dial 拨接入层 KCP 面：同一 UDP socket 上先 hello 换 flow-id，再在其上建 KCP 会话。
// hello 的重试与超时策略与 UDP 面共用（dialHello）：被限流/票类拒绝时同样只能靠读超时发现；
// 后端类失败则有 1 字节「暂不可用」回执，走百毫秒级快速重试（见 edgeconn.go 的 udpHello）。
func (a edgeKCPAdapter) Dial(ctx context.Context) (*edgeKCPConn, error) {
	udpConn, flowID, err := dialHello(ctx, a.addr, a.ticket, a.hello)
	if err != nil {
		return nil, err
	}
	pc := &flowPacketConn{PacketConn: datagramConn{conn: udpConn}, flowID: flowID}
	sess, err := kcpgo.NewConn(a.addr, nil, 0, 0, pc)
	if err != nil {
		_ = udpConn.Close()
		return nil, fmt.Errorf("建立 KCP 会话失败（接入层=%s）: %w", a.addr, err)
	}
	applyBattleKCPDefaults(sess)
	return &edgeKCPConn{sess: sess}, nil
}

// applyBattleKCPDefaults 把 battle 侧缺省 KCP 参数应用到会话（缺省值见文件头常量）。
func applyBattleKCPDefaults(sess *kcpgo.UDPSession) {
	sess.SetNoDelay(kcpNoDelay, kcpInterval, kcpResend, kcpNC)
	sess.SetWindowSize(kcpSndWnd, kcpRcvWnd)
	sess.SetMtu(kcpMTU)
	sess.SetACKNoDelay(true)
	sess.SetWriteDelay(false)
}

// ReadFrame 从 KCP 会话读一个完整帧（帧引擎读取循环调用；字节流语义，帧边界由帧头长度切分）。
func (a edgeKCPAdapter) ReadFrame(c *edgeKCPConn, maxBodySize int) (frame.Header, []byte, error) {
	hdr, body, err := frame.ReadInto(c.sess, maxBodySize)
	if err != nil {
		// 会话关闭/对端 EOF 原样上抛（读循环据此退出）；其余按坏帧处理。
		if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrUnexpectedEOF) {
			return frame.Header{}, nil, err
		}
		return frame.Header{}, nil, fmt.Errorf("%w: %w", engine.ErrBadFrame, err)
	}
	return hdr, body, nil
}

// WriteFrame 编码整帧后以**单个 KCP 消息**写入（不额外加 flow-id：前缀由 flowPacketConn 在
// 数据报层加，KCP 载荷回到 battle 时已被接入层剥掉）。
func (a edgeKCPAdapter) WriteFrame(c *edgeKCPConn, h frame.Header, body []byte, maxBodySize int) error {
	msg, err := frame.Encode(h, body, maxBodySize)
	if err != nil {
		return err
	}
	defer frame.PutBuf(msg)
	_, err = c.sess.Write(msg)
	return err
}

// SetReadDeadline 设置读截止时间（帧引擎的 idle 超时用）。
func (a edgeKCPAdapter) SetReadDeadline(c *edgeKCPConn, t time.Time) error {
	return c.sess.SetReadDeadline(t)
}

// SetWriteDeadline 设置写截止时间。
func (a edgeKCPAdapter) SetWriteDeadline(c *edgeKCPConn, t time.Time) error {
	return c.sess.SetWriteDeadline(t)
}

// Close 关闭 KCP 会话（底层 UDP socket 由进程退出回收）。
func (a edgeKCPAdapter) Close(c *edgeKCPConn) error { return c.sess.Close() }

// RemoteAddr 返回对端地址文本（取不到时回退接入层地址）。
func (a edgeKCPAdapter) RemoteAddr(c *edgeKCPConn) string {
	if c.sess == nil || c.sess.RemoteAddr() == nil {
		return a.addr
	}
	return c.sess.RemoteAddr().String()
}

// dialEdgeKCP 建立一条经接入层 KCP 面的帧连接（票据同时进 hello 段与逐帧会话槽）。
func dialEdgeKCP(ctx context.Context, addr, slot string, ticket []byte, hello helloPolicy) (frameClient, error) {
	cli, err := engine.NewStreamClient[*edgeKCPConn](
		edgeKCPAdapter{addr: addr, ticket: ticket, hello: hello},
		engine.WithClientCodec[*edgeKCPConn](encoding.GetCodec("json")),
		engine.WithClientKind[*edgeKCPConn](transport.KindKCP),
		engine.WithClientSessionProvider[*edgeKCPConn](func() string { return slot }),
		engine.WithClientIdleTimeout[*edgeKCPConn](30*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("构造接入层 KCP 帧客户端失败: %w", err)
	}
	return kcpFrameClient{cli: cli}, nil
}

// kcpFrameClient 把框架帧引擎客户端适配为 frameClient（KCP 面经接入层的载体）。
type kcpFrameClient struct {
	cli *engine.StreamClient[*edgeKCPConn]
}

// Invoke 发起一次帧 op（走框架帧引擎的一元调用语义）。
func (k kcpFrameClient) Invoke(ctx context.Context, operation string, req, resp any) error {
	return k.cli.Invoke(ctx, operation, req, resp)
}

// OnNotify 挂接服务端推送分发（帧广播/结算通知都从这里到达）。
func (k kcpFrameClient) OnNotify(fn func(operation string, payload []byte)) {
	k.cli.SetNotifyHandler(fn)
}

// Close 关闭连接（引擎会读循环退出并释放资源）。
func (k kcpFrameClient) Close() error { return k.cli.Close() }

// datagramConn 是已连接 UDP socket 的 PacketConn 适配：kcp-go 用 WriteTo 发包，而
// *net.UDPConn 在已连接 socket 上 WriteTo 会返回 ErrWriteToConnected，故收敛为 Write。
type datagramConn struct{ conn *net.UDPConn }

// ReadFrom 从已连接 socket 读一个数据报并补回对端地址。
func (c datagramConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, err := c.conn.Read(p)
	return n, c.conn.RemoteAddr(), err
}

// WriteTo 向已连接的对端写一个数据报（远端由连接固定，忽略入参地址）。
func (c datagramConn) WriteTo(p []byte, _ net.Addr) (int, error) { return c.conn.Write(p) }

// Close 关闭底层 socket。
func (c datagramConn) Close() error { return c.conn.Close() }

// LocalAddr 返回本地地址。
func (c datagramConn) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// SetDeadline 设置读写截止时间。
func (c datagramConn) SetDeadline(t time.Time) error { return c.conn.SetDeadline(t) }

// SetReadDeadline 设置读截止时间。
func (c datagramConn) SetReadDeadline(t time.Time) error { return c.conn.SetReadDeadline(t) }

// SetWriteDeadline 设置写截止时间。
func (c datagramConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

// flowPacketConn 是 KCP 会话看到的 PacketConn：写侧加 flow-id 前缀、读侧剥前缀；
// 前缀失配/过短的包按接入层语义直接丢弃（不视为会话故障）。
type flowPacketConn struct {
	net.PacketConn
	flowID uint64
	rbuf   []byte // 读侧暂存（KCP 单读 goroutine 使用）
	wbuf   []byte // 写侧暂存（KCP 单写 goroutine 使用）
}

// ReadFrom 读一个数据报，剥掉本流前缀后交给 KCP。
func (c *flowPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	if len(c.rbuf) < len(p)+edge.FlowIDLen {
		c.rbuf = make([]byte, len(p)+edge.FlowIDLen)
	}
	for {
		n, addr, err := c.PacketConn.ReadFrom(c.rbuf)
		if err != nil {
			return 0, addr, err
		}
		if n < edge.FlowIDLen || binary.BigEndian.Uint64(c.rbuf[:edge.FlowIDLen]) != c.flowID {
			continue
		}
		return copy(p, c.rbuf[edge.FlowIDLen:n]), addr, nil
	}
}

// WriteTo 给发出的数据报加 flow-id 前缀后写出。
func (c *flowPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if cap(c.wbuf) < edge.FlowIDLen+len(p) {
		c.wbuf = make([]byte, edge.FlowIDLen+len(p))
	}
	c.wbuf = c.wbuf[:edge.FlowIDLen+len(p)]
	binary.BigEndian.PutUint64(c.wbuf[:edge.FlowIDLen], c.flowID)
	copy(c.wbuf[edge.FlowIDLen:], p)
	if _, err := c.PacketConn.WriteTo(c.wbuf, addr); err != nil {
		return 0, err
	}
	return len(p), nil
}
