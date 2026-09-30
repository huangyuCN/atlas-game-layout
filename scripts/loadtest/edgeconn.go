// edgeconn.go 提供「经接入层 UDP 面」的帧连接载体：用框架 `contrib/edge` 的 hello/flow-id
// 原语完成握手与逐包前缀，再把连接交给框架帧引擎客户端（`transport/frame/engine`）。
//
// 为什么需要这一层：框架的 `transport/udp` 客户端直接对着 battle 帧端口说话（没有接入层
// 握手段），而经接入层的数据报面有两条硬约定（规格 §2.1/§3.2）：首包是 hello 段（换 flow-id），
// 此后**双向每个数据报**都带 8 字节 flow-id 前缀。接入层不解析帧正文，故前缀由本载体加/剥。
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/encoding"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// edgeUDPConn 是一条经接入层 UDP 面的连接状态（socket 已换到 flow-id）。
type edgeUDPConn struct {
	conn    *net.UDPConn
	flowID  uint64
	readBuf []byte
	// writeBuf 是发送缓冲：帧编码 + 前缀拼接都在它上面完成，避免每帧两次分配。
	// 帧引擎对同一连接的写是串行的（内部 writeMu），故此缓冲无需额外加锁。
	writeBuf []byte
}

// edgeUDPAdapter 实现 engine.ClientAdapter[*edgeUDPConn]：拨号即完成接入层 hello 握手。
type edgeUDPAdapter struct {
	addr   string        // 接入层 UDP 面地址（host:port）
	ticket []byte        // 票据密文（hello 段载荷）
	hello  time.Duration // hello 握手读超时
}

// Dial 拨接入层 UDP 面并完成 hello 握手，返回已带 flow-id 的连接。
func (a edgeUDPAdapter) Dial(ctx context.Context) (*edgeUDPConn, error) {
	raddr, err := net.ResolveUDPAddr("udp", a.addr)
	if err != nil {
		return nil, fmt.Errorf("解析接入层地址 %s 失败: %w", a.addr, err)
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, fmt.Errorf("拨接入层 %s 失败: %w", a.addr, err)
	}
	if err := ctx.Err(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	flowID, err := udpHello(conn, a.ticket, a.hello)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &edgeUDPConn{
		conn:     conn,
		flowID:   flowID,
		readBuf:  make([]byte, 64*1024),
		writeBuf: make([]byte, 0, 64*1024),
	}, nil
}

// udpHello 在同一 socket 上发 hello 段并读回 flow-id（接入层不回执、只回 8 字节）。
func udpHello(conn *net.UDPConn, ticket []byte, timeout time.Duration) (uint64, error) {
	if _, err := conn.Write(edge.EncodeHello(ticket)); err != nil {
		return 0, fmt.Errorf("写 hello 段失败: %w", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return 0, err
	}
	buf := make([]byte, edge.FlowIDLen)
	if _, err := readFull(conn, buf); err != nil {
		return 0, fmt.Errorf("读 flow-id 失败（接入层可能已拒绝该票）: %w", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf), nil
}

// ReadFrame 读一个数据报、剥掉 flow-id 前缀，再按「一个数据报一条帧」解码。
func (a edgeUDPAdapter) ReadFrame(c *edgeUDPConn, maxBodySize int) (frame.Header, []byte, error) {
	n, err := c.conn.Read(c.readBuf)
	if err != nil {
		return frame.Header{}, nil, err
	}
	_, payload, err := edge.DecodeFlowID(c.readBuf[:n])
	if err != nil {
		return frame.Header{}, nil, fmt.Errorf("%w: %w", engine.ErrBadFrame, err)
	}
	hdr, body, err := frame.DecodeMessage(payload, maxBodySize)
	if err != nil {
		return frame.Header{}, nil, fmt.Errorf("%w: %w", engine.ErrBadFrame, err)
	}
	if len(body) == 0 {
		return hdr, nil, nil
	}
	// body 是 readBuf 的子切片（会被下一次读覆盖），拷贝进帧池以匹配引擎的 PutBuf 语义。
	out := frame.GetBuf(len(body))
	copy(out, body)
	return hdr, out, nil
}

// WriteFrame 编码帧并在前面拼 flow-id 前缀后发出。
func (a edgeUDPAdapter) WriteFrame(c *edgeUDPConn, h frame.Header, body []byte, maxBodySize int) error {
	msg, err := frame.Encode(h, body, maxBodySize)
	if err != nil {
		return err
	}
	defer frame.PutBuf(msg)
	c.writeBuf = c.writeBuf[:0]
	c.writeBuf = binary.BigEndian.AppendUint64(c.writeBuf, c.flowID)
	c.writeBuf = append(c.writeBuf, msg...)
	_, err = c.conn.Write(c.writeBuf)
	return err
}

// SetReadDeadline 设置读截止时间（帧引擎的 idle 超时用）。
func (a edgeUDPAdapter) SetReadDeadline(c *edgeUDPConn, t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline 设置写截止时间。
func (a edgeUDPAdapter) SetWriteDeadline(c *edgeUDPConn, t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// Close 关闭底层 socket。
func (a edgeUDPAdapter) Close(c *edgeUDPConn) error { return c.conn.Close() }

// RemoteAddr 返回对端地址文本（取不到时回退接入层地址）。
func (a edgeUDPAdapter) RemoteAddr(c *edgeUDPConn) string {
	if c.conn == nil || c.conn.RemoteAddr() == nil {
		return a.addr
	}
	return c.conn.RemoteAddr().String()
}

// dialEdgeUDP 建立一条经接入层 UDP 面的帧连接（票据同时进 hello 段与逐帧会话槽）。
func dialEdgeUDP(ctx context.Context, addr, slot string, ticket []byte) (frameClient, error) {
	cli, err := engine.NewStreamClient[*edgeUDPConn](
		edgeUDPAdapter{addr: addr, ticket: ticket, hello: 3 * time.Second},
		engine.WithClientCodec[*edgeUDPConn](encoding.GetCodec("json")),
		engine.WithClientKind[*edgeUDPConn](transport.KindUDP),
		engine.WithClientSessionProvider[*edgeUDPConn](func() string { return slot }),
		engine.WithClientIdleTimeout[*edgeUDPConn](30*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("构造接入层 UDP 帧客户端失败: %w", err)
	}
	return engineFrameClient{cli: cli}, nil
}

// engineFrameClient 把框架帧引擎客户端适配为 frameClient（UDP 面经接入层的载体）。
type engineFrameClient struct {
	cli *engine.StreamClient[*edgeUDPConn]
}

// Invoke 发起一次帧 op（走框架帧引擎的一元调用语义）。
func (e engineFrameClient) Invoke(ctx context.Context, operation string, req, resp any) error {
	return e.cli.Invoke(ctx, operation, req, resp)
}

// OnNotify 挂接服务端推送分发（帧广播/结算通知都从这里到达）。
func (e engineFrameClient) OnNotify(fn func(operation string, payload []byte)) {
	e.cli.SetNotifyHandler(fn)
}

// Close 关闭连接（引擎会读循环退出并释放资源）。
func (e engineFrameClient) Close() error { return e.cli.Close() }

// readFull 读满 len(buf) 字节（io.ReadFull 的本地实现，避免为一处引入 io 依赖）。
func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
