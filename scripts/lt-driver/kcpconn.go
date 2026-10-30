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
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/huangyuCN/atlas/contrib/edge"
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
//
// done 是连接的「停止信号」：Close 先关它，所有阻塞点据此退出。
// 没有它就是现场那条 bug——读协程只看得到会话层的错误，而会话关闭在读侧是
// 可以无限重放的（每次 Read 立刻返回同一个错误），读循环于是永不退出。
type edgeKCPConn struct {
	sess *kcpgo.UDPSession
	udp  *net.UDPConn  // 底层 UDP socket：kcp-go 对调用方传入的 PacketConn 不做 Close，必须由我们关
	done chan struct{} // 停止信号：Close 时先关，读循环凭它无条件退出
	once sync.Once     // 停止信号只广播一次
}

// stopped 报告连接是否已进入关闭流程。
func (c *edgeKCPConn) stopped() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
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
	logKCPParams()
	return &edgeKCPConn{sess: sess, udp: udpConn, done: make(chan struct{})}, nil
}

// applyBattleKCPDefaults 把 battle 侧缺省 KCP 参数应用到会话（缺省值见文件头常量）。
// 压测可经环境变量覆盖（LT_KCP_NODELAY/INTERVAL/RESEND/NC/SNDWND/RCVWND）用于参数扫描：
// KCP 是端到端会话，两端参数必须同档，故驱动必须能跟随 battle 配置切换。
func applyBattleKCPDefaults(sess *kcpgo.UDPSession) {
	sess.SetNoDelay(envInt("LT_KCP_NODELAY", kcpNoDelay), envInt("LT_KCP_INTERVAL", kcpInterval), envInt("LT_KCP_RESEND", kcpResend), envInt("LT_KCP_NC", kcpNC))
	sess.SetWindowSize(envInt("LT_KCP_SNDWND", kcpSndWnd), envInt("LT_KCP_RCVWND", kcpRcvWnd))
	sess.SetMtu(kcpMTU)
	sess.SetACKNoDelay(true)
	sess.SetWriteDelay(false)
}

// envInt 读整数环境变量，缺失或非法时返回缺省值。
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// kcpParamsOnce 保证生效参数只打印一次。
var kcpParamsOnce sync.Once

// logKCPParams 打印本轮生效的 KCP 参数，便于报告标注。
func logKCPParams() {
	kcpParamsOnce.Do(func() {
		fmt.Fprintf(os.Stderr, "kcp-params nodelay=%d interval=%d resend=%d nc=%d sndwnd=%d rcvwnd=%d mtu=%d\n",
			envInt("LT_KCP_NODELAY", kcpNoDelay), envInt("LT_KCP_INTERVAL", kcpInterval), envInt("LT_KCP_RESEND", kcpResend),
			envInt("LT_KCP_NC", kcpNC), envInt("LT_KCP_SNDWND", kcpSndWnd), envInt("LT_KCP_RCVWND", kcpRcvWnd), kcpMTU)
	})
}

// errKCPStopped 表示连接已进入关闭流程（驱动侧停止信号已广播）。
// 它与会话错误同属「读循环必须退出」的终态，绝不能被归成坏帧。
var errKCPStopped = errors.New("lt-driver: kcp 连接已关闭")

// ReadFrame 从 KCP 会话读一个完整帧（帧引擎读取循环调用；字节流语义，帧边界由帧头长度切分）。
//
// 错误分类是**收尾能不能走完**的关键：引擎 readLoop 对 engine.ErrBadFrame 是软跳过
// （continue，不退出循环），对其余错误才 FailAll + Invalidate + 退出。而 KCP 会话在
// 「已关闭 / 读超时」时会**立刻**重放同一个错误——若归成坏帧，读循环就以满核空转、
// 永不退出（现场 900% CPU、上万协程停在 ReadFrame 的直接原因）。故这些错误必须上抛。
func (a edgeKCPAdapter) ReadFrame(c *edgeKCPConn, maxBodySize int) (frame.Header, []byte, error) {
	hdr, body, err := frame.ReadInto(c.sess, maxBodySize)
	if err != nil {
		return frame.Header{}, nil, classifyKCPReadErr(c, err)
	}
	// 关闭竞态：帧已到但连接正在收尾，同样按终态退出（body 归池）。
	if c.stopped() {
		frame.PutBuf(body)
		return frame.Header{}, nil, errKCPStopped
	}
	return hdr, body, nil
}

// classifyKCPReadErr 把一次读错误分成「终态」与「坏帧」两类。
//
// 终态：连接已进入关闭流程，或会话已不可再用（EOF / 对端关闭 / 读超时）——原样上抛，
// 读循环据此退出并让上层重连。
// 坏帧：连接还活着、只是这一条字节流解不出合法帧头——包成 engine.ErrBadFrame 软跳过。
func classifyKCPReadErr(c *edgeKCPConn, err error) error {
	if c.stopped() || kcpReadIsTerminal(err) {
		return err
	}
	return fmt.Errorf("%w: %w", engine.ErrBadFrame, err)
}

// kcpReadIsTerminal 判定读错误是否属于「会话不可再用」的终态。
//
// 三个来源：对端 EOF（io.EOF / io.ErrUnexpectedEOF）、本地关闭（net.ErrClosed /
// io.ErrClosedPipe，kcp-go 在 die 关闭后就是这样返回的）、读超时（引擎的 idle 检测，
// kcp-go 的 timeoutError 实现了 net.Error）。
func kcpReadIsTerminal(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
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

// Close 收尾 KCP 连接，顺序**不可颠倒**：先停读协程，再关会话，最后关底层 socket。
//
//  1. 关 done —— 读循环据此把任何后续错误判为终态，而不再当坏帧继续空转；
//  2. 把读截止时间拨到过去 —— 打断仍阻塞在 sess.Read 里的读协程（KCP 的 Read 不听
//     ctx，只能靠截止时间唤醒）；
//  3. 关会话 —— kcp-go 退掉 die，它自己的会话协程随之退出；
//  4. 关底层 UDP socket —— kcp-go 对调用方传入的 PacketConn 不做 Close（ownConn=false），
//     不关就会留下一个永远阻塞在 Read 上的 socket 读协程（每连接泄漏一个）。
//
// 幂等：重复调用不再重复广播停止信号。
func (a edgeKCPAdapter) Close(c *edgeKCPConn) error {
	if c == nil || c.sess == nil {
		return nil
	}
	c.once.Do(func() {
		close(c.done) // 1. 广播停止信号（之前的阻塞点都在等它）
		_ = c.sess.SetReadDeadline(time.Now())
	})
	_ = c.sess.Close() // 3. 关会话
	if c.udp != nil {
		_ = c.udp.Close() // 4. 关底层 socket
	}
	return nil
}

// RemoteAddr 返回对端地址文本（取不到时回退接入层地址）。
func (a edgeKCPAdapter) RemoteAddr(c *edgeKCPConn) string {
	if c.sess == nil || c.sess.RemoteAddr() == nil {
		return a.addr
	}
	return c.sess.RemoteAddr().String()
}

// dialEdgeKCP 建立一条经接入层 KCP 面的帧连接（票据同时进 hello 段与逐帧会话槽）。
func dialEdgeKCP(ctx context.Context, addr, slot string, ticket []byte, hello helloPolicy) (frameClient, error) {
	codec, err := uplinkCodec()
	if err != nil {
		return nil, err
	}
	cli, err := engine.NewStreamClient[*edgeKCPConn](
		edgeKCPAdapter{addr: addr, ticket: ticket, hello: hello},
		engine.WithClientCodec[*edgeKCPConn](codec),
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

// OnNotifyVersion 挂接按帧头版本分派的推送分发（ver=1 protojson、ver=2 protobuf）。
func (k kcpFrameClient) OnNotifyVersion(fn func(ver uint8, operation string, payload []byte)) {
	k.cli.SetNotifyHandlerVersion(fn)
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
