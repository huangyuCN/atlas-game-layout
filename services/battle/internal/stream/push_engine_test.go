package stream

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/encoding"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"github.com/huangyuCN/atlas/transport/frame/engine"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// benchPlayers 是推送基准的广播目标数（与「一次广播（10 目标）」的口径一致）。
const benchPlayers = 10

// benchInputs 是基准帧携带的输入条数（每帧带全体参战玩家的输入）。
const benchInputs = 10

// sinkConn 是推送路径测试/基准用的连接句柄（只用于占据帧引擎的连接表，不承载真实 I/O）。
type sinkConn struct {
	// ID 是测试内的连接序号（仅用于区分连接对象）。
	ID int
}

// sinkAdapter 是帧引擎的测试用适配器：写帧只累计帧数与字节数（不落盘、不占缓冲），
// 读帧一直阻塞到 stop 关闭（连接因此在测试/基准期间保持登记状态）。
type sinkAdapter struct {
	stop chan struct{}

	mu     sync.Mutex
	frames int
	bytes  int
	capt   []writtenFrame // 非 nil 时捕获每次写出的帧头版本与帧体副本（wire 断言用）
}

// writtenFrame 是一次写出的观测记录（帧头载荷编码版本 + 帧体副本）。
type writtenFrame struct {
	version uint8
	body    []byte
}

// captureWrites 开启写帧捕获（基准/计数用例不调用：逐帧拷贝会污染分配口径）。
func (a *sinkAdapter) captureWrites() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.capt = make([]writtenFrame, 0, 8)
}

// captured 返回已捕获的写出记录（未开启捕获时返回 nil）。
func (a *sinkAdapter) captured() []writtenFrame {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]writtenFrame(nil), a.capt...)
}

// ReadFrame 阻塞到 stop 关闭后返回 io.EOF（测试/基准期间连接一直存活）。
func (a *sinkAdapter) ReadFrame(*sinkConn, int) (frame.Header, []byte, error) {
	<-a.stop
	return frame.Header{}, nil, io.EOF
}

// WriteFrame 记录一次写出（帧数 + 字节数）后丢弃：写是必需的 syscall，这里只度量引擎侧开销。
// 开启捕获时另存帧头版本与帧体副本（wire 断言用）。
func (a *sinkAdapter) WriteFrame(_ *sinkConn, h frame.Header, body []byte, _ int) error {
	a.mu.Lock()
	a.frames++
	a.bytes += len(body)
	if a.capt != nil {
		a.capt = append(a.capt, writtenFrame{version: h.Version, body: append([]byte(nil), body...)})
	}
	a.mu.Unlock()
	return nil
}

// SetReadDeadline 空实现（测试/基准不设读超时）。
func (a *sinkAdapter) SetReadDeadline(*sinkConn, time.Time) error { return nil }

// SetWriteDeadline 空实现（测试/基准不设写超时）。
func (a *sinkAdapter) SetWriteDeadline(*sinkConn, time.Time) error { return nil }

// Close 空实现（连接由 stop 统一释放）。
func (a *sinkAdapter) Close(*sinkConn) error { return nil }

// RemoteAddr 返回固定的对端地址（测试/基准无真实网络）。
func (a *sinkAdapter) RemoteAddr(c *sinkConn) string { return "sink-" + strconv.Itoa(c.ID) }

// written 返回已写出的帧数与字节数（断言「每个目标各写一帧」用）。
func (a *sinkAdapter) written() (frames, bytes int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.frames, a.bytes
}

// countingCodec 是**计数**编解码器：把帧引擎真实发生的序列化次数记到计数器上，
// 编码结果与默认 json codec（protojson）逐字节一致。
type countingCodec struct {
	marshals atomic.Int64
}

// Marshal 记一次序列化后委托默认 json codec（不改线格式）。
func (c *countingCodec) Marshal(v any) ([]byte, error) {
	c.marshals.Add(1)
	return encodeJSON(v)
}

// Unmarshal 委托默认 json codec。
func (c *countingCodec) Unmarshal(data []byte, v any) error {
	codec := encoding.GetCodec("json")
	if codec == nil {
		return errCodecMissing
	}
	return codec.Unmarshal(data, v)
}

// Name 返回编解码器名（与默认 json codec 同名：语义上它就是 json/protojson）。
func (c *countingCodec) Name() string { return "json" }

// newEnginePort 起一台帧引擎（真实推送路径：编码 + 帧体拼装 + 写出）并返回其直连端口。
// n 条连接经 ServeConn 登记进连接表，连接 ID 由上线事件取得（与生产同一口径）；
// codec 非空时作为引擎编解码器（用于计数），否则用引擎默认。返回值含适配器，
// 供调用方断言写出次数（每个目标各写一帧）。
func newEnginePort(tb testing.TB, n int, codec encoding.Codec) (Port, []uint64, *sinkAdapter) {
	tb.Helper()
	adapter := &sinkAdapter{stop: make(chan struct{})}
	ids := make(chan uint64, n)
	opts := []engine.Option[*sinkConn]{
		engine.WithKind[*sinkConn](transport.KindKCP),
		engine.WithConnLifecycle[*sinkConn](func(ev engine.ConnEvent) {
			if ev.Kind != engine.ConnEventConnected {
				return
			}
			id, err := strconv.ParseUint(ev.ID, 10, 64)
			if err != nil {
				return
			}
			ids <- id
		}),
	}
	if codec != nil {
		opts = append(opts, engine.WithCodec[*sinkConn](codec))
	}
	eng, err := engine.NewStreamEngine[*sinkConn](adapter, opts...)
	if err != nil {
		tb.Fatalf("NewStreamEngine: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eng.ServeConn(context.Background(), &sinkConn{ID: i})
		}(i)
	}
	connIDs := make([]uint64, 0, n)
	for len(connIDs) < n {
		select {
		case id := <-ids:
			connIDs = append(connIDs, id)
		case <-time.After(5 * time.Second):
			tb.Fatalf("等待连接上线超时（已上线 %d/%d）", len(connIDs), n)
		}
	}
	tb.Cleanup(func() {
		close(adapter.stop)
		wg.Wait()
	})
	return ConnPort{Srv: eng}, connIDs, adapter
}

// pushTestFrame 构造一帧真实的广播载荷（带时间戳、快照元信息与全体玩家的输入）。
func pushTestFrame(frameID uint64) *locksteppb.LockstepFrame {
	frame := &locksteppb.LockstepFrame{
		FrameId:    frameID,
		ServerTime: timestamppb.New(time.Unix(1700000000, 0)),
		Snapshot:   &locksteppb.SnapshotMeta{FrameId: frameID, StateHash: make([]byte, 8), SizeBytes: 512},
	}
	for i := 0; i < benchInputs; i++ {
		frame.Inputs = append(frame.Inputs, &locksteppb.LockstepInput{
			FrameId:  frameID,
			PlayerId: fmt.Sprintf("p-%d", i),
			Payload:  make([]byte, 32),
		})
	}
	return frame
}
