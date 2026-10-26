package main

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// 本文件覆盖数据报面 hello 的「暂不可用」快速回执处理（框架诊断 §6 修法 2 的驱动侧）：
// 接入层在解析/拨号失败时回 1 个字节 0x00，客户端必须**立刻**按百毫秒级策略重试，
// 而不是把它当成 flow-id 的前半段继续等满读超时（旧实现的行为）。

// fakeHelloEdge 是只应答 hello 的假接入层：前 unavailable 次 hello 回「暂不可用」回执，
// 之后回 8 字节 flow-id，并记录每次 hello 到达时刻（断言重试间隔用）。
type fakeHelloEdge struct {
	conn        *net.UDPConn
	arrived     chan time.Time
	unavailable int
	flowID      uint64
}

// newFakeHelloEdge 起一个假接入层：unavailable 次回执后改用 flowID 应答。
func newFakeHelloEdge(t *testing.T, unavailable int, flowID uint64) *fakeHelloEdge {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("起假接入层失败: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	fe := &fakeHelloEdge{conn: conn, arrived: make(chan time.Time, 8), unavailable: unavailable, flowID: flowID}
	go fe.serve()
	return fe
}

// addr 返回假接入层地址。
func (f *fakeHelloEdge) addr() string { return f.conn.LocalAddr().String() }

// serve 逐包应答 hello：前 unavailable 次回 1 字节回执，之后回 8 字节 flow-id。
func (f *fakeHelloEdge) serve() {
	buf := make([]byte, 2048)
	for i := 1; ; i++ {
		n, from, err := f.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n == 0 {
			continue
		}
		f.arrived <- time.Now()
		var reply []byte
		if i <= f.unavailable {
			reply = []byte{0x00} // 「暂不可用」回执
		} else {
			reply = make([]byte, 8)
			binary.BigEndian.PutUint64(reply, f.flowID)
		}
		if _, err := f.conn.WriteToUDP(reply, from); err != nil {
			return
		}
	}
}

// nextArrival 等下一次 hello 到达，超时即测试失败。
func (f *fakeHelloEdge) nextArrival(t *testing.T, timeout time.Duration) time.Time {
	t.Helper()
	select {
	case at := <-f.arrived:
		return at
	case <-time.After(timeout):
		t.Fatal("等待假接入层收到 hello 超时")
		return time.Time{}
	}
}

// TestDialHello_UnavailableReplyRetriesFast 覆盖「收到回执后 100–200ms 内重试」：
// 单次读超时给到 3s，旧实现只能等满它才重试，回执路径必须把这个间隔压到百毫秒级。
func TestDialHello_UnavailableReplyRetriesFast(t *testing.T) {
	const wantFlowID = 0x0102030405060708
	fe := newFakeHelloEdge(t, 2, wantFlowID) // 前两次回执，第三次成功

	conn, flowID, err := dialHello(context.Background(), fe.addr(), []byte("ticket"), helloPolicy{
		timeout: 3 * time.Second, attempts: 3,
	})
	if err != nil {
		t.Fatalf("收到回执后应重试并最终成功，实际 %v", err)
	}
	defer conn.Close()
	if flowID != wantFlowID {
		t.Fatalf("flow-id = %#x，期望 %#x", flowID, wantFlowID)
	}
	first := fe.nextArrival(t, time.Second)
	for i := 2; i <= 3; i++ {
		next := fe.nextArrival(t, time.Second)
		if gap := next.Sub(first); gap < 100*time.Millisecond || gap > 200*time.Millisecond {
			t.Fatalf("第 %d 次 hello 距上次 %v，期望收到回执后 100–200ms 内重试（旧实现会等满 3s 超时）", i, gap)
		}
		first = next
	}
}

// TestDialHello_UnavailableReplyHonorsAttempts 覆盖重试上限可配：一直回执时按 attempts 收手，
// 不无限重试（每多一轮重试就是一次握手套接）。
func TestDialHello_UnavailableReplyHonorsAttempts(t *testing.T) {
	fe := newFakeHelloEdge(t, 100, 1) // 永远回执

	done := make(chan struct{})
	var err error
	go func() {
		_, _, err = dialHello(context.Background(), fe.addr(), []byte("ticket"), helloPolicy{
			timeout: 2 * time.Second, attempts: 2,
		})
		close(done)
	}()
	for i := 0; i < 2; i++ {
		fe.nextArrival(t, time.Second)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("重试上限未生效：dialHello 在一直收到回执时没有按 attempts 收手")
	}
	if err == nil {
		t.Fatal("一直收到回执时应返回错误")
	}
	// 上限之后不得再来第四次 hello（多试一次就是白等一轮握手）。
	select {
	case <-fe.arrived:
		t.Fatalf("尝试次数应为 2，实际发出了更多的 hello（多余到达: %v）", time.Now())
	case <-time.After(250 * time.Millisecond):
	}
}
