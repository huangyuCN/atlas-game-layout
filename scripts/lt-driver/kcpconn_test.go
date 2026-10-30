// kcpconn_test.go 覆盖 KCP 收尾的两条硬约束：**关闭后读循环必须能退出**、
// **读循环退出后协程必须回落**。
//
// 现场症状（kcp 4,096 / 2,048 均复现）：负载跑完、无 SUMMARY；驱动 900% CPU、
// 21,170 个 goroutine，其中 13,350 栈停在 KCP ReadFrame。根因是「关闭/读超时」
// 两种**终态**错误被 edgeKCPAdapter.ReadFrame 归成 engine.ErrBadFrame，而引擎的
// readLoop 对坏帧是 `continue`（软跳过、不退出）——于是一条被关掉的会话上，
// 每一次 Read 都立刻返回同一个错误，读协程以满核空转、永不退出。
//
// 本文件的断言只落在「适配器对引擎暴露的错误分类」这一条接缝上，不碰引擎内部。
package main

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// testKCPFlowID 是假接入层回给测试连接的 flow-id（取值任意，只要前后一致）。
const testKCPFlowID = 0x1122334455667788

// newTestKCPConn 起一条「只握到 hello、没有 KCP 对端」的连接：这是暴露
// 「关闭时不停止读循环」的最小载体——会话不会自己死，只能由 Close 收走。
func newTestKCPConn(t *testing.T) (edgeKCPAdapter, *edgeKCPConn) {
	t.Helper()
	fe := newFakeHelloEdge(t, 0, testKCPFlowID)
	a := edgeKCPAdapter{
		addr: fe.addr(), ticket: []byte("ticket"),
		hello: helloPolicy{timeout: time.Second, attempts: 1},
	}
	c, err := a.Dial(context.Background())
	if err != nil {
		t.Fatalf("建立测试 KCP 连接失败: %v", err)
	}
	t.Cleanup(func() { _ = a.Close(c) })
	return a, c
}

// kcpTerminalRead 读一次并断言「引擎据此能退出读循环」（即不能是坏帧）。
func kcpTerminalRead(t *testing.T, a edgeKCPAdapter, c *edgeKCPConn, scene string) error {
	t.Helper()
	_, _, err := a.ReadFrame(c, 4096)
	if err == nil {
		t.Fatalf("%s：仍读到了帧，期望终止性错误", scene)
	}
	if errors.Is(err, engine.ErrBadFrame) {
		t.Fatalf("%s 被归成 engine.ErrBadFrame（引擎 readLoop 对坏帧 continue 死转，永不退出）: %v", scene, err)
	}
	return err
}

// TestKCPReadFrame_会话关闭后必须是可终止错误 覆盖「关闭路径」：
// kcp-go 在会话 Close 之后让 Read 返回 io.ErrClosedPipe；旧实现把它包成
// engine.ErrBadFrame，读循环于是满核空转——这是收尾卡死、无 SUMMARY 的直接原因。
func TestKCPReadFrame_会话关闭后必须是可终止错误(t *testing.T) {
	a, c := newTestKCPConn(t)
	if err := a.Close(c); err != nil {
		t.Fatalf("关闭 KCP 会话失败: %v", err)
	}
	kcpTerminalRead(t, a, c, "会话已关闭后的读")
}

// TestKCPReadFrame_读超时必须是可终止错误 覆盖「idle 超时」：引擎用
// SetReadDeadline 做 idle 检测，kcp-go 的 timeoutError 实现了 net.Error
// 但既不是 io.EOF 也不是 net.ErrClosed，旧实现同样归成坏帧 ⇒ 一条 idle 连接
// 立刻变成满核空转的读循环（这也是负载期 900% CPU 的一个来源）。
func TestKCPReadFrame_读超时必须是可终止错误(t *testing.T) {
	a, c := newTestKCPConn(t)
	if err := a.SetReadDeadline(c, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("设置读截止时间失败: %v", err)
	}
	kcpTerminalRead(t, a, c, "读超时")
}

// fakeTimeoutErr 模拟 kcp-go 的读超时错误：实现了 net.Error 且 Timeout() 为真。
type fakeTimeoutErr struct{}

// Error 实现 error。
func (fakeTimeoutErr) Error() string { return "timeout" }

// Timeout 报告这是一次超时（net.Error 语义）。
func (fakeTimeoutErr) Timeout() bool { return true }

// Temporary 报告这是一次可重试的暂时性错误（net.Error 语义）。
func (fakeTimeoutErr) Temporary() bool { return true }

// TestKCPReadErr_分类边界 锁定 classifier 的分界：终态必须原样上抛，协议垃圾必须
// 仍按坏帧软跳过——这条反向约束防止「修复空转」时把坏帧软跳过语义一并削掉。
func TestKCPReadErr_分类边界(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		terminal bool
	}{
		{"对端 EOF", io.EOF, true},
		{"载荷中途 EOF", io.ErrUnexpectedEOF, true},
		{"本地关闭会话", io.ErrClosedPipe, true},
		{"socket 已关闭", net.ErrClosed, true},
		{"读超时（kcp-go timeoutError 形态）", fakeTimeoutErr{}, true},
		{"协议垃圾（magic 错）", errors.New("frame: bad magic"), false},
		{"长度域超界", errors.New("frame: body too large"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := kcpReadIsTerminal(tc.err); got != tc.terminal {
				t.Fatalf("kcpReadIsTerminal(%v) = %v，期望 %v", tc.err, got, tc.terminal)
			}
			// 连接仍活着（done 未关）时的最终归类。
			live := &edgeKCPConn{done: make(chan struct{})}
			got := classifyKCPReadErr(live, tc.err)
			isBad := errors.Is(got, engine.ErrBadFrame)
			if isBad == tc.terminal {
				t.Fatalf("classifyKCPReadErr(%v) 坏帧=%v，期望坏帧=%v", tc.err, isBad, !tc.terminal)
			}
		})
	}
}

// TestKCPReadErr_关闭信号优先 覆盖「done 一旦关闭，任何错误都必须算终态」：
// 关闭流程进行中即便底层报的是可恢复错误，也不能让读循环继续转。
func TestKCPReadErr_关闭信号优先(t *testing.T) {
	conn := &edgeKCPConn{done: make(chan struct{})}
	close(conn.done)
	if got := classifyKCPReadErr(conn, errors.New("frame: bad magic")); errors.Is(got, engine.ErrBadFrame) {
		t.Fatalf("连接已进入关闭流程时仍把错误归成坏帧: %v", got)
	}
}

// TestKCPClient_关闭后读协程必须回落 覆盖「协程生命周期」：客户端 Close 之后，
// 引擎读循环与 kcp-go 自身的会话协程都必须退出，协程数回到基线附近。
// 旧实现（关闭错误被当坏帧）会让读循环永远空转 ⇒ 协程只增不减。
func TestKCPClient_关闭后读协程必须回落(t *testing.T) {
	fe := newFakeHelloEdge(t, 0, testKCPFlowID)
	codec, err := uplinkCodec()
	if err != nil {
		t.Fatalf("取上行编解码器失败: %v", err)
	}
	base := runtime.NumGoroutine()
	cli, err := engine.NewStreamClient[*edgeKCPConn](
		edgeKCPAdapter{
			addr: fe.addr(), ticket: []byte("ticket"),
			hello: helloPolicy{timeout: time.Second, attempts: 1},
		},
		engine.WithClientCodec[*edgeKCPConn](codec),
		engine.WithClientIdleTimeout[*edgeKCPConn](30*time.Second),
	)
	if err != nil {
		t.Fatalf("构造帧客户端失败: %v", err)
	}
	// 只启动读循环（Ensure 触发 Dial + readLoop）；不发请求，避免依赖对端回执。
	if _, err := cli.ConnManager().Ensure(context.Background()); err != nil {
		t.Fatalf("启动读循环失败: %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("关闭帧客户端失败: %v", err)
	}
	// 只允许 1 个残余（调度抖动）；旧实现的引擎读循环空转 + kcp-go socket 读协程
	// 卡在原地，会稳定多出 2 个。
	waitGoroutines(t, base+1, 3*time.Second)
}

// waitGoroutines 等到协程数回落到 want 以内；超时即判失败（打印当前数量）。
func waitGoroutines(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := runtime.NumGoroutine()
		if got <= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("关闭后协程未回落：期望 ≤%d，实际 %d（读循环仍在空转）", want, got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// 让编译器确认测试里用到的哨兵确实来自 kcp-go 的错误面（文档作用，不参与运行）。
var (
	_ = io.ErrClosedPipe
	_ net.Error
)
