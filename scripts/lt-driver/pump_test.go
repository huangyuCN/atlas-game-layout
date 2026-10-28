package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 本文件守「驱动按 -duration 自动收尾」：到点停止发送并让整轮压测走下去（打印 SUMMARY），
// 不再因为某条连接的帧调用永不返回而挂死。
//
// 现场症状（诊断 §6）：-duration 60s 的轮次在 t=195s 仍停在 0 pps，进程不退出、不打 SUMMARY，
// 只能人工 kill。根因是 pumpAll 等 wg.Wait()，而单条帧调用没有超时——对端半死时 Invoke
// 会一直等下去，一条连接就足以挂住整轮压测。

// hangingFrame 模拟「调用永不返回、且不理会 ctx」的帧连接（最坏形态：只有到点后的
// 强制收尾能救）。
type hangingFrame struct {
	once   sync.Once
	closed chan struct{}
}

// newHangingFrame 构造永不返回的帧连接桩。
func newHangingFrame() *hangingFrame { return &hangingFrame{closed: make(chan struct{})} }

// Invoke 阻塞到 Close（**有意不监听 ctx**：真实卡死时调用同样不会被 ctx 打断）。
func (f *hangingFrame) Invoke(context.Context, string, any, any) error {
	<-f.closed
	return errors.New("hanging frame closed")
}

// OnNotify 空实现（桩不推送）。
func (f *hangingFrame) OnNotify(func(string, []byte)) {}

// Close 释放阻塞中的调用（幂等）。
func (f *hangingFrame) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

// countingFrame 是立即返回成功的帧连接桩（正常收尾用例）。
type countingFrame struct{ calls atomic.Int64 }

// Invoke 立即成功并计数。
func (f *countingFrame) Invoke(context.Context, string, any, any) error {
	f.calls.Add(1)
	return nil
}

// OnNotify 空实现（桩不推送）。
func (f *countingFrame) OnNotify(func(string, []byte)) {}

// Close 空实现。
func (f *countingFrame) Close() error { return nil }

// ctxHonoringFrame 是「只在 ctx 结束后才返回」的帧连接桩（单次调用预算用例）。
type ctxHonoringFrame struct{ failures atomic.Int64 }

// Invoke 等到 ctx 结束再返回错误。
func (f *ctxHonoringFrame) Invoke(ctx context.Context, _ string, _, _ any) error {
	<-ctx.Done()
	f.failures.Add(1)
	return ctx.Err()
}

// OnNotify 空实现（桩不推送）。
func (f *ctxHonoringFrame) OnNotify(func(string, []byte)) {}

// Close 空实现。
func (f *ctxHonoringFrame) Close() error { return nil }

// TestPumpAllReturnsAtDurationWhenCallHangs 是本修复的核心：
// 一条连接的帧调用永不返回时，到点后整轮负载仍必须收尾（返回），好让驱动打印 SUMMARY 并退出。
//
// 旧实现在此红：pumpAll 等 wg.Wait()，卡住的那条连接让整轮永远收不了尾。
func TestPumpAllReturnsAtDurationWhenCallHangs(t *testing.T) {
	p := &loadPlayer{frame: newHangingFrame(), probe: newProbeSet(false)}
	start := time.Now()
	pumpAll(context.Background(), []*loadPlayer{p}, pumpCfg{
		pps: 100, probe: time.Hour, duration: 50 * time.Millisecond, callTimeout: 20 * time.Millisecond,
	})
	spent := time.Since(start)
	if spent > pumpGrace+time.Second {
		t.Fatalf("到点收尾耗时 %v，期望 ≤ duration+调用预算+pumpGrace（不得被卡死的调用挂住）", spent)
	}
}

// TestPumpStopsSendingAtDuration 验证正常形态：到点即停止发送，上行计数不再增长。
func TestPumpStopsSendingAtDuration(t *testing.T) {
	p := &loadPlayer{frame: &countingFrame{}, probe: newProbeSet(false)}
	pumpAll(context.Background(), []*loadPlayer{p}, pumpCfg{pps: 200, probe: time.Hour, duration: 60 * time.Millisecond})
	sent := p.inputs.Load()
	if sent == 0 {
		t.Fatal("稳态窗口内应至少发过一帧输入")
	}
	time.Sleep(50 * time.Millisecond)
	if got := p.inputs.Load(); got != sent {
		t.Fatalf("到点后仍在发送：%d → %d", sent, got)
	}
}

// TestPumpHonorsCallTimeout 验证单次调用预算：对端不回执时调用被预算打断并计入失败，
// 而不是无限等待（到点仍能自然收尾）。
func TestPumpHonorsCallTimeout(t *testing.T) {
	f := &ctxHonoringFrame{}
	p := &loadPlayer{frame: f, probe: newProbeSet(false)}
	start := time.Now()
	pumpAll(context.Background(), []*loadPlayer{p}, pumpCfg{
		pps: 1, probe: 10 * time.Millisecond, duration: 80 * time.Millisecond, callTimeout: 20 * time.Millisecond,
	})
	if spent := time.Since(start); spent > time.Second {
		t.Fatalf("收尾耗时 %v，期望 ≈ duration（单次调用预算应打断不回执的调用）", spent)
	}
	if f.failures.Load() == 0 {
		t.Fatal("被打断的调用应计入帧面失败（SUMMARY 里要能看出来）")
	}
}

// TestPumpAllIgnoresNilFrame 验证未建帧连接的玩家被跳过（不影响收尾）。
func TestPumpAllIgnoresNilFrame(t *testing.T) {
	start := time.Now()
	pumpAll(context.Background(), []*loadPlayer{{}, nil}, pumpCfg{pps: 10, probe: time.Hour, duration: 30 * time.Millisecond})
	if spent := time.Since(start); spent > 500*time.Millisecond {
		t.Fatalf("无帧连接时应立即返回，实际耗时 %v", spent)
	}
}
