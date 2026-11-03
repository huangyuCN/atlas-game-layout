package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	// pinReadyBudget 是「等 picker 候选集全部就绪」的预算（本地回环应在毫秒级就绪）。
	pinReadyBudget = 2 * time.Second
	// pinReadyPoll 是就绪探测的轮询间隔。
	pinReadyPoll = 10 * time.Millisecond
)

// waitPickable 在预算内按固定间隔调用 probe，直到 probe 返回 nil（就绪）或预算耗尽。
//
// 为什么需要它：gRPC 的 ClientConn 进入 READY 只代表**第一个** SubConn 就绪，此刻
// balancer 的 picker 可能只装了候选集的一部分；此时发起的调用会被兜底改道。故拨号返回后
// 必须先把「候选集是否已齐」探明，再断言选点行为（见 pin_e2e_test.go）。
//
// 返回值：成功为 nil；预算耗尽时返回带尝试次数与最后一次探测错误的错误，便于区分
// 「候选迟迟不就绪」与「拨号根本不通」。
func waitPickable(budget, interval time.Duration, probe func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	var lastErr error
	attempts := 0
	for {
		if err := ctx.Err(); err != nil {
			if lastErr == nil {
				lastErr = err
			}
			return fmt.Errorf("就绪探测超时（%s 内尝试 %d 次）: %w", budget, attempts, lastErr)
		}
		attempts++
		if lastErr = probe(ctx); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
}

// TestWaitPickableRetriesUntilReady 验证就绪探测会重试到成功：
// 前两次「候选集只见到 1/2 台」、第三次齐了就返回 nil（没有这道闸时首帧即断言 ⇒ 偶发红）。
func TestWaitPickableRetriesUntilReady(t *testing.T) {
	calls := 0
	err := waitPickable(2*time.Second, time.Millisecond, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("候选集只见到 1/2 台")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("应在第 3 次探测后成功，实际返回 %v", err)
	}
	if calls != 3 {
		t.Fatalf("应恰好探测 3 次，实际 %d 次", calls)
	}
}

// TestWaitPickableReportsLastError 验证预算耗尽时错误带上最后一次探测错误（排障要看到底层原因）。
func TestWaitPickableReportsLastError(t *testing.T) {
	err := waitPickable(30*time.Millisecond, 5*time.Millisecond, func(context.Context) error {
		return errors.New("connection refused: 127.0.0.1:9001")
	})
	if err == nil {
		t.Fatal("预算耗尽应返回错误，实际 nil")
	}
	if got := err.Error(); !strings.Contains(got, "connection refused: 127.0.0.1:9001") {
		t.Fatalf("错误未带最后一次探测原因: %q", got)
	}
}
