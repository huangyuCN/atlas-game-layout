package observability

import "testing"

// TestInitProfilingDefaultsOff 断言：两个参数为 0 时不触碰运行时（默认关闭）。
func TestInitProfilingDefaultsOff(t *testing.T) {
	calls := 0
	restore := swapProfilingHooks(func(int) { calls++ }, func(int) { calls++ })
	defer restore()

	if err := InitProfiling(0, 0); err != nil {
		t.Fatalf("InitProfiling(0,0) 错误 = %v", err)
	}
	if calls != 0 {
		t.Fatalf("关闭态不应调用运行时设置函数，实际调用 %d 次", calls)
	}
}

// TestInitProfilingAppliesValues 断言：配置了就按值调用（各一次）。
func TestInitProfilingAppliesValues(t *testing.T) {
	var block, mutex int
	restore := swapProfilingHooks(func(v int) { block = v }, func(v int) { mutex = v })
	defer restore()

	if err := InitProfiling(1, 5); err != nil {
		t.Fatalf("InitProfiling(1,5) 错误 = %v", err)
	}
	if block != 1 || mutex != 5 {
		t.Fatalf("采样参数未按值下传：block=%d mutex=%d", block, mutex)
	}
}

// TestInitProfilingRejectsNegative 断言：负数属于配置错误，且不调用运行时。
func TestInitProfilingRejectsNegative(t *testing.T) {
	calls := 0
	restore := swapProfilingHooks(func(int) { calls++ }, func(int) { calls++ })
	defer restore()

	if err := InitProfiling(-1, 0); err == nil {
		t.Fatal("负的 block_profile_rate 应报错")
	}
	if err := InitProfiling(0, -1); err == nil {
		t.Fatal("负的 mutex_profile_fraction 应报错")
	}
	if calls != 0 {
		t.Fatalf("非法配置不应调用运行时设置函数，实际调用 %d 次", calls)
	}
}

// swapProfilingHooks 替换两个钩子并返回还原函数（用例结束后恢复，避免相互污染）。
func swapProfilingHooks(block, mutex func(int)) func() {
	oldBlock, oldMutex := setBlockProfileRate, setMutexProfileFraction
	setBlockProfileRate, setMutexProfileFraction = block, mutex
	return func() {
		setBlockProfileRate, setMutexProfileFraction = oldBlock, oldMutex
	}
}
