package observability

import "fmt"

// 可注入的钩子：便于用例断言"是否调用、按什么值调用"。
// 全局运行时采样状态无法读回，直接断言 runtime 状态不可行。
var (
	setBlockProfileRate     = runtimeSetBlockProfileRate
	setMutexProfileFraction = runtimeSetMutexProfileFraction
)

// InitProfiling 按配置开启运行时的阻塞与互斥采样（进程级，只应在装配期调用一次）。
//
// 两个参数为 0 表示关闭对应采样：此时**不调用**运行时设置函数（默认关闭、零开销）。
// 负数属于配置错误，返回错误交由装配期失败（不静默降级）。
func InitProfiling(blockProfileRate, mutexProfileFraction int) error {
	if blockProfileRate < 0 {
		return fmt.Errorf("observability.block_profile_rate 不得为负：%d", blockProfileRate)
	}
	if mutexProfileFraction < 0 {
		return fmt.Errorf("observability.mutex_profile_fraction 不得为负：%d", mutexProfileFraction)
	}
	if blockProfileRate > 0 {
		setBlockProfileRate(blockProfileRate)
	}
	if mutexProfileFraction > 0 {
		setMutexProfileFraction(mutexProfileFraction)
	}
	return nil
}
