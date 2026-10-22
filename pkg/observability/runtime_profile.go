package observability

import "runtime"

// runtimeSetBlockProfileRate 是 runtime.SetBlockProfileRate 的默认实现（可被用例替换）。
func runtimeSetBlockProfileRate(rate int) { runtime.SetBlockProfileRate(rate) }

// runtimeSetMutexProfileFraction 是 runtime.SetMutexProfileFraction 的默认实现（可被用例替换）。
func runtimeSetMutexProfileFraction(fraction int) { runtime.SetMutexProfileFraction(fraction) }
