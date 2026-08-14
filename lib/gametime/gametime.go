// Package gametime 提供游戏逻辑时钟：单调递增的毫秒时间与帧时间。
// 使用 time.Since 相对起点计算，避免系统时钟回拨影响。
package gametime

import (
	"sync/atomic"
	"time"
)

var start = time.Now()

// NowMilli 返回自进程启动以来的毫秒数（单调）。
func NowMilli() int64 {
	return time.Since(start).Milliseconds()
}

// Clock 是可注入的逻辑时钟（测试与确定性回放使用）。
type Clock struct {
	offset atomic.Int64
}

// NewClock 创建逻辑时钟。
func NewClock() *Clock { return &Clock{} }

// Milli 返回当前逻辑毫秒（= 单调毫秒 + 偏移）。
func (c *Clock) Milli() int64 {
	return NowMilli() + c.offset.Load()
}

// Advance 前进逻辑时钟（测试/回放使用）。
func (c *Clock) Advance(d time.Duration) {
	c.offset.Add(d.Milliseconds())
}
