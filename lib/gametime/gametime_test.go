package gametime

import (
	"testing"
	"time"
)

// TestNowMilliMonotonic 验证单调递增。
func TestNowMilliMonotonic(t *testing.T) {
	a := NowMilli()
	time.Sleep(5 * time.Millisecond)
	b := NowMilli()
	if b <= a {
		t.Fatalf("NowMilli 不单调: %d -> %d", a, b)
	}
}

// TestClockAdvance 验证逻辑时钟偏移。
func TestClockAdvance(t *testing.T) {
	c := NewClock()
	base := c.Milli()
	c.Advance(100 * time.Millisecond)
	if got := c.Milli(); got != base+100 {
		t.Fatalf("Advance 后 Milli = %d, 期望 %d", got, base+100)
	}
}
