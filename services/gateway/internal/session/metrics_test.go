package session

import (
	"context"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/internal/metricstest"
)

// TestSessionMetricsBindUnbind 验证绑定/解绑驱动绑定计数与本地会话数 gauge。
func TestSessionMetricsBindUnbind(t *testing.T) {
	ctx := context.Background()
	rec := metricstest.New()
	m := newTestManager(t, "gw-1")
	m.SetMeter(rec)

	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "t1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if got := rec.CounterValue(MetricSessionBinds, "channel", string(ChannelBiz)); got != 1 {
		t.Fatalf("biz 绑定计数 = %v, 期望 1", got)
	}
	if got, ok := rec.GaugeValue(MetricSessions); !ok || got != 1 {
		t.Fatalf("sessions gauge = %v,%v, 期望 1,true", got, ok)
	}

	// 同一玩家绑定战斗通道：会话数不变（多通道聚合），战斗通道绑定计数 +1。
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 2, kind: "kcp"}).conn(), ChannelBattle, "t1"); err != nil {
		t.Fatalf("Bind battle: %v", err)
	}
	if got := rec.CounterValue(MetricSessionBinds, "channel", string(ChannelBattle)); got != 1 {
		t.Fatalf("battle 绑定计数 = %v, 期望 1", got)
	}
	if got, _ := rec.GaugeValue(MetricSessions); got != 1 {
		t.Fatalf("二次绑定后 sessions gauge = %v, 期望 1", got)
	}

	// 解绑后本地会话归零。
	m.Unbind(ctx, "p-1", 2)
	if got, _ := rec.GaugeValue(MetricSessions); got != 0 {
		t.Fatalf("解绑后 sessions gauge = %v, 期望 0", got)
	}
}

// TestSessionMetricsSweep 验证过期清扫驱动清扫计数、会话数归零，
// 并同步清理连接与凭据反向索引（防过期凭据继续解析身份）。
func TestSessionMetricsSweep(t *testing.T) {
	ctx := context.Background()
	rec := metricstest.New()
	m := newTestManagerWithTTL(t, "gw-1", 20*time.Millisecond)
	m.SetMeter(rec)

	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).connWithRef("conn:1"), ChannelBiz, "t1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, ok := m.PlayerByToken("t1"); !ok {
		t.Fatal("绑定后凭据索引应命中")
	}
	time.Sleep(40 * time.Millisecond)

	if removed := m.SweepOnce(ctx); removed != 1 {
		t.Fatalf("SweepOnce = %d, 期望 1", removed)
	}
	if got := rec.CounterValue(MetricSessionSweeps); got != 1 {
		t.Fatalf("清扫计数 = %v, 期望 1", got)
	}
	if got, _ := rec.GaugeValue(MetricSessions); got != 0 {
		t.Fatalf("清扫后 sessions gauge = %v, 期望 0", got)
	}
	if _, ok := m.PlayerByToken("t1"); ok {
		t.Fatal("清扫后凭据索引应移除")
	}
	if _, ok := m.PlayerByRef("conn:1"); ok {
		t.Fatal("清扫后连接反向索引应移除")
	}
}

// TestSessionMetricsNoopMeter 验证未注入采集器（nil）时全部打点安全短路。
func TestSessionMetricsNoopMeter(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "t1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	m.Unbind(ctx, "p-1", 1)
}
