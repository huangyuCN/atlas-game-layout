package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// newTestClient 起内存 Redis（miniredis）构造客户端。
func newTestClient(t *testing.T) *Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("启动 miniredis 失败: %v", err)
	}
	t.Cleanup(mr.Close)
	client, err := NewClient(Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("NewClient() 错误 = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestNewClientEmptyAddr 验证空地址报错。
func TestNewClientEmptyAddr(t *testing.T) {
	if _, err := NewClient(Options{}); err == nil {
		t.Fatal("NewClient() 期望错误，实际为 nil")
	}
}

// TestPlayerSessionRoundTrip 验证会话令牌写入/读取/删除。
func TestPlayerSessionRoundTrip(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// 不存在时返回空串。
	if v, err := c.GetPlayerSession(ctx, "p1"); err != nil || v != "" {
		t.Fatalf("初始读取 = %q/%v，期望空串", v, err)
	}
	// 写入并读取。
	if err := c.SetPlayerSession(ctx, "p1", "tok-1", time.Minute); err != nil {
		t.Fatalf("SetPlayerSession: %v", err)
	}
	if v, err := c.GetPlayerSession(ctx, "p1"); err != nil || v != "tok-1" {
		t.Fatalf("读取 = %q/%v，期望 tok-1", v, err)
	}
	// 删除。
	if err := c.DelPlayerSession(ctx, "p1"); err != nil {
		t.Fatalf("DelPlayerSession: %v", err)
	}
	if v, _ := c.GetPlayerSession(ctx, "p1"); v != "" {
		t.Fatalf("删除后仍读到 %q", v)
	}
}

// TestPlayerSessionTTL 验证会话过期（miniredis 虚拟时间推进）。
func TestPlayerSessionTTL(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("启动 miniredis 失败: %v", err)
	}
	t.Cleanup(mr.Close)
	c, err := NewClient(Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("NewClient() 错误 = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	if err := c.SetPlayerSession(ctx, "p1", "tok", 50*time.Millisecond); err != nil {
		t.Fatalf("SetPlayerSession: %v", err)
	}
	mr.FastForward(80 * time.Millisecond)
	if v, _ := c.GetPlayerSession(ctx, "p1"); v != "" {
		t.Fatalf("过期后仍读到 %q", v)
	}
}

// TestGatewayRouteRoundTrip 验证 gateway 分布式路由表。
func TestGatewayRouteRoundTrip(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if err := c.SetGatewayRoute(ctx, "p1", "gw-1", time.Minute); err != nil {
		t.Fatalf("SetGatewayRoute: %v", err)
	}
	if v, err := c.GetGatewayRoute(ctx, "p1"); err != nil || v != "gw-1" {
		t.Fatalf("GetGatewayRoute = %q/%v，期望 gw-1", v, err)
	}
	// 覆盖写（玩家迁移到新实例）。
	if err := c.SetGatewayRoute(ctx, "p1", "gw-2", time.Minute); err != nil {
		t.Fatalf("SetGatewayRoute(覆盖): %v", err)
	}
	if v, _ := c.GetGatewayRoute(ctx, "p1"); v != "gw-2" {
		t.Fatalf("覆盖后 = %q，期望 gw-2", v)
	}
}

// TestSetPlayerSessionEmpty 验证空参数报错。
func TestSetPlayerSessionEmpty(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	if err := c.SetPlayerSession(ctx, "", "tok", time.Minute); err == nil {
		t.Fatal("SetPlayerSession() 期望空 playerID 错误")
	}
	if err := c.SetPlayerSession(ctx, "p1", "", time.Minute); err == nil {
		t.Fatal("SetPlayerSession() 期望空 token 错误")
	}
}
