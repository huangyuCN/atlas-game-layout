package nats

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// newTestServer 起内存 NATS Server。
func newTestServer(t *testing.T) string {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1",
		Port: -1,
	})
	if err != nil {
		t.Fatalf("启动 nats-server 失败: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats-server 未就绪")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// TestConnect 验证连接与 Ping。
func TestConnect(t *testing.T) {
	nc, err := Connect(Options{URL: newTestServer(t), Name: "test"})
	if err != nil {
		t.Fatalf("Connect() 错误 = %v", err)
	}
	t.Cleanup(nc.Close)
	if !nc.IsConnected() {
		t.Fatal("连接未建立")
	}
}

// TestConnectEmptyURL 验证空地址报错。
func TestConnectEmptyURL(t *testing.T) {
	if _, err := Connect(Options{}); err == nil {
		t.Fatal("Connect() 期望错误，实际为 nil")
	}
}

// TestPublishSubscribe 验证发布订阅往返。
func TestPublishSubscribe(t *testing.T) {
	nc, err := Connect(Options{URL: newTestServer(t)})
	if err != nil {
		t.Fatalf("Connect() 错误 = %v", err)
	}
	t.Cleanup(nc.Close)

	got := make(chan []byte, 1)
	if _, err := Subscribe(nc, "atlas.test", func(_ string, data []byte) {
		got <- data
	}); err != nil {
		t.Fatalf("Subscribe() 错误 = %v", err)
	}
	// 订阅注册有异步窗口，稍等后发布。
	time.Sleep(100 * time.Millisecond)
	if err := Publish(context.Background(), nc, "atlas.test", []byte("hello")); err != nil {
		t.Fatalf("Publish() 错误 = %v", err)
	}
	select {
	case data := <-got:
		if string(data) != "hello" {
			t.Fatalf("收到 %q，期望 hello", data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待订阅回调超时")
	}
}

// TestSubscribeNilGuard 验证 nil 连接/回调报错。
func TestSubscribeNilGuard(t *testing.T) {
	if _, err := Subscribe(nil, "x", func(string, []byte) {}); err == nil {
		t.Fatal("Subscribe(nil) 期望错误")
	}
	nc, _ := Connect(Options{URL: newTestServer(t)})
	t.Cleanup(nc.Close)
	if _, err := Subscribe(nc, "x", nil); err == nil {
		t.Fatal("Subscribe(nil 回调) 期望错误")
	}
}
