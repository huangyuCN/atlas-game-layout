package assemble

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
)

// testKey 是 32 字节票据密钥（进程内装配测试用）。
var testKey = []byte("0123456789abcdef0123456789abcdef")

// stubResolver 把固定后端地址交给接入层（不走 etcd 目录与注册中心）。
type stubResolver struct {
	address string
}

// Resolve 实现 edge.Resolver。
func (s stubResolver) Resolve(context.Context, edge.ResolveRequest) (edge.Backend, error) {
	if s.address == "" {
		return edge.Backend{}, errors.New("stubResolver: 无后端")
	}
	return edge.Backend{Address: s.address}, nil
}

// echoTCPBackend 起一个本地 TCP echo 假后端（回显收到的全部字节）。
func echoTCPBackend(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("起 TCP 假后端失败: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(conn, conn); _ = conn.Close() }()
		}
	}()
	return ln.Addr().String()
}

// signTicket 用 testKey 签一张票据（返回 base64url 无填充的密文）。
func signTicket(t *testing.T, battleID string) string {
	t.Helper()
	now := time.Now()
	raw, err := ticket.Encode(ticket.Ticket{
		Version: ticket.Version1, KID: 1, PlayerID: "p-1", BattleID: battleID,
		IssuedAt: now, ExpiresAt: now.Add(time.Minute),
	}, testKey)
	if err != nil {
		t.Fatalf("签发票据失败: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// startEmbedded 起一个只开 WS 面的进程内接入层，返回句柄。
func startEmbedded(t *testing.T, backend string) *Edge {
	t.Helper()
	e, err := New(context.Background(), Options{
		Namespace:     "edge-test",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		TicketKey:     testKey,
		Resolver:      stubResolver{address: backend},
		Listeners: []edge.Listener{
			{Name: "ws", Network: edge.NetworkTCP, Address: "127.0.0.1:0", Carrier: edge.CarrierWSUpgrade},
		},
	})
	if err != nil {
		t.Fatalf("进程内装配失败: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := e.Stop(ctx); err != nil {
			t.Errorf("停机失败: %v", err)
		}
	})
	return e
}

// TestEmbeddedWSDirectForward 覆盖进程内装配：健康端点 + WS 面「hello → 验票 → 转发 → 回程」。
func TestEmbeddedWSDirectForward(t *testing.T) {
	e := startEmbedded(t, echoTCPBackend(t))
	if len(e.ListenerAddrs) != 1 {
		t.Fatalf("接入面数应为 1，实际 %d", len(e.ListenerAddrs))
	}
	// 健康检查端点。
	resp, err := http.Get("http://" + e.HTTPURL + "/health")
	if err != nil {
		t.Fatalf("健康检查失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("健康检查应 200，实际 %d", resp.StatusCode)
	}
	// WS 面闭环：升级请求原样转发到假后端并被回显，随后帧字节双向转发。
	conn, err := net.DialTimeout("tcp", e.ListenerAddrs[0], 2*time.Second)
	if err != nil {
		t.Fatalf("拨接入层失败: %v", err)
	}
	defer conn.Close()
	head := fmt.Sprintf("GET /battle?ticket=%s HTTP/1.1\r\nHost: edge.test\r\n"+
		"Upgrade: websocket\r\nConnection: Upgrade\r\n\r\n", signTicket(t, "b-1"))
	if _, err := conn.Write([]byte(head)); err != nil {
		t.Fatalf("写升级请求失败: %v", err)
	}
	if got := readN(t, conn, len(head)); got != head {
		t.Fatalf("升级请求未被原样转发: %q", got)
	}
	if _, err := conn.Write([]byte("frame-input")); err != nil {
		t.Fatalf("写帧字节失败: %v", err)
	}
	if got := readN(t, conn, len("frame-input")); got != "frame-input" {
		t.Fatalf("回程字节不一致: %q", got)
	}
}

// TestEmbeddedRejectsExpiredTicket 覆盖过期票在进程内形态同样被断开。
func TestEmbeddedRejectsExpiredTicket(t *testing.T) {
	e := startEmbedded(t, echoTCPBackend(t))
	now := time.Now()
	raw, err := ticket.Encode(ticket.Ticket{
		Version: ticket.Version1, KID: 1, PlayerID: "p-1", BattleID: "b-1",
		IssuedAt: now.Add(-2 * time.Minute), ExpiresAt: now.Add(-time.Minute),
	}, testKey)
	if err != nil {
		t.Fatalf("签发票据失败: %v", err)
	}
	conn, err := net.DialTimeout("tcp", e.ListenerAddrs[0], 2*time.Second)
	if err != nil {
		t.Fatalf("拨接入层失败: %v", err)
	}
	defer conn.Close()
	head := "GET /battle?ticket=" + base64.RawURLEncoding.EncodeToString(raw) + " HTTP/1.1\r\n" +
		"Host: edge.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
	if _, err := conn.Write([]byte(head)); err != nil {
		t.Fatalf("写升级请求失败: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 64)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("过期票应被断开")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("过期票未被断开（读超时）")
	}
}

// TestEmbeddedConfigValidation 覆盖进程内装配的参数校验（命名空间缺失 / 密钥长度不符）。
func TestEmbeddedConfigValidation(t *testing.T) {
	base := Options{TicketKey: testKey, Resolver: stubResolver{address: "127.0.0.1:1"}}
	if _, err := New(context.Background(), base); err == nil {
		t.Fatal("命名空间缺失应报错")
	} else if _, err := New(context.Background(), Options{
		Namespace: "edge-test", EtcdEndpoints: []string{"127.0.0.1:12379"},
		TicketKey: []byte("short"), Resolver: stubResolver{address: "127.0.0.1:1"},
	}); !errors.Is(err, ticket.ErrKeySize) {
		t.Fatalf("密钥长度不符应报 ticket.ErrKeySize，实际 %v", err)
	}
}

// readN 读满 n 字节（带超时），失败即测试失败。
func readN(t *testing.T, conn net.Conn, n int) string {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, n)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("读回程数据失败: %v", err)
	}
	return string(buf)
}
