package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	"github.com/huangyuCN/atlas/metrics"
)

// randomPortEdgeConfig 返回三面随机端口（且 udp 面在前）的配置，
// 用于覆盖「客户端主面 = 首个 tcp 面」的选取逻辑。
func randomPortEdgeConfig() *conf.Bootstrap {
	svc := validEdgeConfig()
	svc.Edge.Listeners = []*conf.Edge_Listener{
		{Name: "udp", Network: conf.Edge_NETWORK_UDP, Addr: "127.0.0.1:0", Carrier: conf.Edge_CARRIER_DATAGRAM},
		{Name: "ws", Network: conf.Edge_NETWORK_TCP, Addr: "127.0.0.1:0", Carrier: conf.Edge_CARRIER_WS_UPGRADE},
	}
	return svc
}

// TestProxyServerEndpoint 覆盖接入层端点适配：监听前回落配置地址、监听后取内核分配的实际地址。
func TestProxyServerEndpoint(t *testing.T) {
	svc := randomPortEdgeConfig()
	proxy, err := newProxy(svc, stubResolver{}, metrics.Noop(), nil)
	if err != nil {
		t.Fatalf("构造接入层失败: %v", err)
	}
	srv := newProxyServer(proxy, svc.GetEdge().GetListeners())
	before, err := srv.Endpoint()
	if err != nil {
		t.Fatalf("监听前取端点失败: %v", err)
	}
	if before.String() != "tcp://127.0.0.1:0" {
		t.Fatalf("监听前应回落配置主面（首个 tcp 面），实际 %s", before)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- proxy.Start(ctx) }()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer readyCancel()
	if err := proxy.WaitReady(readyCtx); err != nil {
		t.Fatalf("等待就绪失败: %v", err)
	}
	after, err := srv.Endpoint()
	if err != nil {
		t.Fatalf("监听后取端点失败: %v", err)
	}
	addrs := proxy.ListenerAddrs()
	if after.String() != "tcp://"+addrs[1] {
		t.Fatalf("监听后端点应为客户端主面实际地址 tcp://%s，实际 %s", addrs[1], after)
	}
	if strings.HasSuffix(after.String(), ":0") {
		t.Fatalf("监听后端口不应为 0: %s", after)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("停机返回错误: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("接入层未在 5s 内停机")
	}
}
