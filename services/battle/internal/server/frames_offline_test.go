package server

import (
	"testing"
	"time"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
)

// testRecorder 返回帧槽验票的登记端口（连接生命周期桥；投递端口为 nil = 只登记不上报）。
func testRecorder() *stream.Bridge {
	return stream.NewBridge(stream.NewRegistry(), nil)
}

// testPolicy 返回默认帧面策略（掉线窗口 15s，与 battle 配置缺省一致）。
func testPolicy() FramePolicy { return FramePolicy{OfflineTimeout: 15 * time.Second} }

// TestDatagramIdleDerivation 验证数据报面空闲读超时的推导与硬校验（规格 §9.2 硬约束①）：
// 未配置取 offline_timeout/3（缺省 15s → 5s）；显式配置必须严格小于掉线窗口，否则装配失败。
func TestDatagramIdleDerivation(t *testing.T) {
	cases := []struct {
		name       string
		configured time.Duration
		offline    time.Duration
		want       time.Duration
		wantErr    bool
	}{
		{name: "缺省取窗口三分之一", configured: 0, offline: 15 * time.Second, want: 5 * time.Second},
		{name: "非默认窗口同样取三分之一", configured: 0, offline: 900 * time.Millisecond, want: 300 * time.Millisecond},
		{name: "显式配置小于窗口即保留", configured: time.Second, offline: 15 * time.Second, want: time.Second},
		{name: "显式配置等于窗口即失败", configured: 15 * time.Second, offline: 15 * time.Second, wantErr: true},
		{name: "显式配置大于窗口即失败", configured: 20 * time.Second, offline: 15 * time.Second, wantErr: true},
		{name: "关闭掉线判定即不推导", configured: 0, offline: 0, want: 0},
		{name: "关闭掉线判定时显式配置原样保留", configured: 30 * time.Second, offline: 0, want: 30 * time.Second},
	}
	for _, c := range cases {
		got, err := datagramIdle(c.configured, c.offline)
		if c.wantErr {
			if err == nil {
				t.Fatalf("%s：竟然推导成功（idle=%s）", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s：datagramIdle: %v", c.name, err)
		}
		if got != c.want {
			t.Fatalf("%s：idle = %s, 期望 %s", c.name, got, c.want)
		}
	}
}

// TestNewFrameServersRejectsLooseDatagramIdle 验证装配期硬校验：数据报面 idle ≥ offline_timeout
// 即启动失败（KCP/UDP 无关闭握手，只能靠空闲读超时发现掉线；不校验即策略形同虚设）。
func TestNewFrameServersRejectsLooseDatagramIdle(t *testing.T) {
	ops, err := NewFrameOps(nil, testRecorder(), testTicketKey())
	if err != nil {
		t.Fatalf("NewFrameOps: %v", err)
	}
	cases := map[string]*conf.Bootstrap{
		"KCP 面超限": {Server: &configspb.Server{
			Kcp: &configspb.Server_KCP{Addr: "127.0.0.1:0", IdleTimeout: "20s"},
		}},
		"UDP 面等于窗口": {Server: &configspb.Server{
			Udp: &configspb.Server_UDP{Addr: "127.0.0.1:0", IdleTimeout: "15s"},
		}},
	}
	for name, cfg := range cases {
		if _, err := NewFrameServers(cfg, ops, testPolicy(), testRecorder()); err == nil {
			t.Fatalf("%s：数据报面 idle ≥ offline_timeout 竟然构造成功", name)
		}
	}
	// 流式面（WS）有可靠 EOF，下发窗口不影响掉线发现，不受该约束。
	wsCfg := &conf.Bootstrap{Server: &configspb.Server{
		Websocket: &configspb.Server_WebSocket{Addr: "127.0.0.1:0", IdleTimeout: "30s"},
	}}
	if _, err := NewFrameServers(wsCfg, ops, testPolicy(), testRecorder()); err != nil {
		t.Fatalf("WS 面不应受数据报面约束: %v", err)
	}
}
