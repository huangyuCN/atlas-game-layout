package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// TestModeString 验证形态枚举的名称（错误信息/日志用；越界值自解释）。
func TestModeString(t *testing.T) {
	cases := []struct {
		mode Mode
		want string
	}{
		{ModeSingle, "single"},
		{ModeSentinel, "sentinel"},
		{ModeCluster, "cluster"},
		{Mode(99), "unknown(99)"},
	}
	for _, tc := range cases {
		if got := tc.mode.String(); got != tc.want {
			t.Fatalf("Mode(%d).String() = %q, 期望 %q", int(tc.mode), got, tc.want)
		}
	}
}

// TestNewClientSingleUsesAddrs 验证单点形态由 Addrs 承载（取首个地址）且可连通。
func TestNewClientSingleUsesAddrs(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("启动 miniredis 失败: %v", err)
	}
	t.Cleanup(mr.Close)

	// Mode 零值即 single。
	c, err := NewClient(Options{Addrs: []string{mr.Addr()}})
	if err != nil {
		t.Fatalf("NewClient() 错误 = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() 错误 = %v", err)
	}
}

// TestNewClientValidation 验证按形态校验，错误信息含形态名与缺失项。
func TestNewClientValidation(t *testing.T) {
	cases := []struct {
		name    string
		opts    Options
		wantErr string
	}{
		{"single 无地址", Options{}, "single"},
		{"single 多地址", Options{Addrs: []string{"127.0.0.1:6379", "127.0.0.1:6380"}}, "single"},
		{"sentinel 缺 master_name", Options{Mode: ModeSentinel, Addrs: []string{"127.0.0.1:26379"}}, "master_name"},
		{"sentinel 缺地址", Options{Mode: ModeSentinel, MasterName: "mymaster"}, "sentinel"},
		{"cluster 缺地址", Options{Mode: ModeCluster}, "cluster"},
		{"越界形态", Options{Mode: Mode(9), Addrs: []string{"127.0.0.1:6379"}}, "unknown(9)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewClient(tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("NewClient() = %v, 期望错误信息含 %q", err, tc.wantErr)
			}
		})
	}
}

// TestNewClientSentinelAndCluster 验证哨兵/集群形态可构造（惰性连接，不建连）。
func TestNewClientSentinelAndCluster(t *testing.T) {
	sc, err := NewClient(Options{
		Mode:       ModeSentinel,
		MasterName: "mymaster",
		Addrs:      []string{"127.0.0.1:26379", "127.0.0.1:26380"},
		Password:   "pw",
		DB:         2,
	})
	if err != nil {
		t.Fatalf("sentinel NewClient() 错误 = %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })

	cc, err := NewClient(Options{
		Mode:     ModeCluster,
		Addrs:    []string{"127.0.0.1:7000", "127.0.0.1:7001"},
		Password: "pw",
	})
	if err != nil {
		t.Fatalf("cluster NewClient() 错误 = %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
}
