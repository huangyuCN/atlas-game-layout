package app

import (
	"testing"
	"time"

	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas/metrics"
)

// TestSessionOptionsOf 验证会话租期配置映射：
// 空值透传零值交给 session 包默认，非法值启动期报错（不静默降级）。
func TestSessionOptionsOf(t *testing.T) {
	tests := []struct {
		name      string
		session   *configspb.Session
		wantTTL   time.Duration
		wantSweep time.Duration
		wantErr   bool
	}{
		{"未配置", nil, 0, 0, false},
		{"空节", &configspb.Session{}, 0, 0, false},
		{"只配租期", &configspb.Session{Ttl: "45s"}, 45 * time.Second, 0, false},
		{"两个都配", &configspb.Session{Ttl: "45s", SweepInterval: "5s"}, 45 * time.Second, 5 * time.Second, false},
		{"租期非法", &configspb.Session{Ttl: "abc"}, 0, 0, true},
		{"租期非正", &configspb.Session{Ttl: "0s"}, 0, 0, true},
		{"清扫周期无单位", &configspb.Session{SweepInterval: "5"}, 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := sessionOptionsOf(&conf.Bootstrap{Session: tt.session})
			if tt.wantErr {
				if err == nil {
					t.Fatal("期望启动期报错, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("sessionOptionsOf: %v", err)
			}
			if opts.TTL != tt.wantTTL || opts.SweepInterval != tt.wantSweep {
				t.Fatalf("Options = (%v, %v), want (%v, %v)", opts.TTL, opts.SweepInterval, tt.wantTTL, tt.wantSweep)
			}
		})
	}
}

// TestNewSessionManagerPropagatesError 验证非法租期让装配失败（fx 启动期暴露，不进运行期）。
func TestNewSessionManagerPropagatesError(t *testing.T) {
	cli, err := pkredis.NewClient(pkredis.Options{Addrs: []string{"127.0.0.1:1"}, Namespace: "test"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	cfg := &conf.Bootstrap{Session: &configspb.Session{Ttl: "30"}}
	if _, err := newSessionManager(cfg, cli, metrics.Noop()); err == nil {
		t.Fatal("非法 session.ttl 应让 newSessionManager 失败")
	}
	cfg = &conf.Bootstrap{Session: &configspb.Session{Ttl: "45s"}}
	m, err := newSessionManager(cfg, cli, metrics.Noop())
	if err != nil {
		t.Fatalf("合法配置应装配成功: %v", err)
	}
	if m == nil {
		t.Fatal("newSessionManager 返回 nil")
	}
}

// TestLinkTimeoutOf 验证会话联动（网关 → game internal 面 Register/Login/Logout）的预算解析：
// 缺省 5s（与 relay.timeout 同口径）、显式值生效、非法与非正值**启动期报错**。
//
// 回归背景：这一跳曾经没有任何显式预算，实际生效的是框架 gRPC 客户端的缺省 2s，
// 于是「配了 5s 也没用」，失败耗时恰好 2.002s。缺省值与非法值两条断言在旧实现下都会红。
func TestLinkTimeoutOf(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "未配置取缺省", value: "", want: 5 * time.Second},
		{name: "显式 8s", value: "8s", want: 8 * time.Second},
		{name: "非法字符串", value: "abc", wantErr: true},
		{name: "零值", value: "0s", wantErr: true},
		{name: "负值", value: "-3s", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &conf.Bootstrap{Link: &conf.Link{Timeout: c.value}}
			got, err := linkTimeoutOf(cfg)
			if c.wantErr {
				if err == nil {
					t.Fatalf("非法预算 %q 应报错，got %v", c.value, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("预算 %q 解析失败: %v", c.value, err)
			}
			if got != c.want {
				t.Fatalf("预算 = %v，期望 %v", got, c.want)
			}
		})
	}
}

// TestNewPlayerConnRejectsInvalidLinkTimeout 验证非法预算让装配期失败（不是启动后才暴露）。
func TestNewPlayerConnRejectsInvalidLinkTimeout(t *testing.T) {
	cfg := &conf.Bootstrap{Link: &conf.Link{Timeout: "not-a-duration"}}
	conn, err := NewPlayerConn(cfg, nil, nil)
	if err == nil {
		_ = conn.Close()
		t.Fatal("非法 link.timeout 应让装配失败")
	}
}
