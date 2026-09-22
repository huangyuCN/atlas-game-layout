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
	cli, err := pkredis.NewClient(pkredis.Options{Addrs: []string{"127.0.0.1:1"}})
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
