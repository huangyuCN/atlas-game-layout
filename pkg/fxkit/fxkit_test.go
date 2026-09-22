package fxkit

import (
	"testing"
	"time"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
)

// fakeConf 是满足 RegistryConfig（GetRegistry + GetRuntime）的最小测试配置。
type fakeConf struct {
	runtime *configspb.Runtime
	reg     *configspb.Registry
}

func (f *fakeConf) GetRegistry() *configspb.Registry { return f.reg }

func (f *fakeConf) GetRuntime() *configspb.Runtime { return f.runtime }

// TestEtcdEndpoints 验证从配置提取 etcd 端点（含缺失分支）。
func TestEtcdEndpoints(t *testing.T) {
	with := &fakeConf{reg: &configspb.Registry{
		Etcd: &configspb.Registry_Etcd{Endpoints: []string{"127.0.0.1:12379"}},
	}}
	got := EtcdEndpoints(with)
	if len(got) != 1 || got[0] != "127.0.0.1:12379" {
		t.Fatalf("EtcdEndpoints() = %v, 期望 [127.0.0.1:12379]", got)
	}

	// registry 段缺失时返回空切片（由调用方决定是否报错）。
	if got := EtcdEndpoints(&fakeConf{}); len(got) != 0 {
		t.Fatalf("EtcdEndpoints() 缺失时应为空, 实际 %v", got)
	}
}

// TestNewEtcdClientMissingEndpoints 验证缺少 registry.etcd 时快速失败并给出指引。
func TestNewEtcdClientMissingEndpoints(t *testing.T) {
	_, err := NewEtcdClient(&fakeConf{})
	if err == nil {
		t.Fatal("NewEtcdClient() 缺少配置时期望报错，实际为 nil")
	}
	t.Log(err)
}

// TestNewEtcdClientOK 验证合法端点可构造客户端（惰性连接，不要求真 etcd）。
func TestNewEtcdClientOK(t *testing.T) {
	cfg := &fakeConf{reg: &configspb.Registry{
		Etcd: &configspb.Registry_Etcd{Endpoints: []string{"127.0.0.1:1"}},
	}}
	cli, err := NewEtcdClient(cfg)
	if err != nil {
		t.Fatalf("NewEtcdClient() 错误 = %v", err)
	}
	defer func() { _ = cli.Close() }()

	reg, err := NewRegistrar(cfg, cli)
	if err != nil {
		t.Fatalf("NewRegistrar() 错误 = %v", err)
	}
	if reg == nil {
		t.Fatal("NewRegistrar() 应返回注册器")
	}
	disc, err := NewEtcdDiscovery(cfg, cli)
	if err != nil {
		t.Fatalf("NewEtcdDiscovery() 错误 = %v", err)
	}
	if disc == nil {
		t.Fatal("NewEtcdDiscovery() 应返回发现器")
	}
}

// TestNewRegistrarNilClient 验证空客户端被拒绝。
func TestNewRegistrarNilClient(t *testing.T) {
	if _, err := NewRegistrar(&fakeConf{}, nil); err == nil {
		t.Fatal("NewRegistrar(nil) 期望报错，实际为 nil")
	}
}

// TestRegistryOptionsNamespace 验证键前缀派生：显式配置优先，否则按环境隔离。
func TestRegistryOptionsNamespace(t *testing.T) {
	cases := []struct {
		name string
		cfg  *fakeConf
		want string
	}{
		{"显式 namespace 优先", &fakeConf{reg: &configspb.Registry{Namespace: "/custom/ns"}}, "/custom/ns"},
		{"按 runtime.env 隔离", &fakeConf{runtime: &configspb.Runtime{Env: "test"}}, "/atlas/services/test"},
		{"env 缺省 default", &fakeConf{runtime: &configspb.Runtime{}}, "/atlas/services/default"},
		{"配置段全缺", &fakeConf{}, "/atlas/services/default"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts, err := RegistryOptions(c.cfg)
			if err != nil {
				t.Fatalf("RegistryOptions() 错误 = %v", err)
			}
			if opts.Namespace != c.want {
				t.Fatalf("namespace = %q, 期望 %q", opts.Namespace, c.want)
			}
		})
	}
}

// TestRegistryOptionsTTL 验证 ttl_seconds 映射：未配置交给底层默认值，非法值快速失败。
func TestRegistryOptionsTTL(t *testing.T) {
	ttl := func(v int32) *int32 { return &v }

	for _, bad := range []int32{0, -1} {
		if _, err := RegistryOptions(&fakeConf{reg: &configspb.Registry{TtlSeconds: ttl(bad)}}); err == nil {
			t.Fatalf("ttl_seconds=%d 期望报错，实际为 nil", bad)
		}
	}

	opts, err := RegistryOptions(&fakeConf{reg: &configspb.Registry{TtlSeconds: ttl(30)}})
	if err != nil {
		t.Fatalf("RegistryOptions() 错误 = %v", err)
	}
	if opts.TTL != 30*time.Second {
		t.Fatalf("TTL = %v, 期望 30s", opts.TTL)
	}

	opts, err = RegistryOptions(&fakeConf{})
	if err != nil {
		t.Fatalf("RegistryOptions() 错误 = %v", err)
	}
	if opts.TTL != 0 {
		t.Fatalf("未配置 TTL 应为零值（交给底层默认），实际 %v", opts.TTL)
	}
}
