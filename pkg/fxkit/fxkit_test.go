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
	cfg := &fakeConf{
		runtime: testRuntime(),
		reg: &configspb.Registry{
			Etcd: &configspb.Registry_Etcd{Endpoints: []string{"127.0.0.1:1"}},
		},
	}
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

// TestRegistryOptionsNamespace 验证键前缀的唯一来源：runtime.namespace 经框架
// namespace.Derive 派生的 RegistryPrefix（不再读 registry.namespace，也不按 env 兜底）。
func TestRegistryOptionsNamespace(t *testing.T) {
	cases := []struct {
		name string
		cfg  *fakeConf
		want string
	}{
		{"按 runtime.namespace 派生", &fakeConf{runtime: &configspb.Runtime{Namespace: "test", Env: "prod"}}, "/atlas/services/test"},
		{"env 不参与派生", &fakeConf{runtime: &configspb.Runtime{Namespace: "p5-a", Env: "test"}}, "/atlas/services/p5-a"},
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

// TestRegistryOptionsRequiresNamespace 验证命名空间缺失即报错（R9：不回落 env/default）。
func TestRegistryOptionsRequiresNamespace(t *testing.T) {
	cfgs := []*fakeConf{
		{},
		{runtime: &configspb.Runtime{Env: "test"}},
		{reg: &configspb.Registry{Ttl: "30s"}},
	}
	for i, cfg := range cfgs {
		if _, err := RegistryOptions(cfg); err == nil {
			t.Fatalf("第 %d 个配置缺少 runtime.namespace 时期望报错，实际为 nil", i)
		}
	}
}

// TestRegistryOptionsTTL 验证 ttl 映射：未配置交给底层默认值，非法值快速失败。
func TestRegistryOptionsTTL(t *testing.T) {
	for _, bad := range []string{"0s", "-1s", "abc", "30"} {
		if _, err := RegistryOptions(&fakeConf{runtime: testRuntime(), reg: &configspb.Registry{Ttl: bad}}); err == nil {
			t.Fatalf("ttl=%q 期望报错，实际为 nil", bad)
		}
	}

	opts, err := RegistryOptions(&fakeConf{runtime: testRuntime(), reg: &configspb.Registry{Ttl: "30s"}})
	if err != nil {
		t.Fatalf("RegistryOptions() 错误 = %v", err)
	}
	if opts.TTL != 30*time.Second {
		t.Fatalf("TTL = %v, 期望 30s", opts.TTL)
	}

	opts, err = RegistryOptions(&fakeConf{runtime: testRuntime()})
	if err != nil {
		t.Fatalf("RegistryOptions() 错误 = %v", err)
	}
	if opts.TTL != 0 {
		t.Fatalf("未配置 TTL 应为零值（交给底层默认），实际 %v", opts.TTL)
	}
}

// TestNewRedisClientAppliesNamespace 验证 redis 客户端的键命名空间唯一来源是
// runtime.namespace（经框架 namespace.Derive 派生，与 actor subject / 业务 topic 同源）：
// 共用同一 redis 的多套部署靠它隔离。客户端惰性连接，构造不需要真实 redis。
func TestNewRedisClientAppliesNamespace(t *testing.T) {
	tests := []struct {
		name string
		rt   *configspb.Runtime
		want string
	}{
		{"按 runtime.namespace 派生", &configspb.Runtime{Namespace: "iso", Env: "test"}, "atlas:iso:gw:p1"},
		{"env 不参与派生", &configspb.Runtime{Namespace: "p5-a", Env: "prod"}, "atlas:p5-a:gw:p1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &fakeRedisConf{data: &configspb.Data{Redis: &configspb.Data_Redis{Addrs: []string{"127.0.0.1:1"}}}, rt: tt.rt}
			cli, err := NewRedisClient(cfg)
			if err != nil {
				t.Fatalf("NewRedisClient: %v", err)
			}
			t.Cleanup(func() { _ = cli.Close() })
			if got := cli.Keys().GatewayRoute("p1"); got != tt.want {
				t.Fatalf("Keys().GatewayRoute = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestNewRedisClientRequiresNamespace 验证命名空间缺失即报错（R9：不回落 env/default）。
func TestNewRedisClientRequiresNamespace(t *testing.T) {
	cfg := &fakeRedisConf{data: &configspb.Data{Redis: &configspb.Data_Redis{Addrs: []string{"127.0.0.1:1"}}}}
	if _, err := NewRedisClient(cfg); err == nil {
		t.Fatal("缺 runtime.namespace 期望报错，实际为 nil")
	}
}

// TestTopics 验证业务 topic 前缀与 actor/redis 同源（唯一来源 runtime.namespace），
// 缺失即报错。
func TestTopics(t *testing.T) {
	topics, err := Topics(&fakeConf{runtime: &configspb.Runtime{Namespace: "p5-a", Env: "test"}})
	if err != nil {
		t.Fatalf("Topics() 错误 = %v", err)
	}
	if got := topics.Push("p1"); got != "atlas.p5-a.push.p1" {
		t.Fatalf("Push = %q, 期望 atlas.p5-a.push.p1", got)
	}
	if _, err := Topics(&fakeConf{}); err == nil {
		t.Fatal("缺 runtime.namespace 期望报错，实际为 nil")
	}
}

// testRuntime 返回带命名空间的测试用服务身份。
func testRuntime() *configspb.Runtime { return &configspb.Runtime{Namespace: "test"} }
