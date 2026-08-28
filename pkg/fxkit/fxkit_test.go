package fxkit

import (
	"testing"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
)

// fakeConf 是满足 WithRegistry 的最小测试配置。
type fakeConf struct {
	reg *configspb.Registry
}

func (f *fakeConf) GetRegistry() *configspb.Registry { return f.reg }

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
	cli, err := NewEtcdClient(&fakeConf{reg: &configspb.Registry{
		Etcd: &configspb.Registry_Etcd{Endpoints: []string{"127.0.0.1:1"}},
	}})
	if err != nil {
		t.Fatalf("NewEtcdClient() 错误 = %v", err)
	}
	defer func() { _ = cli.Close() }()

	reg, err := NewRegistrar(cli)
	if err != nil {
		t.Fatalf("NewRegistrar() 错误 = %v", err)
	}
	if reg == nil {
		t.Fatal("NewRegistrar() 应返回注册器")
	}
}

// TestNewRegistrarNilClient 验证空客户端被拒绝。
func TestNewRegistrarNilClient(t *testing.T) {
	if _, err := NewRegistrar(nil); err == nil {
		t.Fatal("NewRegistrar(nil) 期望报错，实际为 nil")
	}
}
