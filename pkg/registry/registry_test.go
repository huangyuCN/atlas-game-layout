package registry

import (
	"testing"
	"time"

	"github.com/huangyuCN/atlas/namespace"
	"github.com/huangyuCN/atlas/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// newTestClient 构造测试用 etcd 客户端（惰性连接，不要求真 etcd）。
func newTestClient(t *testing.T) *clientv3.Client {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:12379"},
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("构造 etcd 客户端失败: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestNewEtcd 验证注册器构造与接口满足性（键前缀取 namespace.Derive 的 RegistryPrefix）。
func TestNewEtcd(t *testing.T) {
	derived, err := namespace.Derive("test")
	if err != nil {
		t.Fatalf("namespace.Derive(test): %v", err)
	}
	r, err := NewEtcd(newTestClient(t), Options{Namespace: derived.RegistryPrefix, TTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("NewEtcd() 错误 = %v", err)
	}
	var _ registry.Registrar = r // 接口满足性断言
}

// TestNewEtcdRequiresNamespace 验证键前缀缺失即报错（R9：不回落 /atlas/services 根前缀，
// 否则多套部署会共用同一键空间而互相覆盖注册）。
func TestNewEtcdRequiresNamespace(t *testing.T) {
	if _, err := NewEtcd(newTestClient(t), Options{}); err == nil {
		t.Fatal("NewEtcd() 期望缺少键前缀错误，实际为 nil")
	}
}

// TestNewEtcdNilClient 验证 nil 客户端报错。
func TestNewEtcdNilClient(t *testing.T) {
	if _, err := NewEtcd(nil, Options{}); err == nil {
		t.Fatal("NewEtcd() 期望错误，实际为 nil")
	}
}
