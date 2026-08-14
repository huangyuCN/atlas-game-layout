package registry

import (
	"testing"
	"time"

	"github.com/huangyuCN/atlas/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestNewEtcd 验证注册器构造与接口满足性。
func TestNewEtcd(t *testing.T) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:12379"},
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("构造 etcd 客户端失败: %v", err)
	}
	defer func() { _ = client.Close() }()

	r, err := NewEtcd(client, Options{Namespace: "/test/services", TTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("NewEtcd() 错误 = %v", err)
	}
	var _ registry.Registrar = r // 接口满足性断言
}

// TestNewEtcdNilClient 验证 nil 客户端报错。
func TestNewEtcdNilClient(t *testing.T) {
	if _, err := NewEtcd(nil, Options{}); err == nil {
		t.Fatal("NewEtcd() 期望错误，实际为 nil")
	}
}
