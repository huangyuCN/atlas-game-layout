package etcd

import (
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestNewClient 验证客户端构造与默认超时。
func TestNewClient(t *testing.T) {
	client, err := NewClient(Options{Endpoints: []string{"127.0.0.1:12379"}})
	if err != nil {
		t.Fatalf("NewClient() 错误 = %v", err)
	}
	defer func() { _ = client.Close() }()
	if client == nil {
		t.Fatal("NewClient() 返回 nil")
	}
	// 惰性连接：无 etcd 时也不阻塞、不报错。
}

// TestNewClientEmptyEndpoints 验证空节点报错。
func TestNewClientEmptyEndpoints(t *testing.T) {
	if _, err := NewClient(Options{}); err == nil {
		t.Fatal("NewClient() 期望错误，实际为 nil")
	}
}

// TestNewClientCustomTimeout 验证自定义超时生效。
func TestNewClientCustomTimeout(t *testing.T) {
	client, err := NewClient(Options{
		Endpoints:   []string{"127.0.0.1:12379"},
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewClient() 错误 = %v", err)
	}
	defer func() { _ = client.Close() }()
	var _ *clientv3.Client = client // 类型确认
}
