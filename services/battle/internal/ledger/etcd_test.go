package ledger

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcdEndpoints 返回集成用例的 etcd 地址（ATLAS_LEDGER_ETCD，逗号分隔）；未设置即跳过。
func etcdEndpoints() []string {
	raw := os.Getenv("ATLAS_LEDGER_ETCD")
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
}

// TestEtcdStoreRecordLookup 验证 etcd 留档实现（需真实 etcd，按 AGENTS.md 在集成服务器执行）：
//
//	ATLAS_LEDGER_ETCD=127.0.0.1:12379 go test ./services/battle/internal/ledger/ -run TestEtcdStore -count=1
//
// 断言：写入后可读且内容一致；前缀不同互不可见（命名空间隔离）；租约到期自动消失
// （留档不需要清扫任务）；空 battle_id 不落档。
func TestEtcdStoreRecordLookup(t *testing.T) {
	endpoints := etcdEndpoints()
	if len(endpoints) == 0 {
		t.Skip("未设置 ATLAS_LEDGER_ETCD，跳过 etcd 留档集成用例")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Skipf("etcd 不可用: %v", err)
	}
	prefix := fmt.Sprintf("/atlas-it/ledger/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = cli.Delete(context.Background(), prefix, clientv3.WithPrefix())
		_ = cli.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store := NewEtcdStore(cli, prefix)
	if err := store.Record(ctx, "b-1", Entry{Winner: "p-a", Players: []string{"p-a", "p-b"}}, 2*time.Second); err != nil {
		t.Fatalf("Record: %v", err)
	}
	got, ok, err := store.Lookup(ctx, "b-1")
	if err != nil || !ok || got.Winner != "p-a" || len(got.Players) != 2 {
		t.Fatalf("留档内容不符: %+v ok=%v err=%v", got, ok, err)
	}
	// 命名空间隔离：不同前缀看不到彼此的留档（共用同一 etcd 的两套部署不串台）。
	if _, ok, _ := NewEtcdStore(cli, prefix+"-other").Lookup(ctx, "b-1"); ok {
		t.Fatal("不同前缀不应命中留档")
	}
	if _, ok, _ := store.Lookup(ctx, "b-missing"); ok {
		t.Fatal("未留档的对局不应命中")
	}
	if err := store.Record(ctx, "", Entry{Winner: "p-a"}, time.Second); err != nil {
		t.Fatalf("空 battle_id 应忽略: %v", err)
	}

	// 租约 TTL 到期即自动消失（轮询到未命中，避免依赖精确到期时刻）。
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok, err := store.Lookup(ctx, "b-1"); err != nil {
			t.Fatalf("Lookup: %v", err)
		} else if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("留档未按租约 TTL 过期")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
