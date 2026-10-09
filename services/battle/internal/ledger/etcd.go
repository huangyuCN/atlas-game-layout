// 留档的 etcd 实现：结算节点写一次，任意节点可读——跨节点准入的唯一事实源。

package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// minLeaseSeconds 是 etcd 租约的最短秒数（etcd 只接受正整数秒，更短的 TTL 向上取整）。
const minLeaseSeconds = int64(1)

// EtcdStore 是留档的 etcd 实现：每局一个键（`<prefix>/<battleID>`），值为 Entry 的 JSON，
// 挂租约 TTL——到期自动消失，不需要清扫任务；TTL 与本地留档同源（stream.EndedTTL）。
//
// 前缀由装配层按命名空间派生（`<derived.EtcdDirectory>/battle_ended`），使共用同一 etcd 的
// 两套部署不共享留档（与目录/节点归属键同一隔离口径）。
type EtcdStore struct {
	cli    *clientv3.Client
	prefix string
}

// NewEtcdStore 构造 etcd 留档存储（cli 为空即不可用，读写都报错而不是静默成功）。
func NewEtcdStore(cli *clientv3.Client, prefix string) *EtcdStore {
	return &EtcdStore{cli: cli, prefix: prefix}
}

// Record 实现 Store：写入留档并挂租约（ttl ≤ 0 视为已过期，不静默永久保留）。
func (s *EtcdStore) Record(ctx context.Context, battleID string, e Entry, ttl time.Duration) error {
	if s == nil || s.cli == nil {
		return fmt.Errorf("ledger: etcd 客户端为空")
	}
	if battleID == "" {
		return nil
	}
	if ttl <= 0 {
		return fmt.Errorf("ledger: 留档 TTL 必须为正（battle=%s）", battleID)
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("ledger: 留档序列化失败: %w", err)
	}
	lease, err := s.cli.Grant(ctx, leaseSeconds(ttl))
	if err != nil {
		return fmt.Errorf("ledger: 申请留档租约失败: %w", err)
	}
	if _, err := s.cli.Put(ctx, s.key(battleID), string(payload), clientv3.WithLease(lease.ID)); err != nil {
		return fmt.Errorf("ledger: 写入留档失败: %w", err)
	}
	return nil
}

// Lookup 实现 Store：读留档；键不存在返回未命中（未留档不是错误）。
func (s *EtcdStore) Lookup(ctx context.Context, battleID string) (Entry, bool, error) {
	if s == nil || s.cli == nil {
		return Entry{}, false, fmt.Errorf("ledger: etcd 客户端为空")
	}
	if battleID == "" {
		return Entry{}, false, nil
	}
	resp, err := s.cli.Get(ctx, s.key(battleID))
	if err != nil {
		return Entry{}, false, fmt.Errorf("ledger: 读取留档失败: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return Entry{}, false, nil
	}
	var e Entry
	if err := json.Unmarshal(resp.Kvs[0].Value, &e); err != nil {
		return Entry{}, false, fmt.Errorf("ledger: 留档反序列化失败: %w", err)
	}
	return e, true, nil
}

// key 返回该对局的留档键（与本地留档同为按 battle_id 索引）。
func (s *EtcdStore) key(battleID string) string { return s.prefix + "/" + battleID }

// leaseSeconds 把 TTL 向上取整为 etcd 租约秒数（至少 minLeaseSeconds）。
func leaseSeconds(ttl time.Duration) int64 {
	seconds := int64((ttl + time.Second - 1) / time.Second)
	if seconds < minLeaseSeconds {
		return minLeaseSeconds
	}
	return seconds
}
