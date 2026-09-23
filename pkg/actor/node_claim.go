package actor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ErrNodeConflict 表示 actor 节点 ID 已被其他进程占用。
// actor NodeID 决定 NATS 节点 subject（`atlas_actor.<ns>.node.<nodeID>.*`），
// 同 ID 会共用同一套 subject、互相收到对方的节点消息（spawn/入站/控制），
// 表现为静默串台与幽灵实例；因此必须拒绝启动，而不是静默顶替。
var ErrNodeConflict = errors.New("actor: actor 节点 ID 已被占用")

// revokeTimeout 是释放/回滚租约的超时上限：Revoke 不能跟随调用方 ctx（释放要在
// 请求生命周期结束后仍然生效），但也**不能没有上限**——etcd 不可达时会挂住停机流程。
const revokeTimeout = 3 * time.Second

// nodeLeaseTTL 是节点归属键的租约 TTL（与目录租约同量级）。
// 进程被 kill -9 后旧租约仍会占键至多该时长——同 ID 快速重启会被自己的残留挡住，
// 与注册中心的实例冲突窗口同性质（等租约过期即可）。
const nodeLeaseTTL = 10 * time.Second

// reclaimBackoff 返回第 attempt 次重新声明归属的退避间隔（步进 500ms，上限 5s）。
func reclaimBackoff(attempt int) time.Duration {
	d := time.Duration(attempt) * 500 * time.Millisecond
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

// nodeClaimer 声明节点 ID 的归属，返回释放函数（须在停机时调用）。
type nodeClaimer interface {
	Claim(ctx context.Context, nodeID string) (release func(context.Context), err error)
}

// etcdNodeClaimer 用 etcd 租约 + 键（`<前缀>/nodes/<nodeID>`）声明节点归属：
// txn 保证「键不存在才写」，冲突时读旧键租约号一并报出，便于运维判断是残留还是活进程。
type etcdNodeClaimer struct {
	ec     *clientv3.Client
	prefix string
	ttl    time.Duration
}

// Claim 实现 nodeClaimer；ec 为 nil（未配置 etcd 的单元测试场景）时退化为空操作。
func (c etcdNodeClaimer) Claim(ctx context.Context, nodeID string) (func(context.Context), error) {
	if c.ec == nil {
		return func(context.Context) {}, nil
	}
	ttl := c.ttl
	if ttl <= 0 {
		ttl = nodeLeaseTTL
	}
	// keepCtx 只用于续租：与调用方请求生命周期解耦（Start 的 ctx 结束后仍要续租），
	// 但 Grant/Txn 仍用调用方 ctx——否则 etcd 不可达时会永久阻塞启动。
	keepCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	leaseID, _, ch, err := c.claimOnce(ctx, keepCtx, nodeID, ttl)
	if err != nil {
		cancel()
		return nil, err
	}
	go c.watchLease(keepCtx, nodeID, ttl, ch)
	release := func(releaseCtx context.Context) {
		cancel() // 先停续租：避免释放后又把租约续回来
		c.revokeBestEffort(releaseCtx, leaseID)
	}
	return release, nil
}

// revokeBestEffort 释放租约（尽力而为）：不跟随调用方 ctx，但有独立超时上限。
func (c etcdNodeClaimer) revokeBestEffort(ctx context.Context, leaseID clientv3.LeaseID) {
	revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), revokeTimeout)
	defer cancel()
	_, _ = c.ec.Revoke(revokeCtx, leaseID)
}

// claimOnce 授予租约、以 txn 声明归属并启动续租；返回租约号、键与续租通道。
// 调用方负责取消 keepCtx 停止续租；是否 Revoke 由调用方决定（正常释放要 Revoke，
// 模拟崩溃/断连则不 Revoke，等 TTL 自然到期）。
func (c etcdNodeClaimer) claimOnce(ctx, keepCtx context.Context, nodeID string, ttl time.Duration) (clientv3.LeaseID, string, <-chan *clientv3.LeaseKeepAliveResponse, error) {
	if nodeID == "" {
		return 0, "", nil, fmt.Errorf("actor: 节点声明缺少 NodeID")
	}
	lease, err := c.ec.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, "", nil, fmt.Errorf("actor: 声明节点归属失败（授予租约）: %w", err)
	}
	key := c.prefix + "/nodes/" + nodeID
	resp, err := c.ec.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, "", clientv3.WithLease(lease.ID))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		c.revokeBestEffort(ctx, lease.ID)
		return 0, "", nil, fmt.Errorf("actor: 声明节点归属失败（事务）: %w", err)
	}
	if !resp.Succeeded {
		held := heldLeaseOf(resp)
		c.revokeBestEffort(ctx, lease.ID)
		return 0, "", nil, fmt.Errorf("%w: nodeID=%s 持有租约=%d；等租约过期（%s）或为该进程显式配置唯一 runtime.id",
			ErrNodeConflict, nodeID, held, ttl)
	}
	ch, err := c.ec.KeepAlive(keepCtx, lease.ID)
	if err != nil {
		c.revokeBestEffort(ctx, lease.ID)
		return 0, "", nil, fmt.Errorf("actor: 节点归属续租启动失败: %w", err)
	}
	return lease.ID, key, ch, nil
}

// watchLease 消费续租响应；通道关闭（连接断开或租约被回收）时按退避**重新声明归属**。
// 归属丢失必须显式暴露、且尽力自愈：静默丢失会让同 ID 的新进程成功占用，
// 回到「共用节点 subject」的老路（启动期检查就形同虚设）。
// 主动释放（Shutdown 触发 cancel）时 ctx 已取消，不会走重声明分支。
func (c etcdNodeClaimer) watchLease(ctx context.Context, nodeID string, ttl time.Duration, ch <-chan *clientv3.LeaseKeepAliveResponse) {
	logger := log.GetLogger()
	drain := func(next <-chan *clientv3.LeaseKeepAliveResponse) {
		for range next {
		}
	}
	drain(ch)
	for attempt := 1; ctx.Err() == nil; attempt++ {
		leaseID, _, next, err := c.claimOnce(ctx, ctx, nodeID, ttl)
		if err == nil {
			logger.Warn("actor: 节点归属续租中断，已重新声明", "nodeID", nodeID, "lease", int64(leaseID))
			drain(next)
			attempt = 0 // 再次中断时退避从头开始
			continue
		}
		logger.Error("actor: 节点归属续租中断且重新声明失败——本进程可能与他人共用该 NodeID",
			"nodeID", nodeID, "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(reclaimBackoff(attempt)):
		}
	}
}

// heldLeaseOf 从「键已存在」分支的事务响应里取旧键租约号（取不到返回 0）。
func heldLeaseOf(resp *clientv3.TxnResponse) clientv3.LeaseID {
	if resp == nil || len(resp.Responses) == 0 {
		return 0
	}
	rng := resp.Responses[0].GetResponseRange()
	if rng == nil || len(rng.Kvs) == 0 {
		return 0
	}
	return clientv3.LeaseID(rng.Kvs[0].Lease)
}
