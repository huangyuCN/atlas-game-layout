package actor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
)

// fakeClaimer 是节点归属声明的测试桩：可控失败与记录释放调用。
type fakeClaimer struct {
	err      error
	claimed  []string
	released int
}

// Claim 实现 nodeClaimer。
func (f *fakeClaimer) Claim(_ context.Context, nodeID string) (func(context.Context), error) {
	if f.err != nil {
		return nil, f.err
	}
	f.claimed = append(f.claimed, nodeID)
	return func(context.Context) { f.released++ }, nil
}

// TestStartAbortsWhenNodeClaimFails 验证节点归属声明失败即启动失败：
// 不进入集群运行时启动（同 ID 会共用 NATS 节点 subject，静默顶替必须被拒）。
func TestStartAbortsWhenNodeClaimFails(t *testing.T) {
	rt := &Runtime{nodeID: "node-x", claimer: &fakeClaimer{err: fmt.Errorf("%w: nodeID=node-x", ErrNodeConflict)}}
	err := rt.Start(context.Background())
	if !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("Start() 错误 = %v, 期望 ErrNodeConflict", err)
	}
}

// TestNodeClaimerCalled 验证声明使用运行时节点 ID。
func TestNodeClaimerCalled(t *testing.T) {
	fc := &fakeClaimer{}
	rt := &Runtime{nodeID: "node-y", claimer: fc}
	// 只验证声明环节：集群运行时未初始化，Start 会在 inner 上 panic，故直接调用 claimer 路径。
	release, err := rt.claimer.Claim(context.Background(), rt.nodeID)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(fc.claimed) != 1 || fc.claimed[0] != "node-y" {
		t.Fatalf("声明节点 ID = %v, 期望 [node-y]", fc.claimed)
	}
	release(context.Background())
	if fc.released != 1 {
		t.Fatalf("释放次数 = %d, 期望 1", fc.released)
	}
}

// TestEtcdNodeClaimConflict 验证 etcd 归属键：同 ID 第二次声明被拒且报出持有租约，
// 释放后可再次声明。无 etcd 时自动跳过（按 AGENTS.md 在 10.10.9.36 执行）。
func TestEtcdNodeClaimConflict(t *testing.T) {
	ctx := context.Background()
	ec, err := etcd.NewClient(etcd.Options{Endpoints: []string{"127.0.0.1:12379"}})
	if err != nil {
		t.Skipf("etcd 不可用: %v", err)
	}
	t.Cleanup(func() { _ = ec.Close() })
	claimer := etcdNodeClaimer{ec: ec, prefix: "/atlas/actors/it-claim", ttl: nodeLeaseTTL}
	nodeID := fmt.Sprintf("it-claim-%d", time.Now().UnixNano())

	// 用带截止的 ctx 调用：etcd 不可达时快速失败并跳过，而不是挂到测试超时。
	claimCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	release, err := claimer.Claim(claimCtx, nodeID)
	if err != nil {
		t.Skipf("etcd 不可用（租约声明失败）: %v", err)
	}
	_, err = claimer.Claim(ctx, nodeID)
	if !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("同 ID 二次声明错误 = %v, 期望 ErrNodeConflict", err)
	}
	release(ctx)
	release2, err := claimer.Claim(ctx, nodeID)
	if err != nil {
		t.Fatalf("释放后重新声明失败: %v", err)
	}
	release2(ctx)
}

// TestEtcdNodeClaimerNilClient 验证未配置 etcd 时退化为空操作（不 panic、可直接释放）。
func TestEtcdNodeClaimerNilClient(t *testing.T) {
	release, err := etcdNodeClaimer{}.Claim(context.Background(), "node-z")
	if err != nil {
		t.Fatalf("空 etcd 声明应无错: %v", err)
	}
	release(context.Background())
}

// 编译期断言：fakeClaimer 满足 nodeClaimer。
var _ nodeClaimer = (*fakeClaimer)(nil)

// TestEtcdNodeClaimReclaimAfterCrash 验证崩溃残留窗口：进程崩溃（无 Shutdown、不释放租约）后，
// 归属键由租约兜底——TTL 到期前同 ID 重启会被自己的残留挡住（ErrNodeConflict），到期后即可重新声明。
// 用短 TTL 避免等 10s；真 etcd 不可达时跳过（按 AGENTS.md 在 10.10.9.36 执行）。
func TestEtcdNodeClaimReclaimAfterCrash(t *testing.T) {
	ctx := context.Background()
	ec, err := etcd.NewClient(etcd.Options{Endpoints: []string{"127.0.0.1:12379"}})
	if err != nil {
		t.Skipf("etcd 不可用: %v", err)
	}
	t.Cleanup(func() { _ = ec.Close() })
	const ttl = 1500 * time.Millisecond
	claimer := etcdNodeClaimer{ec: ec, prefix: "/atlas/actors/it-claim-crash", ttl: ttl}
	nodeID := fmt.Sprintf("it-crash-%d", time.Now().UnixNano())

	// 首次声明后立即取消 ctx：等价于"进程崩溃"——续租停掉、租约**不** Revoke，靠 TTL 回收。
	crashCtx, crash := context.WithTimeout(ctx, 3*time.Second) // 有界：etcd 不可达时快速失败
	if _, _, _, err := claimer.claimOnce(crashCtx, crashCtx, nodeID, ttl); err != nil {
		crash()
		t.Skipf("etcd 不可用（租约声明失败）: %v", err)
	}
	crash()

	// 残留窗口内：同 ID 重新声明必须被拒（否则就是静默顶替）。
	conflictCtx, cancelConflict := context.WithTimeout(ctx, 3*time.Second)
	_, _, _, err = claimer.claimOnce(conflictCtx, conflictCtx, nodeID, ttl)
	cancelConflict()
	if !errors.Is(err, ErrNodeConflict) {
		t.Fatalf("残留窗口内同 ID 声明应报 ErrNodeConflict，实际 %v", err)
	}

	// 租约到期后：可重新声明（等价于 kill -9 后等 TTL 过期即可重启）。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		retryCtx, cancelRetry := context.WithTimeout(ctx, 3*time.Second)
		_, _, _, err := claimer.claimOnce(retryCtx, retryCtx, nodeID, ttl)
		if err == nil {
			cancelRetry() // 停续租；租约留待 TTL 回收（nodeID 唯一，不影响其他用例）
			return
		}
		cancelRetry()
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("租约到期后仍无法重新声明（崩溃残留窗口未收敛）")
}
