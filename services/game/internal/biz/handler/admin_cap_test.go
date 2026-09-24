package handler

// admin_cap_test.go 是管理面单次发放上限（R12）的 TDD 场景测试：上限校验必须发生在
// **审计占位之前**（超限即拒单，不留审计、不调底层），dry-run 同样受限，
// 且配置缺省/0 只回退默认值——**不提供「关闭上限」**。

import (
	"context"
	"testing"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
)

// TestAdminGrantCapRejectsOverLimit 超限（含 dry-run）→ AdminGrantCountExceeded、
// 底层零调用、审计零写入。
func TestAdminGrantCapRejectsOverLimit(t *testing.T) {
	const max = 100
	for _, dryRun := range []bool{false, true} {
		f := newAdminFixture(max)
		ac := writeCtx("gm-1", "k-over")
		ac.DryRun = dryRun
		_, err := f.h.GrantItem(context.Background(), grantReq(ac, "p-1", max+1))
		if !errorv1.IsAdminGrantCountExceeded(err) {
			t.Fatalf("dry_run=%v 超限错误 = %v，期望 AdminGrantCountExceeded", dryRun, err)
		}
		if f.state.grantCalls != 0 {
			t.Fatalf("dry_run=%v 超限不得调用底层，实际 %d 次", dryRun, f.state.grantCalls)
		}
		if f.audit.inserts != 0 {
			t.Fatalf("dry_run=%v 超限不得写审计，实际 %d 次", dryRun, f.audit.inserts)
		}
	}
}

// TestAdminGrantCapAllowsLimit 恰好等于上限（count=max）通过并真正发放。
func TestAdminGrantCapAllowsLimit(t *testing.T) {
	const max = 100
	f := newAdminFixture(max)
	rep, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-max"), "p-1", max))
	if err != nil {
		t.Fatalf("count=max 应通过: %v", err)
	}
	if !rep.GetApplied() || f.state.grantCalls != 1 || f.state.grantCount != max {
		t.Fatalf("count=max 发放不符: rep=%+v calls=%d", rep, f.state.grantCalls)
	}
}

// TestAdminGrantCapDefault 配置缺省/0 → 上限回退默认 100（不提供「关闭上限」）。
func TestAdminGrantCapDefault(t *testing.T) {
	if biz.DefaultMaxGrantCount != 100 {
		t.Fatalf("默认上限 = %d，期望 100", biz.DefaultMaxGrantCount)
	}
	if got := biz.NormalizeMaxGrantCount(0); got != biz.DefaultMaxGrantCount {
		t.Fatalf("缺省上限 = %d，期望 %d", got, biz.DefaultMaxGrantCount)
	}
	f := newAdminFixture(0)
	if _, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-over"), "p-1", 101)); !errorv1.IsAdminGrantCountExceeded(err) {
		t.Fatalf("缺省上限下 count=101 应被拒，实际 %v", err)
	}
	if _, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-ok"), "p-1", 100)); err != nil {
		t.Fatalf("缺省上限下 count=100 应通过，实际 %v", err)
	}
}
