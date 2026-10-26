package server

import (
	"context"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
)

// TestRelayEventUsesGatewayBudget 验证推送/控制回调的预算取自 Gateway 配置值
// （而不是包级常量），且该预算体现在回调 ctx 的 deadline 上。
func TestRelayEventUsesGatewayBudget(t *testing.T) {
	const budget = 37 * time.Millisecond
	g := &Gateway{relayTimeout: budget}
	var remaining time.Duration
	g.relayEvent("test", nil, func(ctx context.Context) ([]string, []byte, bool) {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Error("回调 ctx 缺少 deadline")
			return nil, nil, false
		}
		remaining = time.Until(dl)
		return nil, nil, false
	})
	if remaining <= 0 || remaining > budget {
		t.Fatalf("回调剩余预算 = %v, want (0, %v]", remaining, budget)
	}
}

// TestRelayBudget 验证构造期预算归一：未注入/非正值回落缺省 5s，正值原样保留。
func TestRelayBudget(t *testing.T) {
	if got := relayBudget(0); got != conf.DefaultRelayTimeout {
		t.Fatalf("relayBudget(0) = %v, want %v", got, conf.DefaultRelayTimeout)
	}
	if got := relayBudget(-time.Second); got != conf.DefaultRelayTimeout {
		t.Fatalf("relayBudget(-1s) = %v, want %v", got, conf.DefaultRelayTimeout)
	}
	if got := relayBudget(3 * time.Second); got != 3*time.Second {
		t.Fatalf("relayBudget(3s) = %v, want 3s", got)
	}
}
