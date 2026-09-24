package redis

import (
	"errors"
	"testing"

	"github.com/huangyuCN/atlas/namespace"
)

// mustDerive 派生测试用命名空间（夹具里非法值直接失败）。
func mustDerive(t *testing.T, ns string) namespace.Derived {
	t.Helper()
	derived, err := namespace.Derive(ns)
	if err != nil {
		t.Fatalf("namespace.Derive(%q): %v", ns, err)
	}
	return derived
}

// TestKeys 验证 redis 键按命名空间隔离：同 ID 在不同命名空间下键不同（共用同一 redis 时不串台），
// 键形态保持 atlas:<ns>:<域>:<id>。
func TestKeys(t *testing.T) {
	def, err := NewKeys(mustDerive(t, "default"))
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	if got := def.GatewayRoute("p1"); got != "atlas:default:gw:p1" {
		t.Fatalf("GatewayRoute = %q", got)
	}
	if got := def.PlayerSnapshot("p1"); got != "atlas:default:player:p1" {
		t.Fatalf("PlayerSnapshot = %q", got)
	}
	if got := def.PlayerSession("p1"); got != "atlas:default:session:p1" {
		t.Fatalf("PlayerSession = %q", got)
	}
	if got := def.MatchPlayerTicket("p1"); got != "atlas:default:match:player:p1" {
		t.Fatalf("MatchPlayerTicket = %q", got)
	}
	if got := def.MatchmakerPrefix(); got != "atlas:default:matchmaker:" {
		t.Fatalf("MatchmakerPrefix = %q", got)
	}
	if got := def.MatchParty("party-1"); got != "atlas:default:match:party:party-1" {
		t.Fatalf("MatchParty = %q", got)
	}

	prod, err := NewKeys(mustDerive(t, "prod"))
	if err != nil {
		t.Fatalf("NewKeys: %v", err)
	}
	if prod.GatewayRoute("p1") == def.GatewayRoute("p1") {
		t.Fatal("不同命名空间的路由键不应相同（会串台）")
	}
	if got := prod.GatewayRoute("p1"); got != "atlas:prod:gw:p1" {
		t.Fatalf("prod GatewayRoute = %q", got)
	}
}

// TestNewKeysRequiresNamespace 验证零值 Derived 即报错（R9：不回落 consts.EnvDefault）。
func TestNewKeysRequiresNamespace(t *testing.T) {
	if _, err := NewKeys(namespace.Derived{}); !errors.Is(err, namespace.ErrInvalid) {
		t.Fatalf("零值 Derived 应返回 namespace.ErrInvalid，实际 %v", err)
	}
}
