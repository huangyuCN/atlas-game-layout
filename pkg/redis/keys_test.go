package redis

import "testing"

// TestKeys 验证 redis 键按命名空间隔离：同 ID 在不同命名空间下键不同（共用同一 redis 时不串台），
// 空命名空间回落 default；键形态保持 atlas:<ns>:<域>:<id>。
func TestKeys(t *testing.T) {
	def := NewKeys("")
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

	prod := NewKeys("prod")
	if prod.GatewayRoute("p1") == def.GatewayRoute("p1") {
		t.Fatal("不同命名空间的路由键不应相同（会串台）")
	}
	if got := prod.GatewayRoute("p1"); got != "atlas:prod:gw:p1" {
		t.Fatalf("prod GatewayRoute = %q", got)
	}
}
