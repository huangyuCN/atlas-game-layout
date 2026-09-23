package consts

import "testing"

// TestTopics 验证业务 topic 的命名空间化：同一事件在不同命名空间下落在不同 subject，
// 空命名空间回落 default（两套共用同一 NATS 的部署靠它隔离，见 Topics 文档）。
func TestTopics(t *testing.T) {
	def := NewTopics("")
	if def.Namespace() != EnvDefault {
		t.Fatalf("空命名空间应回落 %q，实际 %q", EnvDefault, def.Namespace())
	}
	if got := def.Push("p1"); got != "atlas.default.push.p1" {
		t.Fatalf("Push = %q", got)
	}
	if got := def.PushWildcard(); got != "atlas.default.push.>" {
		t.Fatalf("PushWildcard = %q", got)
	}
	if got := def.Event("match_done"); got != "atlas.default.event.match_done" {
		t.Fatalf("Event = %q", got)
	}
	if got := def.MatchStarted(); got != "atlas.default.event.match.started" {
		t.Fatalf("MatchStarted = %q", got)
	}
	if got := def.GatewayControl("gw-1"); got != "atlas.default.gw.gw-1" {
		t.Fatalf("GatewayControl = %q", got)
	}

	prod := NewTopics("prod")
	if got := prod.Push("p1"); got != "atlas.prod.push.p1" {
		t.Fatalf("prod Push = %q", got)
	}
	if prod.Push("p1") == def.Push("p1") {
		t.Fatal("不同命名空间的推送 subject 不应相同（会串台）")
	}
}
