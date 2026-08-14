package consts

import "testing"

// TestTopicHelpers 验证主题拼接约定。
func TestTopicHelpers(t *testing.T) {
	if got := PushTopic("p1"); got != "atlas.push.p1" {
		t.Errorf("PushTopic = %q, 期望 atlas.push.p1", got)
	}
	if got := EventTopic("match_done"); got != "atlas.event.match_done" {
		t.Errorf("EventTopic = %q, 期望 atlas.event.match_done", got)
	}
	if got := GatewayTopic("gw-1"); got != "atlas.gw.gw-1" {
		t.Errorf("GatewayTopic = %q, 期望 atlas.gw.gw-1", got)
	}
}
