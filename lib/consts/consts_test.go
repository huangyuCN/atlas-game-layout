package consts

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

// TestTopics 验证业务 topic 的命名空间化：同一事件在不同命名空间下落在不同 subject，
// 前缀取自 namespace.Derive（唯一派生点），键形态保持 atlas.<ns>.*。
func TestTopics(t *testing.T) {
	def, err := NewTopics(mustDerive(t, "default"))
	if err != nil {
		t.Fatalf("NewTopics: %v", err)
	}
	if def.Namespace() != "default" {
		t.Fatalf("命名空间应为 default，实际 %q", def.Namespace())
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

	prod, err := NewTopics(mustDerive(t, "prod"))
	if err != nil {
		t.Fatalf("NewTopics: %v", err)
	}
	if got := prod.Push("p1"); got != "atlas.prod.push.p1" {
		t.Fatalf("prod Push = %q", got)
	}
	if prod.Push("p1") == def.Push("p1") {
		t.Fatal("不同命名空间的推送 subject 不应相同（会串台）")
	}
}

// TestNewTopicsRequiresNamespace 验证零值 Derived 即报错（R9：不回落 EnvDefault）。
func TestNewTopicsRequiresNamespace(t *testing.T) {
	if _, err := NewTopics(namespace.Derived{}); !errors.Is(err, namespace.ErrInvalid) {
		t.Fatalf("零值 Derived 应返回 namespace.ErrInvalid，实际 %v", err)
	}
}
