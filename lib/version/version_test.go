package version

import (
	"strings"
	"testing"
)

// TestDefaults 验证未注入时的缺省值（-ldflags 注入只发生在构建期，单测看到缺省值）。
func TestDefaults(t *testing.T) {
	if Version != "dev" || Commit != "unknown" || BuildTime != "unknown" {
		t.Fatalf("缺省构建信息不符: version=%q commit=%q buildTime=%q", Version, Commit, BuildTime)
	}
}

// TestString 验证摘要包含版本/提交/构建时间三项。
func TestString(t *testing.T) {
	got := String()
	for _, want := range []string{Version, Commit, BuildTime} {
		if !strings.Contains(got, want) {
			t.Fatalf("String() = %q, 期望包含 %q", got, want)
		}
	}
}
