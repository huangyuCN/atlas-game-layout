package idgen

import (
	"strings"
	"testing"
)

// TestNewFormat 验证 ID 形态与唯一性。
func TestNewFormat(t *testing.T) {
	id := New("p")
	if !strings.HasPrefix(id, "p-") {
		t.Fatalf("New(\"p\") = %q, 期望 p- 前缀", id)
	}
	if len(id) != 34 { // p- + 32 hex
		t.Fatalf("New(\"p\") 长度 = %d, 期望 34", len(id))
	}
	// 唯一性（10 万次无重复）。
	seen := make(map[string]struct{}, 100000)
	for i := 0; i < 100000; i++ {
		x := Player()
		if _, dup := seen[x]; dup {
			t.Fatalf("ID 重复: %q", x)
		}
		seen[x] = struct{}{}
	}
}

// TestKindPrefixes 验证各类 ID 前缀。
func TestKindPrefixes(t *testing.T) {
	for name, gen := range map[string]func() string{"player": Player, "match": Match, "battle": Battle} {
		id := gen()
		if id == "" {
			t.Fatalf("%s() 返回空", name)
		}
	}
}
