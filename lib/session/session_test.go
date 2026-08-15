package session

import "testing"

// TestNewToken 验证令牌格式与唯一性。
func TestNewToken(t *testing.T) {
	tok, err := NewToken()
	if err != nil {
		t.Fatalf("NewToken() 错误 = %v", err)
	}
	if !Valid(tok) {
		t.Fatalf("NewToken() = %q 未通过校验", tok)
	}
	// 唯一性抽查。
	a, _ := NewToken()
	b, _ := NewToken()
	if a == b {
		t.Fatal("两次生成的令牌重复")
	}
}

// TestValid 验证非法令牌拒绝。
func TestValid(t *testing.T) {
	cases := map[string]bool{
		"":      false,
		"short": false,
		"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz": false, // 非 hex
		"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234": true,  // 64 hex
	}
	for tok, want := range cases {
		if got := Valid(tok); got != want {
			t.Errorf("Valid(%q) = %v, 期望 %v", tok, got, want)
		}
	}
}
