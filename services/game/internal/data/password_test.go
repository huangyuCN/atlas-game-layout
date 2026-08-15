package data

import (
	"strings"
	"testing"
)

// TestHashAndVerify 验证口令摘要的生成与校验。
func TestHashAndVerify(t *testing.T) {
	salt, hash, err := HashPassword("secret-口令")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if salt == "" || hash == "" {
		t.Fatal("盐与摘要不能为空")
	}
	if salt == hash {
		t.Fatal("盐与摘要不应相同")
	}
	if !VerifyPassword("secret-口令", salt, hash) {
		t.Fatal("正确口令校验失败")
	}
	if VerifyPassword("wrong", salt, hash) {
		t.Fatal("错误口令不应通过校验")
	}
	if VerifyPassword("", salt, hash) {
		t.Fatal("空口令不应通过校验")
	}
}

// TestHashPasswordRandomSalt 验证同一口令两次摘要不同（随机盐）。
func TestHashPasswordRandomSalt(t *testing.T) {
	s1, h1, err := HashPassword("same")
	if err != nil {
		t.Fatalf("HashPassword1: %v", err)
	}
	s2, h2, err := HashPassword("same")
	if err != nil {
		t.Fatalf("HashPassword2: %v", err)
	}
	if s1 == s2 || h1 == h2 {
		t.Fatal("随机盐应导致摘要不同")
	}
}

// TestHashPasswordEmpty 验证空口令拒绝。
func TestHashPasswordEmpty(t *testing.T) {
	if _, _, err := HashPassword(""); err == nil {
		t.Fatal("空口令应报错")
	}
}

// TestVerifyTamperedHash 验证篡改摘要/盐无法通过校验。
func TestVerifyTamperedHash(t *testing.T) {
	salt, hash, err := HashPassword("pwd")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if VerifyPassword("pwd", salt+"x", hash) {
		t.Fatal("篡改盐不应通过校验")
	}
	if VerifyPassword("pwd", salt, strings.ToUpper(hash)) {
		t.Fatal("篡改摘要不应通过校验")
	}
}
