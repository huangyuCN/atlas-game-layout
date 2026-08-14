package errors

import (
	stderrors "errors"
	"fmt"
	"testing"
)

// TestNewAndExtract 验证构造与提取往返。
func TestNewAndExtract(t *testing.T) {
	e := New(400, "BAD_PARAM", "参数错误")
	if Code(e) != 400 || Reason(e) != "BAD_PARAM" {
		t.Fatalf("Code/Reason 提取不符合预期: %d/%s", Code(e), Reason(e))
	}
}

// TestWrappedExtract 验证 wrapped 提取。
func TestWrappedExtract(t *testing.T) {
	e := BadRequest("BAD_PARAM", "x")
	w := fmt.Errorf("wrap: %w", e)
	if Code(w) != 400 || Reason(w) != "BAD_PARAM" {
		t.Fatalf("wrapped 提取不符合预期")
	}
}

// TestStdError 验证普通错误映射为 500。
func TestStdError(t *testing.T) {
	if Code(stderrors.New("plain")) != 500 {
		t.Fatal("普通错误应映射为 500")
	}
}
