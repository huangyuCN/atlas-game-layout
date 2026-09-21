package enumconv

import (
	"strings"
	"testing"
)

// TestMap 验证命中映射与未登记取值的报错信息。
func TestMap(t *testing.T) {
	type src int32
	type dst int
	table := map[src]dst{1: 10, 2: 20}

	got, err := Map(src(2), table, "演示枚举")
	if err != nil || got != 20 {
		t.Fatalf("Map(2) = %v/%v, 期望 20/nil", got, err)
	}

	got, err = Map(src(9), table, "演示枚举")
	if err == nil {
		t.Fatal("未登记取值应报错")
	}
	if got != 0 {
		t.Fatalf("报错时应返回零值，实际 %v", got)
	}
	if !strings.Contains(err.Error(), "演示枚举") || !strings.Contains(err.Error(), "9") {
		t.Fatalf("错误信息应含配置段名与取值: %v", err)
	}
}
