package version

import (
	"fmt"
	"strconv"
	"strings"
)

// Compare 比较两个语义化版本（major.minor.patch，可带 -预发布 后缀）：
// 返回 -1 / 0 / 1；任一侧无法解析即返回错误（调用方按失败即不满足处理）。
//
// 语义：段数不足按 0 补齐（"1.2" == "1.2.0"）；预发布版本低于同号正式版
// （"1.2.0-rc.1" < "1.2.0"）；构建元数据（+build）不参与比较。
func Compare(a, b string) (int, error) {
	an, apre, err := parse(a)
	if err != nil {
		return 0, err
	}
	bn, bpre, err := parse(b)
	if err != nil {
		return 0, err
	}
	for i := 0; i < 3; i++ {
		switch {
		case an[i] < bn[i]:
			return -1, nil
		case an[i] > bn[i]:
			return 1, nil
		}
	}
	switch {
	case apre == bpre:
		return 0, nil
	case apre == "":
		return 1, nil // 正式版高于预发布版
	case bpre == "":
		return -1, nil
	default:
		return strings.Compare(apre, bpre), nil
	}
}

// parse 解析版本串为三段数字 + 预发布标识（空串表示正式版）。
func parse(v string) ([3]int, string, error) {
	var nums [3]int
	s := strings.TrimSpace(v)
	if s == "" {
		return nums, "", fmt.Errorf("version: 版本为空")
	}
	if i := strings.IndexByte(s, '+'); i >= 0 { // 构建元数据不参与比较
		s = s[:i]
	}
	pre := ""
	if i := strings.IndexByte(s, '-'); i >= 0 {
		pre = s[i+1:]
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) > 3 {
		return nums, "", fmt.Errorf("version: 版本段过多 %q", v)
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nums, "", fmt.Errorf("version: 非法版本段 %q（%q）", p, v)
		}
		nums[i] = n
	}
	return nums, pre, nil
}
