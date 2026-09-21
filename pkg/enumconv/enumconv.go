// Package enumconv 提供 proto 枚举 → Go 枚举的映射辅助：配置段用映射表声明
// 对应关系，未登记的取值一律报错（配置错误不静默降级为默认值）。
package enumconv

import "fmt"

// Map 按映射表把源枚举（proto 生成，int32 底层类型）转为目标枚举；
// 未登记的取值返回错误，what 用于错误信息中标识配置段（如 "redis 配置形态"）。
func Map[S ~int32, T ~int](src S, table map[S]T, what string) (T, error) {
	if v, ok := table[src]; ok {
		return v, nil
	}
	var zero T
	return zero, fmt.Errorf("enumconv: 未知 %s 枚举值 %d", what, int32(src))
}
