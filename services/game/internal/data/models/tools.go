//go:build tools

// Package models 的工具依赖声明（tools.go 模式）：
// 保留 cow 生成器依赖，供 go generate 使用（不参与业务编译）。
package models

import (
	_ "github.com/huangyuCN/cow/cmd/undoproxy-gen"
)
