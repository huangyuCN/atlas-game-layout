// Package errors 是游戏模板的统一错误入口：
// 业务错误基于 Atlas 结构化错误（Code/Reason/Metadata），
// 错误码常量由 api/error/v1/errors.proto 经 protoc-gen-atlas-errors 生成
// （M3 里程碑生成，本包预留接线）。
package errors

import (
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// New 构造业务错误（code 为 HTTP 语义码，reason 为业务错误码字符串）。
func New(code int, reason, message string) *atlaserrors.Error {
	return atlaserrors.New(code, reason, message)
}

// Newf 构造格式化业务错误。
func Newf(code int, reason, format string, a ...any) *atlaserrors.Error {
	return atlaserrors.Newf(code, reason, format, a...)
}

// BadRequest 等常用便捷构造（参数校验失败等）。
func BadRequest(reason, message string) *atlaserrors.Error {
	return atlaserrors.BadRequest(reason, message)
}

// Internal 服务内部错误。
func Internal(reason, message string) *atlaserrors.Error {
	return atlaserrors.InternalServer(reason, message)
}

// Code 提取错误的 HTTP 语义码（支持 wrapped）。
func Code(err error) int { return atlaserrors.Code(err) }

// Reason 提取业务错误码（支持 wrapped）。
func Reason(err error) string { return atlaserrors.Reason(err) }
