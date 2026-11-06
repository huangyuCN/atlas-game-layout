// Package actor 默认中间件链：logging / recovery。
// Ask 超时由调用方 ctx 控制（cluster 传输携带 ctx deadline），不设默认中间件超时。
package actor

import (
	"fmt"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"

	"github.com/huangyuCN/atlas/contrib/actor/core"
	atlaslog "github.com/huangyuCN/atlas/log"
)

// LoggingMiddleware 是默认日志中间件：按 actor 类型/PID/消息类型打点。
// 用 ctx 形态（atlaslog.Ctx）：actor 处理上下文携带 span 时，日志行自动
// 关联 trace_id/span_id（Loki 日志 ↔ Tempo 链路互跳的基础）；带客户端
// 请求 ID 时记录 request_id（与客户端 SDK 调试日志一一对应）。
func LoggingMiddleware(next core.TellHandler) core.TellHandler {
	return func(ctx core.ActorContext, msg any) error {
		// 热路径：debug 未开启就立刻放行。原实现无条件做 Header 查询 + fmt.Sprintf("%T") +
		// atlaslog.Ctx（WithContext），即使 debug 关闭也照付——实测每次调用 5 次分配，
		// 4,096 流下该中间件占 battle CPU 8%（约 0.33 核/进程）。字段只在真要输出时才组。
		if ctx == nil || !atlaslog.GetLogger().IsLevelEnabled(atlaslog.LevelDebug) {
			return next(ctx, msg)
		}
		if ctx != nil {
			if id, ok := ctx.Header(opcall.MetadataRequestID); ok {
				atlaslog.Ctx(ctx.Context()).Debug("actor tell",
					"type", ctx.Self().Type(), "uid", ctx.Self().UID(),
					"msg", fmt.Sprintf("%T", msg), "request_id", id)
			} else {
				atlaslog.Ctx(ctx.Context()).Debug("actor tell",
					"type", ctx.Self().Type(), "uid", ctx.Self().UID(), "msg", fmt.Sprintf("%T", msg))
			}
		}
		return next(ctx, msg)
	}
}

// RecoveryMiddleware 是 Tell panic 保护中间件：消息处理 panic 转错误，不击穿 cell。
func RecoveryMiddleware(next core.TellHandler) core.TellHandler {
	return func(ctx core.ActorContext, msg any) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("actor: panic 恢复: %v", r)
				atlaslog.Errorf("actor panic: msg=%T panic=%v", msg, r)
			}
		}()
		return next(ctx, msg)
	}
}

// AskRecoveryMiddleware 是 Ask panic 保护中间件。
func AskRecoveryMiddleware(next core.AskHandler) core.AskHandler {
	return func(ctx core.ActorContext, req any) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("actor: panic 恢复: %v", r)
				atlaslog.Errorf("actor panic: req=%T panic=%v", req, r)
			}
		}()
		return next(ctx, req)
	}
}

// AskLoggingMiddleware 是默认 Ask 日志中间件：按 actor 类型/PID/请求类型打点
// （Debug 级，与 Tell 的 LoggingMiddleware 对称；错误请求升 Warn 记录原因）。
// 用 ctx 形态：日志自动关联 span 的 trace_id/span_id（链路追踪开启时）；
// 带客户端请求 ID 时记录 request_id（与客户端 SDK 调试日志一一对应）。
func AskLoggingMiddleware(next core.AskHandler) core.AskHandler {
	return func(ctx core.ActorContext, req any) (any, error) {
		if ctx == nil {
			return next(ctx, req)
		}
		kv := []any{
			"type", ctx.Self().Type(), "uid", ctx.Self().UID(), "req", fmt.Sprintf("%T", req),
		}
		if id, ok := ctx.Header(opcall.MetadataRequestID); ok {
			kv = append(kv, "request_id", id)
		}
		atlaslog.Ctx(ctx.Context()).Debug("actor ask", kv...)
		resp, err := next(ctx, req)
		if err != nil {
			atlaslog.Ctx(ctx.Context()).Warn("actor ask failed", append(kv, "err", err)...)
		}
		return resp, err
	}
}

// DefaultTellChain 返回默认 Tell 中间件链（m[0] 最外层：logging → recovery）。
func DefaultTellChain() []core.TellMiddleware {
	return []core.TellMiddleware{LoggingMiddleware, RecoveryMiddleware}
}

// DefaultAskChain 返回默认 Ask 中间件链（m[0] 最外层：logging → recovery）。
// Ask 与 Tell 对称：日志打点进链路（Debug 级），panic 保护兜底。
func DefaultAskChain() []core.AskMiddleware {
	return []core.AskMiddleware{AskLoggingMiddleware, AskRecoveryMiddleware}
}
