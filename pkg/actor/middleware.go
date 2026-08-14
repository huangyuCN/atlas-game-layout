// Package actor 默认中间件链：logging / recovery。
// Ask 超时由调用方 ctx 控制（cluster 传输携带 ctx deadline），不设默认中间件超时。
package actor

import (
	"fmt"

	"github.com/huangyuCN/atlas/contrib/actor/core"
	atlaslog "github.com/huangyuCN/atlas/log"
)

// LoggingMiddleware 是默认日志中间件：按 actor 类型/PID/消息类型打点。
func LoggingMiddleware(next core.TellHandler) core.TellHandler {
	return func(ctx core.ActorContext, msg any) error {
		if ctx != nil {
			atlaslog.Debugf("actor tell: type=%s uid=%s msg=%T",
				ctx.Self().Type(), ctx.Self().UID(), msg)
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

// DefaultTellChain 返回默认 Tell 中间件链（m[0] 最外层：logging → recovery）。
func DefaultTellChain() []core.TellMiddleware {
	return []core.TellMiddleware{LoggingMiddleware, RecoveryMiddleware}
}

// DefaultAskChain 返回默认 Ask 中间件链（m[0] 最外层：recovery）。
func DefaultAskChain() []core.AskMiddleware {
	return []core.AskMiddleware{AskRecoveryMiddleware}
}
