// Package nats 提供游戏模板的 NATS 连接与消息封装：
// 事件总线（atlas.event.*）、玩家推送（atlas.push.*）、gateway 控制通道
// （atlas.gw.*）的主题约定见 lib/consts。
package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// Options 是 NATS 连接选项。
type Options struct {
	// URL 是 NATS 地址（nats://host:port）。
	URL string
	// Name 是连接名（可观测与排查用）。
	Name string
}

// Connect 建立 NATS 连接（同步连接，失败即报错）。
func Connect(opts Options) (*nats.Conn, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("nats: url 不能为空")
	}
	co := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(200 * time.Millisecond),
		nats.Timeout(5 * time.Second),
	}
	if opts.Name != "" {
		co = append(co, nats.Name(opts.Name))
	}
	nc, err := nats.Connect(opts.URL, co...)
	if err != nil {
		return nil, fmt.Errorf("nats: 连接 %s 失败: %w", opts.URL, err)
	}
	return nc, nil
}

// Publish 发布消息到 subject 并等待刷盘确认。
func Publish(ctx context.Context, nc *nats.Conn, subject string, data []byte) error {
	if nc == nil {
		return fmt.Errorf("nats: 连接不能为空")
	}
	if err := nc.Publish(subject, data); err != nil {
		return fmt.Errorf("nats: 发布 %s 失败: %w", subject, err)
	}
	// FlushWithContext 要求 ctx 带 deadline，这里统一包默认超时。
	flushCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return nc.FlushWithContext(flushCtx)
}

// Subscribe 订阅 subject，消息回调（同步 handler 在独立 goroutine 执行）。
func Subscribe(nc *nats.Conn, subject string, cb func(subject string, data []byte)) (*nats.Subscription, error) {
	if nc == nil {
		return nil, fmt.Errorf("nats: 连接不能为空")
	}
	if cb == nil {
		return nil, fmt.Errorf("nats: 回调不能为空")
	}
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		cb(m.Subject, m.Data)
	})
	if err != nil {
		return nil, fmt.Errorf("nats: 订阅 %s 失败: %w", subject, err)
	}
	return sub, nil
}

// SubscribeWildcard 订阅通配主题（如 atlas.push.>），用于 gateway 广播过滤模式。
func SubscribeWildcard(nc *nats.Conn, subject string, cb func(subject string, data []byte)) (*nats.Subscription, error) {
	return Subscribe(nc, subject, cb)
}
