// Package testlocator 提供接入层用例共用的「阻塞型目录」替身：监听只有在 Stop 被调用后
// 才会唤醒 Next（形态与 locator/etcd 一致——Stop 关 done → Next 返回 io.EOF）。
//
// 替身刻意不给「只关停止信号」留后门：cancel 里不 Stop 的实现会把 goroutine 永久留在
// Next()（评审 P1-1 的每局泄漏 1 watch + 1 goroutine），用例等它必然超时。
// 本包只被 _test.go 引用，不进任何服务进程的依赖图。
package testlocator

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/huangyuCN/atlas/locator"
)

// WaitTimeout 是等待「监听被停止 / 派生 ctx 被取消」的上限。
const WaitTimeout = 2 * time.Second

// Locator 是只实现 Watch 的目录替身：其余 Locator 方法由 nil 嵌入接口兜底（用例不调用）。
type Locator struct {
	locator.Locator
	// Err 非空时 Watch 直接返回该错误（模拟监听建立失败）。
	Err error

	mu       sync.Mutex
	prefixes []string
	ctxs     []context.Context
	watchers []*Watcher
}

// Watch 记录订阅参数并返回阻塞型监听。
func (l *Locator) Watch(ctx context.Context, opts locator.WatchOptions) (locator.Watcher, error) {
	if l.Err != nil {
		return nil, l.Err
	}
	w := NewWatcher()
	l.mu.Lock()
	l.prefixes = append(l.prefixes, opts.KeyPrefix)
	l.ctxs = append(l.ctxs, ctx)
	l.watchers = append(l.watchers, w)
	l.mu.Unlock()
	return w, nil
}

// Count 返回订阅次数。
func (l *Locator) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.watchers)
}

// Prefix 返回第 i 次订阅的前缀（i 越界返回空串）。
func (l *Locator) Prefix(i int) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if i < 0 || i >= len(l.prefixes) {
		return ""
	}
	return l.prefixes[i]
}

// Ctx 返回第 i 次订阅拿到的 ctx（i 越界返回 nil）。
func (l *Locator) Ctx(i int) context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	if i < 0 || i >= len(l.ctxs) {
		return nil
	}
	return l.ctxs[i]
}

// Watcher 返回第 i 个监听替身（i 越界返回 nil）。
func (l *Locator) Watcher(i int) *Watcher {
	l.mu.Lock()
	defer l.mu.Unlock()
	if i < 0 || i >= len(l.watchers) {
		return nil
	}
	return l.watchers[i]
}

// Watcher 是「只有 Stop 才能唤醒」的监听替身。
type Watcher struct {
	events chan []locator.WatchEvent
	stopCh chan struct{}
	mu     sync.Mutex
	stops  int
}

// NewWatcher 构造阻塞型监听。
func NewWatcher() *Watcher {
	return &Watcher{events: make(chan []locator.WatchEvent, 8), stopCh: make(chan struct{})}
}

// Next 返回排队的变更事件；无事件时阻塞直到 Stop 被调用（此后返回 io.EOF）。
func (w *Watcher) Next() ([]locator.WatchEvent, error) {
	select {
	case evs := <-w.events:
		return evs, nil
	case <-w.stopCh:
		return nil, io.EOF
	}
}

// Stop 关闭监听；重复调用即 panic（与 locator/etcd 同口径：close 已关闭的 channel），
// 故调用方必须保证「恰好一次」。
func (w *Watcher) Stop() error {
	w.mu.Lock()
	w.stops++
	w.mu.Unlock()
	close(w.stopCh)
	return nil
}

// Emit 投递一批变更事件（入队即返回，回调在监听 goroutine 里发生）。
func (w *Watcher) Emit(evs ...locator.WatchEvent) { w.events <- evs }

// Stops 返回 Stop 被调用的次数。
func (w *Watcher) Stops() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stops
}

// Stopped 等待 Stop 被调用；超时返回 false（用例据此点明 goroutine 泄漏）。
func (w *Watcher) Stopped(timeout time.Duration) bool {
	select {
	case <-w.stopCh:
		return true
	case <-time.After(timeout):
		return false
	}
}
