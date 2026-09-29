package server

import (
	"context"
	"sync"

	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
)

// fakeTransport 是测试用流式传输上下文（模拟 TCP/WS/KCP 请求侧环境：连接 ID + 帧头），
// 供会话接口直调与透传测试构造身份来源（连接绑定 / 帧会话槽）。
type fakeTransport struct {
	kind transport.Kind // 模拟的传输种类（决定回写 pusher 的映射）
	op   string         // 模拟的请求 operation
	conn uint64         // 模拟的连接 ID（connFrom 按 connID 寻址）
	hdr  fakeHeader     // 模拟的帧请求头（帧会话槽等）
}

// Kind 返回模拟的传输种类。
func (f *fakeTransport) Kind() transport.Kind { return f.kind }

// Endpoint 返回测试端点占位。
func (f *fakeTransport) Endpoint() string { return "test://gateway" }

// Operation 返回模拟的请求 operation。
func (f *fakeTransport) Operation() string { return f.op }

// RequestHeader 返回帧请求头。
func (f *fakeTransport) RequestHeader() transport.Header { return f.hdr }

// ReplyHeader 返回 nil（帧传输无独立响应头语义）。
func (f *fakeTransport) ReplyHeader() transport.Header { return nil }

// ConnID 返回模拟的连接 ID（服务端推送寻址）。
func (f *fakeTransport) ConnID() uint64 { return f.conn }

// fakeHeader 是测试用 transport.Header（map 存取，模拟帧头载体）。
type fakeHeader map[string]string

// Get 读取键值。
func (h fakeHeader) Get(key string) string { return h[key] }

// Set 写入键值。
func (h fakeHeader) Set(key, value string) { h[key] = value }

// Add 追加键值（单值形态与 Set 同义）。
func (h fakeHeader) Add(key, value string) { h[key] = value }

// Delete 删除键。
func (h fakeHeader) Delete(key string) { delete(h, key) }

// Keys 返回全部键名。
func (h fakeHeader) Keys() []string {
	out := make([]string, 0, len(h))
	for k := range h {
		out = append(out, k)
	}
	return out
}

// Values 返回键的全部取值（单值形态）。
func (h fakeHeader) Values(key string) []string {
	if v, ok := h[key]; ok {
		return []string{v}
	}
	return nil
}

// fakePush 记录一次服务端下行推送。
type fakePush struct {
	connID  uint64 // 目标连接 ID
	op      string // 推送 operation（消息完整名）
	payload []byte // 推送载荷
}

// fakePusher 实现 pushServer：记录 PushRaw 调用（会话回写与挤下线断言用，
// 不写真实网络）。
type fakePusher struct {
	mu     sync.Mutex
	pushes []fakePush
}

// PushRaw 记录一次推送。
func (p *fakePusher) PushRaw(connID uint64, operation string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pushes = append(p.pushes, fakePush{
		connID:  connID,
		op:      operation,
		payload: append([]byte(nil), payload...),
	})
	return nil
}

// snapshot 返回已记录推送的副本（并发安全）。
func (p *fakePusher) snapshot() []fakePush {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]fakePush(nil), p.pushes...)
}

// connCtx 构造携带流式连接上下文的测试请求 ctx：kind 标识传输种类（TCP 业务 /
// KCP 战斗），frameToken 非空时注入帧会话槽（UDP/KCP 每帧验证身份的形态）。
func connCtx(kind transport.Kind, op string, connID uint64, frameToken string) context.Context {
	hdr := fakeHeader{}
	if frameToken != "" {
		hdr[frame.RequestHeaderKeySession] = frameToken
	}
	return transport.NewServerContext(context.Background(), &fakeTransport{
		kind: kind, op: op, conn: connID, hdr: hdr,
	})
}
