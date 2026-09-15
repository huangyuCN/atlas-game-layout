package server // 把透传引擎注册到四种帧传输：运行时遍历注解路由表，
// 对 access=CLIENT 的 operation 逐一 Subscribe 通用透传 handler（注解驱动，
// 新增域 op 零 gateway 代码）。

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/transport/frame/engine"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	"google.golang.org/protobuf/proto"
)

// RegisterRelayTCPServer 把透传路由注册到 TCP 业务通道。
func RegisterRelayTCPServer(s *tcpt.Server, r *Relay) error {
	return r.Each(func(e relay.RouteEntry) error {
		return s.Subscribe(e.Operation, relayHandler(r, e))
	})
}

// RegisterRelayWSServer 把透传路由注册到 WebSocket 通道。
func RegisterRelayWSServer(s *wst.Server, r *Relay) error {
	return r.Each(func(e relay.RouteEntry) error {
		return s.Subscribe(e.Operation, relayHandler(r, e))
	})
}

// RegisterRelayKCPServer 把透传路由注册到 KCP 战斗通道。
func RegisterRelayKCPServer(s *kcpt.Server, r *Relay) error {
	return r.Each(func(e relay.RouteEntry) error {
		return s.Subscribe(e.Operation, relayHandler(r, e))
	})
}

// RegisterRelayUDPServer 把透传路由注册到 UDP 战斗通道。
func RegisterRelayUDPServer(s *udpt.Server, r *Relay) error {
	return r.Each(func(e relay.RouteEntry) error {
		return s.Subscribe(e.Operation, relayHandler(r, e))
	})
}

// relayHandler 是通用透传桩：解码请求 → Relay.Forward → 编码回执。
// 业务错误原样上抛（传输引擎统一 EncodeReply 落错误包络）。
func relayHandler(r *Relay, e relay.RouteEntry) engine.MsgHandler {
	return func(ctx context.Context, dec engine.Decoder, enc engine.Encoder) ([]byte, error) {
		req, err := decodeEntry(e, dec)
		if err != nil {
			return nil, err
		}
		resp, err := r.Forward(ctx, e, req)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, nil
		}
		return enc(resp)
	}
}

// decodeEntry 按路由条目构造请求消息并解码（dec 已按请求帧载荷编码分派 codec）。
func decodeEntry(e relay.RouteEntry, dec engine.Decoder) (proto.Message, error) {
	req := e.NewRequest()
	if err := dec(req); err != nil {
		return nil, fmt.Errorf("relay: 请求解码失败 operation=%s: %w", e.Operation, err)
	}
	return req, nil
}
