package server // 把透传引擎注册到业务帧传输：运行时遍历注解路由表，对 access=CLIENT 的
// operation 逐一 Subscribe 通用帧 op handler（注解驱动，新增域 op 零 gateway 代码）。
// handler 本体是框架组件 frameops.Handler.Serve（与 battle 帧面共用），
// 「遍历路由表逐 op 注册」同样与 battle 帧面共用一份实现（pkg/frameroute），
// 本文件只做业务协议两类传输的形态适配。
//
// 战斗帧面（KCP/UDP）不在网关：客户端直连接入层 → battle 帧面。

import (
	"github.com/huangyuCN/atlas-game-layout/pkg/frameroute"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
)

// RegisterRelayTCPServer 把透传路由注册到 TCP 业务通道。
func RegisterRelayTCPServer(s *tcpt.Server, r *Relay) error {
	return frameroute.Register(r.ops, s.Subscribe)
}

// RegisterRelayWSServer 把透传路由注册到 WebSocket 业务通道。
func RegisterRelayWSServer(s *wst.Server, r *Relay) error {
	return frameroute.Register(r.ops, s.Subscribe)
}
