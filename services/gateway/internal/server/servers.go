package server

import (
	"fmt"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
)

// NewTCPServer 构造 TCP 业务通道服务端。
func NewTCPServer(cfg *conf.Bootstrap) (*tcpt.Server, error) {
	addr := "0.0.0.0:0"
	if n := cfg.GetTcp(); n != nil && n.GetAddr() != "" {
		addr = n.GetAddr()
	}
	srv, err := tcpt.NewServer(tcpt.WithAddress(addr))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 TCP 服务端失败: %w", err)
	}
	return srv, nil
}

// NewWSServer 构造 WebSocket 服务端（单通道形态同时承载业务与战斗协议）。
func NewWSServer(cfg *conf.Bootstrap) (*wst.Server, error) {
	addr := "0.0.0.0:0"
	if n := cfg.GetWebsocket(); n != nil && n.GetAddr() != "" {
		addr = n.GetAddr()
	}
	srv, err := wst.NewServer(wst.WithAddress(addr))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 WebSocket 服务端失败: %w", err)
	}
	return srv, nil
}

// NewKCPServer 构造 KCP 战斗通道服务端。
func NewKCPServer(cfg *conf.Bootstrap) (*kcpt.Server, error) {
	addr := "0.0.0.0:0"
	if n := cfg.GetKcp(); n != nil && n.GetAddr() != "" {
		addr = n.GetAddr()
	}
	srv, err := kcpt.NewServer(kcpt.WithAddress(addr))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 KCP 服务端失败: %w", err)
	}
	return srv, nil
}

// NewUDPServer 构造 UDP 战斗通道服务端。
func NewUDPServer(cfg *conf.Bootstrap) (*udpt.Server, error) {
	addr := "0.0.0.0:0"
	if n := cfg.GetUdp(); n != nil && n.GetAddr() != "" {
		addr = n.GetAddr()
	}
	srv, err := udpt.NewServer(udpt.WithAddress(addr))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 UDP 服务端失败: %w", err)
	}
	return srv, nil
}

// RegisterGatewayHandlers 将会话生命周期与透传引擎注册到各协议 Server：
// 会话接口（gateway.v1.Session，Gateway 自留）+ 透传路由表（域 service 的
// access=CLIENT op，运行时注册，注解驱动）；四传输同构（连接即会话，UDP/KCP 按帧槽验证）。
func RegisterGatewayHandlers(tcpSrv *tcpt.Server, wsSrv *wst.Server, kcpSrv *kcpt.Server, udpSrv *udpt.Server, g *Gateway) error {
	if err := gatewayv1.RegisterSessionTCPServer(tcpSrv, g); err != nil {
		return fmt.Errorf("server: 注册 TCP 会话协议失败: %w", err)
	}
	if err := gatewayv1.RegisterSessionWSServer(wsSrv, g); err != nil {
		return fmt.Errorf("server: 注册 WS 会话协议失败: %w", err)
	}
	if err := gatewayv1.RegisterSessionKCPServer(kcpSrv, g); err != nil {
		return fmt.Errorf("server: 注册 KCP 会话协议失败: %w", err)
	}
	if err := gatewayv1.RegisterSessionUDPServer(udpSrv, g); err != nil {
		return fmt.Errorf("server: 注册 UDP 会话协议失败: %w", err)
	}
	if err := RegisterRelayTCPServer(tcpSrv, g.Relay()); err != nil {
		return fmt.Errorf("server: 注册 TCP 透传路由失败: %w", err)
	}
	if err := RegisterRelayWSServer(wsSrv, g.Relay()); err != nil {
		return fmt.Errorf("server: 注册 WS 透传路由失败: %w", err)
	}
	if err := RegisterRelayKCPServer(kcpSrv, g.Relay()); err != nil {
		return fmt.Errorf("server: 注册 KCP 透传路由失败: %w", err)
	}
	if err := RegisterRelayUDPServer(udpSrv, g.Relay()); err != nil {
		return fmt.Errorf("server: 注册 UDP 透传路由失败: %w", err)
	}
	return nil
}
