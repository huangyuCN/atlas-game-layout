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

// RegisterGatewayHandlers 将统一 handler 注册到各协议 Server（按用途绑定，D6）：
// 业务通道（tcp/ws）注册认证协议，战斗通道（ws/kcp/udp）注册战斗协议。
func RegisterGatewayHandlers(tcpSrv *tcpt.Server, wsSrv *wst.Server, kcpSrv *kcpt.Server, udpSrv *udpt.Server, g *Gateway) error {
	if err := gatewayv1.RegisterGatewayAuthTCPServer(tcpSrv, g); err != nil {
		return fmt.Errorf("server: 注册 TCP 认证协议失败: %w", err)
	}
	if err := gatewayv1.RegisterGatewayAuthWSServer(wsSrv, g); err != nil {
		return fmt.Errorf("server: 注册 WS 认证协议失败: %w", err)
	}
	if err := gatewayv1.RegisterGatewayBattleWSServer(wsSrv, g); err != nil {
		return fmt.Errorf("server: 注册 WS 战斗协议失败: %w", err)
	}
	if err := gatewayv1.RegisterGatewayBattleKCPServer(kcpSrv, g); err != nil {
		return fmt.Errorf("server: 注册 KCP 战斗协议失败: %w", err)
	}
	if err := gatewayv1.RegisterGatewayBattleUDPServer(udpSrv, g); err != nil {
		return fmt.Errorf("server: 注册 UDP 战斗协议失败: %w", err)
	}
	return nil
}
