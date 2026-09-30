package server

import (
	"fmt"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
)

// NewTCPServer 构造 TCP 业务通道服务端（启动参数全部来自 server.tcp 配置）。
func NewTCPServer(cfg *conf.Bootstrap) (*tcpt.Server, error) {
	return serverutil.TCPServer(cfg.GetServer().GetTcp())
}

// NewWSServer 构造 WebSocket 业务通道服务端（启动参数全部来自 server.websocket 配置）。
// 战斗帧不走本服务：客户端凭 battle_ticket 直连接入层 → battle 帧面。
func NewWSServer(cfg *conf.Bootstrap) (*wst.Server, error) {
	return serverutil.WSServer(cfg.GetServer().GetWebsocket())
}

// RegisterGatewayHandlers 将会话生命周期与透传引擎注册到业务协议 Server：
// 会话接口（gateway.v1.Session，Gateway 自留）以 meteredSession 打点装饰注册 +
// 透传路由表（域 service 的 access=CLIENT op，运行时注册，注解驱动）；
// 协议可选：仅对配置声明了的协议注册（conf 协议节 nil = 该协议不启用，
// Server 不注册 handler 也不进启停组——对外不可用，模板可按需裁剪）。
//
// 战斗帧面（KCP/UDP）与战斗 op 自阶段 3 批次 5 起不在此注册：网关只承载业务 op，
// 经网关发战斗 op 会在帧引擎层明确失败（TRANSPORT_NOT_FOUND，不是静默丢弃）。
func RegisterGatewayHandlers(cfg *conf.Bootstrap, tcpSrv *tcpt.Server, wsSrv *wst.Server, g *Gateway) error {
	// 会话接口统一打点装饰（自留接口与透传共用 gateway_requests_total）。
	sess := newMeteredSession(g, g.meter)
	if cfg.TCPEnabled() {
		if err := gatewayv1.RegisterSessionTCPServer(tcpSrv, sess); err != nil {
			return fmt.Errorf("server: 注册 TCP 会话协议失败: %w", err)
		}
		if err := RegisterRelayTCPServer(tcpSrv, g.Relay()); err != nil {
			return fmt.Errorf("server: 注册 TCP 透传路由失败: %w", err)
		}
	}
	if cfg.WebSocketEnabled() {
		if err := gatewayv1.RegisterSessionWSServer(wsSrv, sess); err != nil {
			return fmt.Errorf("server: 注册 WS 会话协议失败: %w", err)
		}
		if err := RegisterRelayWSServer(wsSrv, g.Relay()); err != nil {
			return fmt.Errorf("server: 注册 WS 透传路由失败: %w", err)
		}
	}
	return nil
}
