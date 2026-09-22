package conf

// 客户端接入协议的启用判定只此一份：不配该节 = 该协议不启用。
// 装配层（internal/app）用它决定是否进启停组，server 层用它决定是否注册 handler——
// 两处各写一遍 cfg.GetServer().GetXxx() != nil 会在启用语义变化时漏改一处。

// TCPEnabled 报告是否启用 TCP 接入通道。
func (b *Bootstrap) TCPEnabled() bool { return b.GetServer().GetTcp() != nil }

// WebSocketEnabled 报告是否启用 WebSocket 接入通道。
func (b *Bootstrap) WebSocketEnabled() bool { return b.GetServer().GetWebsocket() != nil }

// KCPEnabled 报告是否启用 KCP 接入通道。
func (b *Bootstrap) KCPEnabled() bool { return b.GetServer().GetKcp() != nil }

// UDPEnabled 报告是否启用 UDP 接入通道。
func (b *Bootstrap) UDPEnabled() bool { return b.GetServer().GetUdp() != nil }
