package conf

// 客户端接入协议的启用判定只此一份：节点缺失、或必需参数 addr 为空 = 该协议不启用
// （唯一缺省约定见 docs/config.md；「节点缺失 ≡ 其全部参数为空」，故两种写法同一结果）。
// 装配层（internal/app）用它决定是否进启停组，server 层用它决定是否注册 handler——
// 两处各写一遍判空会在启用语义变化时漏改一处。
//
// 只覆盖业务协议（tcp/websocket）：战斗帧面（kcp/udp）自阶段 3 批次 5 起不在网关。

// TCPEnabled 报告是否启用 TCP 接入通道（节点在且 addr 非空）。
func (b *Bootstrap) TCPEnabled() bool { return b.GetServer().GetTcp().GetAddr() != "" }

// WebSocketEnabled 报告是否启用 WebSocket 接入通道（节点在且 addr 非空）。
func (b *Bootstrap) WebSocketEnabled() bool { return b.GetServer().GetWebsocket().GetAddr() != "" }
