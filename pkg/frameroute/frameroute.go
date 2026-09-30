// Package frameroute 把帧 op 路由逐条注册到一类帧传输：路由表是 atlas.route.v1 注解生成物的
// 唯一事实源，网关透传引擎与 battle 帧面共用本实现——避免「遍历路由表逐 op 注册」出现两份
// 而登记口径漂移（漏注册的 op 表现为请求期稳定 reason，而不是装配期失败）。
package frameroute

import (
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// Registrar 是一类帧传输的注册接缝（tcp/kcp/udp/ws 的 Server.Subscribe 均满足此形）。
type Registrar func(operation string, handler engine.MsgHandler) error

// Register 把 ops 上 access=CLIENT 的路由条目逐条注册到一类帧传输（每个 op 一个 handler，
// 与注解一一对应）；registrar 返回错误即中断。handler 本体是框架组件 frameops.Handler.Serve
// （身份解析与投递端口由消费方在装配期注入），本包只管「哪些 op 要注册」这一件事。
func Register(ops *frameops.Handler, registrar Registrar) error {
	return ops.Each(func(entry relay.RouteEntry) error {
		return registrar(entry.Operation, ops.Serve(entry))
	})
}
