// 直连保活探针（battle.v1.BattleService/Ping）：客户端在无输入期间周期发送，让帧面保持
// 活跃——数据报面的掉线判定靠帧面空闲读超时发现（缺省 offline_timeout/3 = 5s），长时间
// 静默还会让 NAT 映射失效。
//
// 本方法**不刷新任何应用层状态**，因为活跃刷新已由帧引擎在收包时自动完成：
//   - 流式面（KCP/WS）：引擎每轮读循环重设空闲读超时（框架 transport/frame/engine
//     ServeConn 的 SetReadDeadline），收到任何合法帧即刷新，与 op 是什么无关；
//   - 数据报面（KCP/UDP 的 peer 表）：合法 Request 解码后即刷新该 peer 的最近活跃时间
//     （框架 transport/frame/engine 的 touchPeer），空闲驱逐因而不会触发。
//
// 故 Ping 只做一件事：成功返回（Tell）。引擎据此不回帧，也不产生回执载荷；battle 也不在
// 名单/帧号/结算上留下任何痕迹，重复与乱序调用天然幂等。若将来活跃刷新改为应用层记账
// （例如按玩家存最近活跃时间），改动点在本文件，探针的对外契约不变。

package actor

import (
	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas/contrib/actor/core"
)

// Ping 实现 battlev1actor.BattleService：保活探针——只让帧面保持活跃并成功返回
// （Tell，returns Empty 即无回执；RPC 面的 Empty 由生成的接入层桩返回）。
// 身份与寻址都在帧面完成（帧槽票据验票 + 按 battle_id 路由到本 actor），故方法体不读请求、
// 不校验发起者、不写任何对局状态，不动参战名单 / 帧号 / 结算。
func (b *BattleActor) Ping(_ core.ActorContext, _ *battlev1.PingReq) error {
	return nil
}
