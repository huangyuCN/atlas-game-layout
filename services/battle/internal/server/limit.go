// 「单包 ≤16 KiB」契约的 battle 侧落点（P0-4）。
//
// 契约为**两侧同口径**：接入层（contrib/edge）数据报面回程按整个数据报计上限（16 KiB），
// 超出的数据报被直接丢弃（客户端只会等到调用超时——下行不得出现静默黑洞）；battle 帧面
// 因此必须保证任何一条回执都装得进一个包，装不进的回执在**交给引擎之前**就以可判定 reason
// 拒绝，并按 operation 计数（与接入层 edge_downlink_dropped_total{reason=oversize} 呼应：
// battle 侧拦住、接入层兜底）。
//
// 本文件三个落点：
//  1. frameBodySize / limitBody：帧面装配期把 max_body_size 收敛到契约值（未配置即补默认，
//     显式超限即启动失败——配大了不会生效，只会让大回执在接入层被丢弃）；
//  2. LimitReply：handler 回执超过上限即替换为可判定拒绝（TRANSPORT_DOWNLINK_FAILED）；
//  3. 配置注释（services/battle/configs/config.yaml 的 server.kcp/udp/websocket.max_body_size）。

package server

import (
	"context"
	"fmt"

	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/transport/frame"
	"github.com/huangyuCN/atlas/transport/frame/engine"
	"google.golang.org/protobuf/proto"
)

// MaxFrameBodySize 是 battle 帧面单帧 body 的上限：接入层回程单包上限（16 KiB）减去帧头长度。
// 口径依据：帧头 + body 才是落在网线上的数据报，而接入层按**整个数据报**判超限；
// 故 body 上限必须比 16 KiB 小一个帧头，否则「恰好合规」的回执仍会被接入层丢掉。
const MaxFrameBodySize = 16*1024 - frame.HeaderSize

// frameBodySize 返回某帧面生效的 body 上限：未配置（0）即取契约上限；显式配置只允许
// 0 < n ≤ MaxFrameBodySize，超限或负数即装配期失败（该错误让进程启动失败，不静默放过）。
func frameBodySize(configured int64, field string) (int, error) {
	switch {
	case configured == 0:
		return MaxFrameBodySize, nil
	case configured < 0 || configured > MaxFrameBodySize:
		return 0, fmt.Errorf("server: %s=%d 超出单包上限 %d（接入层回程按整个数据报计 16 KiB，"+
			"超出的数据报会被接入层丢弃；配大了不会生效）", field, configured, MaxFrameBodySize)
	default:
		return int(configured), nil
	}
}

// limitBody 返回按契约收敛后的帧面配置：克隆注入的配置对象并写入 body 上限
// （proto.Clone 保证不改动配置中心下发的共享对象）；未启用的面（零值配置）原样返回。
func limitBody[C proto.Message](conf C, field string, get func(C) int64, set func(C, int64)) (C, error) {
	var zero C
	if !proto.Message(conf).ProtoReflect().IsValid() {
		return zero, nil // 该面未配置（typed nil）：不构造也就不校验
	}
	size, err := frameBodySize(get(conf), field)
	if err != nil {
		return zero, err
	}
	cloned, ok := proto.Clone(conf).(C)
	if !ok {
		return zero, fmt.Errorf("server: %s 配置克隆类型异常", field)
	}
	set(cloned, int64(size))
	return cloned, nil
}

// frameHandler 返回一个已注册帧 op 的 handler 链：**内层限包**（超包回执换可判定拒绝，
// P0-4）→ **外层观测**（耗时直方图 + 失败按 reason 计数，P1-4②）。顺序有意如此：
// 超包拒绝是 op 的一次失败，必须计入 op 错误口径（外层看得到内层的结果）。
func frameHandler(operation string, h engine.MsgHandler, m *FrameMetrics) engine.MsgHandler {
	return m.Wrap(operation, LimitReply(operation, h, m))
}

// LimitReply 是「单包 ≤16 KiB」契约的帧面闸门：handler 回执超过上限即**不返回超包回执**，
// 改回可判定拒绝（reason=TRANSPORT_DOWNLINK_FAILED：回执已生成但下发失败，客户端可换更小的
// 请求重试，如补帧区间收窄），并按 operation 计数。
//
// 为什么必须在这一层拦：引擎对超包回执是**静默丢弃**（只落日志），客户端只会等到超时；
// 超包是「请求区间太大」这类客户端可自行收窄的问题，给稳定 reason 才可判定。
func LimitReply(operation string, h engine.MsgHandler, m *FrameMetrics) engine.MsgHandler {
	return func(ctx context.Context, dec engine.Decoder, enc engine.Encoder) ([]byte, error) {
		out, err := h(ctx, dec, enc)
		if err != nil || len(out) <= MaxFrameBodySize {
			return out, err
		}
		m.oversizeOf(operation).Add(1)
		return nil, atlaserrors.New(500, atlaserrors.ReasonDownlinkFailed,
			fmt.Sprintf("回执 %d 字节超过单包上限 %d（operation=%s）：请缩小请求区间后重试",
				len(out), MaxFrameBodySize, operation))
	}
}
