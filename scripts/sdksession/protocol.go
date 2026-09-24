// Package sdksession 提供 SDK 会话协议接缝（client.SessionProtocol）的模板侧实现：
// 会话 op 名、凭据提取、过期时间与「被挤下线」推送识别全部取自模板生成的会话描述符
// （api/gateway/v1/opclient），SDK 内核不保留任何 op 字面量副本。
//
// 为什么单独成包：e2e 用例（test/e2e）、闭环脚本（scripts/e2e）与压测脚本
// （scripts/loadtest）三处都要接入同一份接缝，集中一份避免形态各自漂移。
package sdksession

import (
	"fmt"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-game-layout/api/gateway/v1/opclient"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	sdkframe "github.com/huangyuCN/atlas-sdk-go/frame"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// protocol 是零状态的接缝实现：每个方法直接转发生成物素材。
type protocol struct{}

// Protocol 返回会话协议接缝（零状态，可重复取用）。
func Protocol() sdkclient.SessionProtocol { return protocol{} }

// NewSession 构造已注入会话协议接缝的 SDK 会话管理器（模板侧统一入口，
// 免去每个调用点手写 WithSessionProtocol）。
func NewSession(opts ...sdkclient.SessionOption) *sdkclient.Session {
	return sdkclient.NewSession(append([]sdkclient.SessionOption{
		sdkclient.WithSessionProtocol(protocol{}),
	}, opts...)...)
}

// ReplyAs 把 SDK 会话回执断言为模板生成的具体 DTO 类型（如 *gatewayv1.RegisterReply）；
// 类型不符显式报错，不静默取零值——回执类型由接缝 op 与模板生成物共同决定，
// 不符即契约漂移，必须在调用侧暴露。
func ReplyAs[T any](reply any) (T, error) {
	typed, ok := reply.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("会话回执类型不符：期望 %T", zero)
	}
	return typed, nil
}

// Ops 返回生成物声明的 5 个会话 op 名。
func (protocol) Ops() sdkclient.SessionOps {
	ops := gatewayv1opclient.SessionProtocolOps
	return sdkclient.SessionOps{
		Register:  ops.Register,
		Login:     ops.Login,
		Resume:    ops.Resume,
		Logout:    ops.Logout,
		Heartbeat: ops.Heartbeat,
	}
}

// Token 经生成物提取器取 token（会话请求/回执；无该字段返回空串）。
func (protocol) Token(msg any) string { return gatewayv1opclient.SessionToken(msg) }

// PlayerID 经生成物提取器取玩家 ID（无该字段返回空串）。
func (protocol) PlayerID(msg any) string { return gatewayv1opclient.SessionPlayerID(msg) }

// ExpiresAt 经生成物提取器取过期时间（模板当前无该字段，恒 0，本轮不启用续期）。
func (protocol) ExpiresAt(msg any) int64 { return gatewayv1opclient.SessionExpiresAt(msg) }

// Kicked 按生成物推送 op 判定「被挤下线」并解出原因枚举名。msg 是推送信封
// （client.PushEnvelope：op + 帧头编码版本 + 原始字节），**必须按 envelope.Version 选解码器**：
// ver=1 是 protojson JSON 字节、ver=2 是 protobuf wire 字节（S0.5 修订 1——帧头版本一度在
// 推送分发处被丢弃，按单一编码解码会让 ver=2 的原因静默丢失：ok=true 而 reason 为空）。
// 未知版本或载荷解不出时不 panic：返回 ok=true + 空 reason（识别为被挤下线但取不到原因），
// 不猜测编码、不误判为其他推送。
func (protocol) Kicked(op string, msg any) (string, bool) {
	if op != gatewayv1opclient.SessionPushOps.KickedNotify {
		return "", false
	}
	envelope, ok := msg.(sdkclient.PushEnvelope)
	if !ok || len(envelope.Body) == 0 {
		return "", true // 识别为被挤下线，但载荷取不到原因
	}
	var notify gatewayv1.KickedNotify
	switch envelope.Version {
	case sdkframe.Version:
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(envelope.Body, &notify); err != nil {
			return "", true
		}
	case sdkframe.Version2:
		if err := proto.Unmarshal(envelope.Body, &notify); err != nil {
			return "", true
		}
	default:
		return "", true // 未知帧头版本：不猜编码
	}
	return notify.GetReason().String(), true
}
