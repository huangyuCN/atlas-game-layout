// Package framelimit 汇总 battle 帧面「单包 ≤16 KiB」契约的取值与推导：
// 帧面装配（max_body_size 收敛）、回执闸门（server.LimitReply）与业务回执自裁
// （补帧回执封顶）共用同一份口径——多处各写一个 16 KiB 迟早漂移。
package framelimit

import "github.com/huangyuCN/atlas/transport/frame"

const (
	// MaxBodySize 是单帧 body 的上限：接入层回程单包 16 KiB 减去帧头 16 字节 = 16368。
	// 依据：帧头 + body 才是落在网线上的数据报，而接入层按**整个数据报**判超限，
	// 故 body 上限必须比 16 KiB 小一个帧头。
	MaxBodySize = 16*1024 - frame.HeaderSize

	// ReplyEnvelopeSize 是响应帧 body 的外层信封长度：[hasError(1)][dataLen(4)]，
	// 见 atlas transport/frame/reply.go 的 EncodeReply（成功与失败两种形态同长）。
	ReplyEnvelopeSize = 5

	// ReplyPayloadLimit 是回执**载荷**（业务消息编码后）的硬上限：
	// body = 信封 + 载荷，故载荷必须比单帧 body 再小一个信封。
	ReplyPayloadLimit = MaxBodySize - ReplyEnvelopeSize

	// ReplyMargin 是业务回执自裁时留的安全余量（字节）：约 0.4% 的包容量，用于容纳
	// 「回执信封以后再加一段」与编码器实现差异；闸门（ReplyPayloadLimit）仍然兜底。
	ReplyMargin = 64

	// ReplyBudget 是业务回执自裁的字节预算（补帧回执按它截断）：
	// 载荷 + 余量都不越过 ReplyPayloadLimit。
	ReplyBudget = ReplyPayloadLimit - ReplyMargin
)
