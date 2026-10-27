// 推送载荷编码（「一次序列化、多人复用」的落点）：一帧只编码一次，产物 []byte 复用于该帧的
// 全部目标；目标写入仍各自一帧（写是必需的 syscall，不省也不并发改写同一份帧体）。

package stream

import (
	"errors"

	"github.com/huangyuCN/atlas/encoding"
)

// Encoder 把一条推送消息编码为帧载荷（帧 body 里 operation 之后的 payload 段）。
// 实现必须与帧引擎 Push 的编码口径一致，否则「预编码 + PushRaw」与「Push」下发的字节会不同。
type Encoder func(msg any) ([]byte, error)

// errCodecMissing 表示 json codec 未注册，预编码无从进行（生产装配不会发生：
// 本包依赖的 frame/engine 包会注册 json codec）。
var errCodecMissing = errors.New("stream: json codec 未注册（缺少 encoding/json 导入）")

// errProtoCodecMissing 表示 proto codec 未注册，protobuf 预编码无从进行
// （打开 protobuf 开关的部署必须导入 encoding/proto）。
var errProtoCodecMissing = errors.New("stream: proto codec 未注册（缺少 encoding/proto 导入）")

// encodeJSON 是默认推送载荷编码器：与帧引擎 Push 同一条路径
// （encoding.GetCodec("json") → protojson，EmitUnpopulated=true），
// 故「预编码 + PushRaw」与直接 Push 的线格式逐字节一致。
func encodeJSON(msg any) ([]byte, error) {
	codec := encoding.GetCodec("json")
	if codec == nil {
		return nil, errCodecMissing
	}
	return codec.Marshal(msg)
}

// EncodeProtobuf 是 protobuf 二进制推送载荷编码器（载荷编码 ver=2，规范 §3.1 载荷编码协商）：
// 与帧引擎在 ver=2 请求下使用的 codec 同一份实现（encoding.GetCodec("proto") → proto.Marshal）。
//
// 体积收益来自编码方式本身（无 JSON 键名与 EmitUnpopulated 的零值字段）：同一帧广播实测
// 约 protojson 的 29%（≈3.5×），且**不减少帧数**——每连接每帧仍是一次写。
func EncodeProtobuf(msg any) ([]byte, error) {
	codec := encoding.GetCodec("proto")
	if codec == nil {
		return nil, errProtoCodecMissing
	}
	return codec.Marshal(msg)
}
