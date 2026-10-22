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
