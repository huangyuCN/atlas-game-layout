// 帧广播解码状态机（每战斗会话一份）：只信 base_seq，跳变即丢弃 + 请求关键帧。

package downlink

import (
	"fmt"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Requester 是发送关键帧请求的端口（由会话层注入：发 RequestFrameKeyframe(last_seq)）。
type Requester func(lastSeq uint64) error

// Decoder 是每战斗会话一份的解码状态机（设计 §4.3 客户端状态机）：
//
//	收到 ver=1/2 全量: 基线重置到该帧，交付上层，清 pendingReq
//	收到 ver=3 增量:   序号不连续或 base_seq 与自己不符 ⇒ 丢弃 + 请求关键帧（去重）
//	                   序号 <= 基线                   ⇒ 迟到/重复，静默丢弃
//	                   其余                           ⇒ 合并后交付上层
//
// **不缓冲后续增量等关键帧**：那会把「一个可恢复的失步」变成「一段静默花屏」。
type Decoder struct {
	request Requester

	full     *battlev1.FrameBroadcast
	lastSeq  uint64
	hasBase  bool
	pending  bool // 已请求、尚未拿到关键帧（去重，请求风暴的客户端侧闸门）
	requests int  // 累计发出去重后的关键帧请求次数（观测/测试用）
	drops    int  // 累计丢弃的增量条数（失步/迟到，观测用）
}

// NewDecoder 构造解码器；request 为 nil 时只丢弃不失步帧（纯解码形态）。
func NewDecoder(request Requester) *Decoder { return &Decoder{request: request} }

// LastSeq 返回当前基线帧号（= 已交付的最后一帧）。
func (d *Decoder) LastSeq() uint64 { return d.lastSeq }

// Pending 返回是否已请求关键帧但还没拿到。
func (d *Decoder) Pending() bool { return d.pending }

// Requests 返回累计发出的关键帧请求次数（去重后）。
func (d *Decoder) Requests() int { return d.requests }

// Drops 返回累计丢弃的增量条数。
func (d *Decoder) Drops() int { return d.drops }

// Apply 处理一帧载荷：ver=1 protojson / ver=2、3 protobuf。
// 返回 (可交付的完整帧, 是否交付, 错误)；失步的增量返回 (nil, false, nil)——
// 「丢弃」不是错误，只有载荷本身解不开才是错误（§7 I6：任意字节不得 panic）。
func (d *Decoder) Apply(payload []byte, ver uint8) (*battlev1.FrameBroadcast, bool, error) {
	switch ver {
	case 1:
		return d.applyFull(payload, protojson.Unmarshal)
	case 2:
		return d.applyFull(payload, proto.Unmarshal)
	case VersionDelta:
		return d.applyDelta(payload)
	default:
		return nil, false, fmt.Errorf("downlink: 未知的载荷版本 %d", ver)
	}
}

// applyFull 处理全量帧（ver=1/2，关键帧也走这里）：基线直接重置。
func (d *Decoder) applyFull(payload []byte, unmarshal func([]byte, proto.Message) error) (*battlev1.FrameBroadcast, bool, error) {
	bc := new(battlev1.FrameBroadcast)
	if err := unmarshal(payload, bc); err != nil {
		return nil, false, fmt.Errorf("downlink: 全量帧解码失败: %w", err)
	}
	d.full = bc
	d.lastSeq = bc.GetFrame().GetFrameId()
	d.hasBase = true
	d.pending = false
	return bc, true, nil
}

// applyDelta 处理增量帧：严格校验基线，不符即丢弃并请求关键帧（不猜、不缓冲）。
func (d *Decoder) applyDelta(payload []byte) (*battlev1.FrameBroadcast, bool, error) {
	delta := new(battlev1.FrameBroadcastDelta)
	if err := proto.Unmarshal(payload, delta); err != nil {
		return nil, false, fmt.Errorf("downlink: 增量帧解码失败: %w", err)
	}
	if !d.hasBase {
		d.requestKeyframe()
		return nil, false, nil // 没有基线：无从合并，请求关键帧
	}
	if delta.GetSeq() <= d.lastSeq {
		return nil, false, nil // 迟到/重复：静默丢弃（不发请求，避免回绕放大）
	}
	if delta.GetSeq() != d.lastSeq+1 || delta.GetBaseSeq() != d.lastSeq {
		d.drops++
		d.requestKeyframe()
		return nil, false, nil
	}
	d.full = Merge(d.full, delta)
	d.lastSeq = delta.GetSeq()
	d.pending = false
	return d.full, true, nil
}

// requestKeyframe 发一次关键帧请求（同一 pending 窗口内只发一次）。
func (d *Decoder) requestKeyframe() {
	if d.pending {
		return
	}
	d.pending = true
	d.requests++
	if d.request != nil {
		_ = d.request(d.lastSeq)
	}
}

// epochNanos 把绝对 UnixNano 还原为时间（合并 server_time 用）。
func epochNanos(nanos int64) time.Time { return time.Unix(0, nanos) }
