package framelimit

import (
	"testing"

	"github.com/huangyuCN/atlas/transport/frame"
)

// TestContractValues 钉住契约取值：任一个数变了都等于改「单包 ≤16 KiB」的判定口径。
func TestContractValues(t *testing.T) {
	cases := []struct {
		name string
		got  int
		want int
	}{
		{"MaxBodySize", MaxBodySize, 16368},
		{"ReplyEnvelopeSize", ReplyEnvelopeSize, 5},
		{"ReplyPayloadLimit", ReplyPayloadLimit, 16363},
		{"ReplyBudget", ReplyBudget, 16299},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %d，期望 %d", tc.name, tc.got, tc.want)
		}
	}
}

// TestReplyEnvelopeSizeMatchesFrame 断言信封常量与框架真实编码一致：成功回执的 body
// 就是 [hasError(1)][dataLen(4)]。框架改了信封而这里没跟，则自裁预算会悄悄多算 5 字节。
func TestReplyEnvelopeSizeMatchesFrame(t *testing.T) {
	body := frame.EncodeReply(nil, nil, "")
	defer frame.PutBuf(body)
	if len(body) != ReplyEnvelopeSize {
		t.Fatalf("框架成功回执信封 = %d 字节，期望 %d（transport/frame/reply.go 的格式变了？）",
			len(body), ReplyEnvelopeSize)
	}
}

// TestBudgetsNestedAndFillPacket 断言三级预算严格嵌套（自裁 < 闸门 < body）且 body 恰好
// 填满「16 KiB − 帧头」：不嵌套就会出现「自裁后仍被闸门拦下」或「恰好合规却被接入层丢弃」。
func TestBudgetsNestedAndFillPacket(t *testing.T) {
	if ReplyBudget >= ReplyPayloadLimit || ReplyPayloadLimit >= MaxBodySize {
		t.Fatalf("预算未严格嵌套: 自裁=%d 闸门=%d body=%d", ReplyBudget, ReplyPayloadLimit, MaxBodySize)
	}
	if MaxBodySize+frame.HeaderSize != 16*1024 {
		t.Fatalf("MaxBodySize(%d) + 帧头(%d) ≠ 16 KiB：单包契约变了", MaxBodySize, frame.HeaderSize)
	}
	if ReplyMargin <= 0 {
		t.Fatalf("安全余量必须为正，实际 %d", ReplyMargin)
	}
}
