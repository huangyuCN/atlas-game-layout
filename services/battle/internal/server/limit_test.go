package server

import (
	"context"
	"strings"
	"testing"

	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/framelimit"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// TestFrameBodySizeContract 验证「单包 ≤16 KiB」契约的装配期口径：未配置按契约补默认，
// 显式配置不得超限（超限即启动失败——配大了不会生效，只会让大回执在接入层被丢弃）。
func TestFrameBodySizeContract(t *testing.T) {
	cases := []struct {
		name       string
		configured int64
		want       int
		wantErr    string
	}{
		{"未配置取契约上限", 0, MaxFrameBodySize, ""},
		{"显式等于上限", MaxFrameBodySize, MaxFrameBodySize, ""},
		{"显式更小", 4096, 4096, ""},
		{"显式超限", 1 << 20, 0, "超出单包上限"},
		{"显式负数", -1, 0, "超出单包上限"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := frameBodySize(tc.configured, "server.kcp.max_body_size")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("错误 = %v，期望包含 %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("上限 = %d（err=%v），期望 %d", got, err, tc.want)
			}
		})
	}
}

// TestLimitBodyClonesAndCaps 验证配置收敛：不改注入对象（克隆后写入），未启用面原样返回。
func TestLimitBodyClonesAndCaps(t *testing.T) {
	get := (*configspb.Server_KCP).GetMaxBodySize
	set := func(c *configspb.Server_KCP, n int64) { c.MaxBodySize = n }

	small := &configspb.Server_KCP{Addr: "127.0.0.1:0", MaxBodySize: 1024}
	got, err := limitBody(small, "server.kcp", get, set)
	if err != nil || got.GetMaxBodySize() != 1024 {
		t.Fatalf("显式小值应保留: %d（err=%v）", got.GetMaxBodySize(), err)
	}
	if small.GetMaxBodySize() != 1024 {
		t.Fatal("不得改动注入的配置对象")
	}

	unset := &configspb.Server_KCP{Addr: "127.0.0.1:0"}
	capped, err := limitBody(unset, "server.kcp", get, set)
	if err != nil || capped.GetMaxBodySize() != int64(MaxFrameBodySize) {
		t.Fatalf("未配置应补契约上限 %d: %d（err=%v）", MaxFrameBodySize, capped.GetMaxBodySize(), err)
	}

	over := &configspb.Server_KCP{Addr: "127.0.0.1:0", MaxBodySize: 1 << 20}
	if _, err := limitBody(over, "server.kcp", get, set); err == nil {
		t.Fatal("显式超限应报错（启动失败）")
	}

	if got, err := limitBody((*configspb.Server_KCP)(nil), "server.kcp", get, set); err != nil || got != nil {
		t.Fatalf("未启用面应原样返回: %v（err=%v）", got, err)
	}
}

// TestLimitReplyRejectsOversize 验证超包回执被替换为可判定拒绝 + 计数（P0-4）：引擎对超包
// 回执是静默丢弃，故必须在回执交给引擎之前拦住，客户端拿到稳定 reason 而不是等到超时。
//
// 口径是**载荷**上限：网线上的 body = 载荷 + 5 字节回执信封，故「载荷恰好等于 body 上限」
// 也必须拒绝——否则它反推出的 body 比单包上限大 5 字节，接入层照样整包丢弃。
func TestLimitReplyRejectsOversize(t *testing.T) {
	stub := newMetricStub()
	m := NewFrameMetrics(stub)
	op := battlev1opclient.BattleServiceProtocolOps.SyncFrames

	oversize := []int{MaxFrameBodySize, MaxFrameBodySize + 1, framelimit.ReplyPayloadLimit + 1}
	for i, n := range oversize {
		out, err := LimitReply(op, sizeStub(n), m)(context.Background(), nil, nil)
		if out != nil {
			t.Fatalf("第 %d 例（载荷 %d 字节）：超包回执不得下发（引擎会静默丢弃）", i+1, n)
		}
		if se := atlaserrors.FromError(err); se.Reason != atlaserrors.ReasonDownlinkFailed || se.Code != 500 {
			t.Fatalf("第 %d 例：reason/code = %q/%d（err=%v），期望 %q/500",
				i+1, se.Reason, se.Code, err, atlaserrors.ReasonDownlinkFailed)
		}
	}
	if got := stub.count(MetricReplyOversize, "operation", op); got != float64(len(oversize)) {
		t.Fatalf("超包计数 = %v，期望 %d", got, len(oversize))
	}

	got, err := LimitReply(op, sizeStub(framelimit.ReplyPayloadLimit), m)(context.Background(), nil, nil)
	if err != nil || len(got) != framelimit.ReplyPayloadLimit {
		t.Fatalf("载荷上限内的回执应原样透传: len=%d err=%v", len(got), err)
	}
	if n := stub.count(MetricReplyOversize, "operation", op); n != float64(len(oversize)) {
		t.Fatalf("上限内不应计数（累计 %v，期望 %d）", n, len(oversize))
	}
}

// sizeStub 返回固定长度的回执 handler（长度即业务载荷字节数）。
func sizeStub(n int) engine.MsgHandler {
	return func(context.Context, engine.Decoder, engine.Encoder) ([]byte, error) {
		return make([]byte, n), nil
	}
}

// TestFrameHandlerChain 验证注册链路的两层包装真的串在一起（顺序：内限包、外观测）：
// 超包回执既被换成可判定拒绝，也计入 op 错误口径与耗时直方图。
func TestFrameHandlerChain(t *testing.T) {
	stub := newMetricStub()
	m := NewFrameMetrics(stub)
	op := battlev1opclient.BattleServiceProtocolOps.SyncFrames
	big := func(context.Context, engine.Decoder, engine.Encoder) ([]byte, error) {
		return make([]byte, MaxFrameBodySize+1), nil
	}
	if _, err := frameHandler(op, big, m)(context.Background(), nil, nil); err == nil {
		t.Fatal("超包回执应被拒绝")
	}
	if got := stub.count(MetricReplyOversize, "operation", op); got != 1 {
		t.Fatalf("超包计数 = %v，期望 1", got)
	}
	if got := stub.count(MetricOpErrors, "operation", op, "reason", atlaserrors.ReasonDownlinkFailed); got != 1 {
		t.Fatalf("op 错误计数 = %v，期望 1（超包拒绝计入 op 失败口径）", got)
	}
	if obs := stub.observations(MetricOpDuration, "operation", op); len(obs) != 1 {
		t.Fatalf("耗时观测 = %v，期望 1 次", obs)
	}
}
