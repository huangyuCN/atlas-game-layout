package stream

import (
	"bytes"
	"strings"
	"testing"

	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/transport"
)

// captureLogs 接管全局日志器并返回输出缓冲（用例断言日志字段，结束后恢复原日志器）。
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	old := atlaslog.GetLogger()
	var buf bytes.Buffer
	atlaslog.SetLogger(atlaslog.New(atlaslog.WithWriter(&buf)))
	t.Cleanup(func() { atlaslog.SetLogger(old) })
	return &buf
}

// TestConnStreamID 验证 battle 侧直连流标识的口径：流式面按连接 ID、数据报面按对端键、
// 两者皆无返回空串（不臆造标识）。
func TestConnStreamID(t *testing.T) {
	cases := []struct {
		name string
		conn Conn
		want string
	}{
		{"流式面按连接 ID", Conn{Kind: transport.KindKCP, ConnID: 7}, "kcp/7"},
		{"数据报面按对端键", Conn{Kind: transport.KindUDP, Peer: "10.0.0.9:5000"}, "udp/10.0.0.9:5000"},
		{"无任何标识", Conn{Kind: transport.KindWebSocket}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ConnStreamID(tc.conn); got != tc.want {
				t.Fatalf("ConnStreamID = %q，期望 %q", got, tc.want)
			}
		})
	}
}

// TestCloseBattleLogsStreamID 验证结算关闭直连的日志带 stream_id + player_id + battle_id
// 关联字段（P1-4③）：结算关键路径的每一条直连都要能按这三个字段定位
// （与接入层 `s-…` 是两段各自标识，按 player_id + battle_id 对齐）。
func TestCloseBattleLogsStreamID(t *testing.T) {
	logs := captureLogs(t)
	reg, kcpPort, _ := newTestRegistry()
	reg.RecordEnded("b-1", "p-1", []string{"p-1", "p-2"})
	reg.Register("p-1", kcpConn(7, "b-1"))
	reg.CloseBattle("b-1")

	out := logs.String()
	for _, want := range []string{"stream_id=kcp/7", "player_id=p-1", "battle_id=b-1", "winner=p-1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("结算关闭日志缺少 %q：\n%s", want, out)
		}
	}
	if len(kcpPort.closed) != 1 {
		t.Fatalf("关闭的直连数 = %d，期望 1", len(kcpPort.closed))
	}
}
