package stream

import (
	"context"
	"fmt"
	"testing"

	"github.com/huangyuCN/atlas/transport"
)

// BenchmarkPublishFrameBroadcast10 度量「一帧广播给 10 个目标」的完整推送开销
// （注册表寻址 → 载荷编码 → 帧引擎拼装上帧体并写出），用于对照「一次序列化、多人复用」
// 前后的 ns/op、B/op 与 allocs/op。
func BenchmarkPublishFrameBroadcast10(b *testing.B) {
	port, connIDs, _ := newEnginePort(b, benchPlayers, nil)
	reg := NewRegistry()
	reg.BindPort(transport.KindKCP, port)
	battleID := "b-bench"
	players := make([]string, 0, benchPlayers)
	for i := 0; i < benchPlayers; i++ {
		playerID := fmt.Sprintf("p-%d", i)
		players = append(players, playerID)
		reg.Register(playerID, Conn{Kind: transport.KindKCP, ConnID: connIDs[i], BattleID: battleID})
	}
	frame := pushTestFrame(42)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 生产路径：一帧广播给全部目标（每帧只编码一次，同一份载荷复用于全部目标）。
		if err := reg.PublishFrames(ctx, battleID, players, frame); err != nil {
			b.Fatalf("PublishFrames: %v", err)
		}
	}
}
