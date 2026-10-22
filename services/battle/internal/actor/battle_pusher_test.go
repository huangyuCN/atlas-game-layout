// 直连推送端口的内存实现（battle actor 用例共用）：记录帧广播/结束/出局通知与关闭的对局，
// 并按「一次调用 = 一次编码」的口径记录广播次数（供「每帧一次」断言使用）。
// 与 battle_test.go 分文件：那里是装配与环境，这里是推送端口的测试替身。

package actor

import (
	"context"
	"sync"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
)

// memPusher 是直连推送端口的内存实现（帧/结束/出局通知与关闭对局的记录）。
type memPusher struct {
	mu         sync.Mutex
	frames     map[string][]*locksteppb.LockstepFrame // playerID → 帧序列
	ends       map[string][]string                    // playerID → 胜者序列
	outs       map[string][]outRecord                 // playerID → 出局广播序列
	closed     []string                               // 已关闭的对局 ID（结算后回收，规格 §9.8）
	broadcasts []int                                  // 每次帧广播的目标数（一次调用 = 一次编码）
}

// newMemPusher 构造直连推送内存实现。
func newMemPusher() *memPusher {
	return &memPusher{
		frames: make(map[string][]*locksteppb.LockstepFrame),
		ends:   make(map[string][]string),
		outs:   make(map[string][]outRecord),
	}
}

// PublishFrames 实现 biz.BattlePusher：一次调用把同一帧记给全部目标
// （调用次数即编码次数，用例据 broadcasts 断言「每帧一次」）。
func (p *memPusher) PublishFrames(_ context.Context, _ string, players []string, f *locksteppb.LockstepFrame) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.broadcasts = append(p.broadcasts, len(players))
	for _, playerID := range players {
		p.frames[playerID] = append(p.frames[playerID], f)
	}
	return nil
}

// broadcastStats 返回帧广播的调用次数与目标数合计（一次调用 = 一次编码）。
func (p *memPusher) broadcastStats() (calls, targets int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, n := range p.broadcasts {
		targets += n
	}
	return len(p.broadcasts), targets
}

// PublishEnd 实现 biz.BattlePusher：记录直连结束通知。
func (p *memPusher) PublishEnd(_ context.Context, playerID, _, winner string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ends[playerID] = append(p.ends[playerID], winner)
	return nil
}

// PublishOut 实现 biz.BattlePusher：记录玩家出局广播（掉线判负，规格 §9.3）。
func (p *memPusher) PublishOut(_ context.Context, playerID, _, outPlayerID string, reason battlev1.PlayerOutReason) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.outs[playerID] = append(p.outs[playerID], outRecord{playerID: outPlayerID, reason: reason})
	return nil
}

// CloseBattle 实现 biz.BattlePusher：记录关闭的对局。
func (p *memPusher) CloseBattle(battleID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = append(p.closed, battleID)
}

// snapshots 返回帧/结束通知与关闭对局的快照（测试断言用）。
func (p *memPusher) snapshots() (map[string][]*locksteppb.LockstepFrame, map[string][]string, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	frames := make(map[string][]*locksteppb.LockstepFrame, len(p.frames))
	for k, v := range p.frames {
		frames[k] = append([]*locksteppb.LockstepFrame(nil), v...)
	}
	ends := make(map[string][]string, len(p.ends))
	for k, v := range p.ends {
		ends[k] = append([]string(nil), v...)
	}
	return frames, ends, append([]string(nil), p.closed...)
}
