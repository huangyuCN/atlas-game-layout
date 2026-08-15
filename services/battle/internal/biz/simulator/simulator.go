// Package simulator 提供确定性战斗模拟器示例（一维竞速 + 胜负判定）。
// 要求：同一初始状态与输入序列，任意执行顺序结果一致（lockstep 确定性契约）。
package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"

	"github.com/huangyuCN/atlas/lockstep"
)

// Battle 是竞速战斗模拟器：玩家沿 0..TrackLen 赛道推进，
// 先到终点者胜；MaxFrames 帧内无人到达则位置领先者胜。
// 状态更新按玩家定位（顺序无关），序列化（json map 按键排序）保证确定性。
type Battle struct {
	trackLen  int32
	maxFrames uint64
	pos       map[string]int32
	score     map[string]int32
	winner    string
	frame     uint64 // 当前帧（快照帧号）
}

// New 构造竞速模拟器。
func New(trackLen int32, maxFrames uint64) *Battle {
	return &Battle{
		trackLen:  trackLen,
		maxFrames: maxFrames,
		pos:       make(map[string]int32),
		score:     make(map[string]int32),
	}
}

// Step 实现 lockstep.Simulator：应用一帧输入并推进状态。
func (b *Battle) Step(_ context.Context, frame lockstep.FrameID, inputs []lockstep.Input) error {
	b.frame = uint64(frame)
	if b.winner != "" {
		return nil // 已分出胜负：后续帧不再变化（确定性）
	}
	for _, in := range inputs {
		pos := b.pos[string(in.Player)]
		if len(in.Payload) > 0 {
			pos += int32(in.Payload[0])
		}
		if pos > b.trackLen {
			pos = b.trackLen
		}
		b.pos[string(in.Player)] = pos
		if pos >= b.trackLen && b.score[string(in.Player)] == 0 {
			b.score[string(in.Player)] = 1
			b.winner = string(in.Player)
		}
	}
	// 帧数耗尽：位置领先者胜（无人领先则平局）。
	if b.winner == "" && uint64(frame) >= b.maxFrames {
		best, bestPos := "", int32(-1)
		tie := false
		for p, pos := range b.pos {
			if pos > bestPos {
				best, bestPos, tie = p, pos, false
			} else if pos == bestPos {
				tie = true
			}
		}
		if !tie && best != "" {
			b.winner = best
		}
	}
	return nil
}

// Snapshot 实现 lockstep.Simulator：状态序列化 + 哈希。
func (b *Battle) Snapshot(_ context.Context) (lockstep.Snapshot, error) {
	st := State{Positions: b.pos, Scores: b.score, Winner: b.winner}
	raw, err := json.Marshal(&st)
	if err != nil {
		return lockstep.Snapshot{}, fmt.Errorf("simulator: 状态序列化失败: %w", err)
	}
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return lockstep.Snapshot{
		Frame: lockstep.FrameID(b.frame),
		State: raw,
		Hash:  h.Sum64(),
	}, nil
}

// Restore 实现 lockstep.Simulator：从快照恢复。
func (b *Battle) Restore(_ context.Context, snap lockstep.Snapshot) error {
	var st State
	if err := json.Unmarshal(snap.State, &st); err != nil {
		return fmt.Errorf("simulator: 状态反序列化失败: %w", err)
	}
	b.pos = st.Positions
	b.score = st.Scores
	b.winner = st.Winner
	return nil
}

// Winner 返回当前胜者玩家（空串=未分胜负）。
func (b *Battle) Winner() string { return b.winner }

// Score 返回指定玩家得分。
func (b *Battle) Score(player string) int32 { return b.score[player] }

// State 是快照内的业务状态（json 确定性序列化；battle actor 结算解析复用）。
type State struct {
	Positions map[string]int32 `json:"positions"`
	Scores    map[string]int32 `json:"scores"`
	Winner    string           `json:"winner"`
}
