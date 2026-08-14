// Package idgen 提供游戏模板的 ID 生成：玩家/对局/战斗等标识，
// 统一「前缀 + UUID」形态（短横线去除，长度固定，全局唯一）。
package idgen

import (
	"strings"

	"github.com/google/uuid"
)

// New 生成形如 "<prefix>-<32位hex>" 的全局唯一 ID。
func New(prefix string) string {
	return prefix + "-" + strings.ReplaceAll(uuid.NewString(), "-", "")
}

// Player 生成玩家 ID。
func Player() string { return New("p") }

// Match 生成对局 ID（匹配池对局）。
func Match() string { return New("m") }

// Battle 生成战斗 ID。
func Battle() string { return New("b") }
