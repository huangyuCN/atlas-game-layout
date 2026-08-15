// Package models 定义战斗结果聚合（mongo 结算落库）。
package models

// PlayerResult 是单个玩家的战斗结果（内嵌文档）。
type PlayerResult struct {
	PlayerID string `bson:"player_id" json:"player_id"`
	Win      bool   `bson:"win" json:"win"`
	Score    int32  `bson:"score" json:"score"`
}

// BattleResult 是战斗结算结果（mongo 文档，_id 即 battleID）。
type BattleResult struct {
	BattleID    string         `bson:"_id" json:"battle_id"`
	MatchID     string         `bson:"match_id" json:"match_id"`
	Players     []PlayerResult `bson:"players" json:"players"`
	TotalFrames uint64         `bson:"total_frames" json:"total_frames"`
	SettledAt   int64          `bson:"settled_at" json:"settled_at"`
}
