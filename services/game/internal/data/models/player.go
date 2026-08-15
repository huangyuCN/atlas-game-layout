// Package models 定义玩家聚合根（纯数据模型）。
// 业务写入经 cow 生成的 undo 写代理（zz_generated.undo_proxy.go）完成，
// 本文件只含字段声明与序列化标签，保持模型干净（写代理方法全部在生成物中）。
package models

// +cow:undoproxy-gen=true
// Player 是玩家聚合根：内存写模型（cow undo 回滚）与持久化视图共用同一结构。
// PlayerActor 单协程串行写满足 cow 前提；BSON 用于 mongo、JSON 用于 redis 快照。
type Player struct {
	PlayerID  string  `bson:"_id" json:"player_id"`
	Account   string  `bson:"account" json:"account"`
	Salt      string  `bson:"salt" json:"salt"`         // 示例取舍：快照含认证字段（生产建议分离存储）
	Password  string  `bson:"password" json:"password"` // 示例取舍：同上
	Nickname  string  `bson:"nickname" json:"nickname"`
	Level     int32   `bson:"level" json:"level"`
	Items     []*Item `bson:"items" json:"items"`
	CreatedAt int64   `bson:"created_at" json:"created_at"`
}

// Item 是背包条目（聚合根子结构，指针元素配合 cow 写代理）。
type Item struct {
	ItemID uint32 `bson:"item_id" json:"item_id"`
	Count  uint32 `bson:"count" json:"count"`
}
