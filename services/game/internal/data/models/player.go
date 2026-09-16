// Package models 定义玩家聚合根（纯数据模型）。
// 业务写入经 cow 生成的 undo 写代理（zz_generated.undo_proxy.go）完成，
// 本文件只含字段声明与序列化标签，保持模型干净（写代理方法全部在生成物中）。
package models

// Player 是玩家聚合根：内存写模型（cow undo 回滚）与 mongo 持久化视图共用同一结构。
// PlayerActor 单协程串行写满足 cow 前提；PersistVersion 是耐久性版本戳
// （仅在落盘且判脏路径经 PutPersistVersion 自增一次，Redis/Mongo 共用同一版本，
// 登录按它选源取大者；老档视为 0；不进客户端协议）。
//
// +cow:undoproxy-gen=true
type Player struct {
	PlayerID       string  `bson:"_id" json:"player_id"`
	Account        string  `bson:"account" json:"account"`
	Salt           string  `bson:"salt" json:"salt"`         // 认证字段：仅存 mongo，不出 redis 快照（见 PlayerSnapshot）
	Password       string  `bson:"password" json:"password"` // 认证字段：仅存 mongo（sha256 摘要）
	Nickname       string  `bson:"nickname" json:"nickname"`
	Level          int32   `bson:"level" json:"level"`
	Items          []*Item `bson:"items" json:"items"`
	CreatedAt      int64   `bson:"created_at" json:"created_at"`
	PersistVersion uint64  `bson:"persistVersion" json:"-"` // 耐久性版本戳（不进客户端协议）
}

// Item 是背包条目（聚合根子结构，指针元素配合 cow 写代理）。
type Item struct {
	ItemID uint32 `bson:"item_id" json:"item_id"`
	Count  uint32 `bson:"count" json:"count"`
}

// PlayerSnapshot 是 redis 快照视图：Player 剥离认证字段（凭据只存 mongo，
// 快照泄露面收敛），含 PersistVersion 供登录选源比较。字段 tag 集与 Player
// 同名同形（_id 除外：快照键为 playerID），bson.Marshal 直接可用。
type PlayerSnapshot struct {
	PlayerID       string  `bson:"_id"`
	Account        string  `bson:"account"`
	Nickname       string  `bson:"nickname"`
	Level          int32   `bson:"level"`
	Items          []*Item `bson:"items"`
	CreatedAt      int64   `bson:"created_at"`
	PersistVersion uint64  `bson:"persistVersion"`
}

// SnapshotFromPlayer 构造聚合根的 redis 快照视图（剥离认证字段）。
func SnapshotFromPlayer(p *Player) *PlayerSnapshot {
	return &PlayerSnapshot{
		PlayerID:       p.PlayerID,
		Account:        p.Account,
		Nickname:       p.Nickname,
		Level:          p.Level,
		Items:          p.Items,
		CreatedAt:      p.CreatedAt,
		PersistVersion: p.PersistVersion,
	}
}

// RestoreToPlayer 把快照视图回填进聚合根（认证字段由 LoadPlayer 调用方另行补齐——
// 认证数据仅存 mongo，登录校验走 mongo 专用读）。
func RestoreToPlayer(snap *PlayerSnapshot, p *Player) {
	p.PlayerID = snap.PlayerID
	p.Account = snap.Account
	p.Nickname = snap.Nickname
	p.Level = snap.Level
	p.Items = snap.Items
	p.CreatedAt = snap.CreatedAt
	p.PersistVersion = snap.PersistVersion
}
