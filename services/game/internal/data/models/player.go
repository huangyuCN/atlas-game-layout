// Package models 定义玩家聚合根（纯数据模型）。
// 业务写入经 cow 生成的 undo 写代理（zz_generated.undo_proxy.go）完成，
// 本文件只含字段声明与序列化标签，保持模型干净（写代理方法全部在生成物中）。
package models

// Player 是玩家聚合根：内存写模型与 mongo 持久化视图共用同一结构。
// PlayerActor 单协程串行写满足 cow 前提；PersistVersion 是耐久性版本戳
// （仅在落盘且判脏路径自增一次，Redis/Mongo 共用同一版本，登录按它选源
// 取大者；老档视为 0；不进客户端协议）。
//
// 认证字段（Salt/Password）只在注册建档（Create）时写入 mongo；此后所有
// 聚合根→mongo 的更新写（Upsert）均不携带认证字段（见 repo 层 $set 剔除），
// 换密码等凭据变更需走显式专用路径，防止无认证字段的内存副本把 mongo
// 凭据覆盖为空。
//
// +cow:undoproxy-gen=true
type Player struct {
	PlayerID  string  `bson:"_id" json:"player_id"`
	Account   string  `bson:"account" json:"account"`
	Salt      string  `bson:"salt" json:"-"`     // 认证字段：仅存 mongo（redis 快照与 json 视图均不出）
	Password  string  `bson:"password" json:"-"` // 认证字段：仅存 mongo（sha256 摘要）
	Nickname  string  `bson:"nickname" json:"nickname"`
	Level     int32   `bson:"level" json:"level"`
	Items     []*Item `bson:"items" json:"items"`
	CreatedAt int64   `bson:"created_at" json:"created_at"`
	// PersistVersion 是耐久性版本戳（不进客户端协议与 json 视图）。
	PersistVersion uint64 `bson:"persistVersion" json:"-"`
}

// Item 是背包条目（聚合根子结构，指针元素配合 cow 写代理）。
type Item struct {
	ItemID uint32 `bson:"item_id" json:"item_id"`
	Count  uint32 `bson:"count" json:"count"`
}
