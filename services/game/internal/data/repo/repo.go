// Package repo 提供玩家数据的缓存与持久化操作：
// 在线内存聚合根（PlayerActor 持有）→ 定时 redis 快照 → 下线 mongo 持久化；
// 上线加载走「redis 优先、mongo 兜底」三级链路。
package repo

import (
	"context"
	"errors"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
)

// ErrPlayerNotFound 表示玩家数据不存在。
var ErrPlayerNotFound = errors.New("repo: 玩家不存在")

// FlushResult 是统一落盘的结果分类（驱动冻结 / 强制下线流程的编排依据）。
type FlushResult int

// FlushResult 的取值。
const (
	// FlushSkipped 数据相对上次成功落盘未变，跳过（版本不涨）。
	FlushSkipped FlushResult = iota
	// FlushRedisOK Redis 写成功（首选路径；不写 Mongo）。
	FlushRedisOK
	// FlushMongoOK Redis 失败、Mongo 降级成功（调用方按场景决定冻结或下线）。
	FlushMongoOK
	// FlushBothFailed 双库失败（在线 → 冻结；下线 → 按四组合表退出）。
	FlushBothFailed
)

// PlayerRepo 是玩家数据仓储接口（biz 层依赖；实现见 PlayerStore）。
type PlayerRepo interface {
	// LoadPlayer 登录选源：Redis 连不上不能登录；两边按 persistVersion 取大者
	// （相同用 Redis）；选源后双向对齐补写。返回的 Player 不含认证字段
	// （口令校验走 LoadCredential）。
	LoadPlayer(ctx context.Context, playerID string) (*models.Player, error)
	// LoadCredential mongo 专用读（含认证字段；登录口令校验用）。
	LoadCredential(ctx context.Context, playerID string) (*models.Player, error)
	// FindByAccount 按账号查询（注册判重用，仅 mongo）。
	FindByAccount(ctx context.Context, account string) (*models.Player, error)
	// CreatePlayer 注册建档（mongo 插入）。
	CreatePlayer(ctx context.Context, player *models.Player) error
	// FlushPlayer 统一落盘：CRC 跳过 → persistVersion 自增 → 先 Redis 后 Mongo
	// 降级；返回结果分类与本次落盘后的数据指纹。
	FlushPlayer(ctx context.Context, player *models.Player, lastCRC uint32) (FlushResult, uint32, error)
	// SavePlayer 持久化到 mongo（无条件 Upsert）。
	SavePlayer(ctx context.Context, player *models.Player) error
	// PersistSnapshot 玩家快照 PERSIST（去 TTL：未落库权威副本语义）。
	PersistSnapshot(ctx context.Context, playerID string) error
	// TTLOfSnapshot 快照 TTL 查询（-1 = PERSIST 过）。
	TTLOfSnapshot(ctx context.Context, playerID string) (time.Duration, error)
	// ExpireSnapshot 恢复快照 TTL（登录补写 Mongo 成功后）。
	ExpireSnapshot(ctx context.Context, playerID string, ttl time.Duration) error
}
