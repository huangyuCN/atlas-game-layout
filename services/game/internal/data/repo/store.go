package repo

// 本文件是玩家仓储的组合实现（耐久性选源与落盘编排）。
// 职责（对齐持久层耐久性设计第一期）：
//   - LoadPlayer：登录选源——Redis 连不上不能登录；两边都读按 persistVersion
//     取大者（相同用 Redis）；选源后双向对齐补写（禁止旧缓存回档）。
//   - LoadCredential：mongo 专用读（含认证字段；登录口令校验走这里）。
//   - FlushPlayer：统一落盘（CRC 跳过 → persistVersion 自增一次 → 编码一次 →
//     先 Redis，成功即止；失败用同一份编码与版本写 Mongo），返回结果分类
//     供调用方进入冻结 / 强制下线流程。

import (
	"context"
	"errors"
	"hash/crc32"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"go.mongodb.org/mongo-driver/bson"
)

// PlayerCache 是落盘依赖的最小 redis 面（RedisPlayerCache 实现；测试可注入 fake）。
type PlayerCache interface {
	Get(ctx context.Context, playerID string) (*models.PlayerSnapshot, error)
	SetEncoded(ctx context.Context, playerID string, b []byte, ttl time.Duration) error
	Set(ctx context.Context, snap *models.PlayerSnapshot, ttl time.Duration) error
	Persist(ctx context.Context, playerID string) error
	TTLOf(ctx context.Context, playerID string) (time.Duration, error)
	Expire(ctx context.Context, playerID string, ttl time.Duration) error
}

// PlayerPersist 是 mongo 权威库的最小依赖面（MongoPlayerRepo 实现；测试可注入 fake）。
type PlayerPersist interface {
	FindByID(ctx context.Context, playerID string) (*models.Player, error)
	Upsert(ctx context.Context, p *models.Player) error
	FindByAccount(ctx context.Context, account string) (*models.Player, error)
	Create(ctx context.Context, p *models.Player) error
}

// PlayerStore 是玩家仓储的组合实现：redis 耐久缓冲 + mongo 权威持久化。
type PlayerStore struct {
	cache   PlayerCache
	persist PlayerPersist
}

// NewPlayerStore 构造组合仓储。
func NewPlayerStore(cache PlayerCache, persist PlayerPersist) *PlayerStore {
	return &PlayerStore{cache: cache, persist: persist}
}

// LoadPlayer 实现登录选源（不含认证字段——口令校验走 LoadCredential）：
//  1. Redis 连不上（非 key miss）→ 返回错误，不能登录；
//  2. Redis miss → 只读 Mongo；
//  3. 命中 → 再读 Mongo，persistVersion 大者胜（相同用 Redis）；
//     Mongo 读失败但 Redis 有档 → 用 Redis（避免 Mongo 抖动全员进不去）；
//  4. 选源后双向对齐补写：选中 Redis → 补写 Mongo（成功且原为 noexpire →
//     EXPIRE 恢复 72h；失败保持 PERSIST）；选中 Mongo → 覆盖 Redis（TTL 72h）。
func (s *PlayerStore) LoadPlayer(ctx context.Context, playerID string) (*models.Player, error) {
	snap, err := s.cache.Get(ctx, playerID)
	if err != nil && !errors.Is(err, ErrPlayerNotFound) {
		return nil, err // Redis 故障：拒绝登录，不回源 Mongo（防旧缓存回档）
	}
	redisHit := err == nil

	mp, mErr := s.persist.FindByID(ctx, playerID)
	if !redisHit {
		if mErr != nil {
			return nil, mErr // 双 miss（含新玩家）
		}
		return mp, nil
	}
	if mErr != nil {
		// Mongo 读失败但 Redis 有档：用 Redis（不做对齐——Mongo 不可用）。
		return snapshotToPlayer(snap), nil
	}
	// 两边都有：persistVersion 大者胜，相同用 Redis。
	if mp.PersistVersion > snap.PersistVersion {
		if err := s.overwriteRedis(ctx, mp); err != nil {
			return nil, err
		}
		return mp, nil
	}
	// Redis 选中：补写 Mongo（选中档比 Mongo 新或相等——相等也补写幂等）；
	// Mongo 补写成功且原 key 为 PERSIST 过（TTL -1）→ EXPIRE 恢复 72h。
	if err := s.persist.Upsert(ctx, snapshotToPlayer(snap)); err != nil {
		return snapshotToPlayer(snap), nil // 补写失败：保持 PERSIST，不 DEL
	}
	if d, terr := s.cache.TTLOf(ctx, playerID); terr == nil && d == -1 {
		_ = s.cache.Expire(ctx, playerID, defaultSnapshotTTL)
	}
	return snapshotToPlayer(snap), nil
}

// LoadCredential 实现 mongo 专用读（含认证字段）：登录口令校验专用。
func (s *PlayerStore) LoadCredential(ctx context.Context, playerID string) (*models.Player, error) {
	return s.persist.FindByID(ctx, playerID)
}

// FindByAccount 实现 PlayerRepo（注册判重，仅 mongo）。
func (s *PlayerStore) FindByAccount(ctx context.Context, account string) (*models.Player, error) {
	return s.persist.FindByAccount(ctx, account)
}

// CreatePlayer 实现 PlayerRepo（注册建档，mongo 插入）。
func (s *PlayerStore) CreatePlayer(ctx context.Context, p *models.Player) error {
	return s.persist.Create(ctx, p)
}

// FlushPlayer 统一落盘（tick/下线/显式落地共用）：
//  1. 数据指纹（BSON 快照，persistVersion 置零参与）相对 lastCRC 未变 → 跳过；
//  2. 判脏 → persistVersion++（持久化侧副作用，不进 undo 回滚栈）→ 编码一次
//     （含新版本）→ 先写 Redis（成功即止，不写 Mongo）；
//  3. Redis 失败 → 用同一份编码与同一版本写 Mongo（降级直写）。
//
// 返回：结果分类 + 本次成功落盘的数据指纹（失败分支返回 lastCRC 不变）。
func (s *PlayerStore) FlushPlayer(ctx context.Context, p *models.Player, lastCRC uint32) (FlushResult, uint32, error) {
	fingerprint, err := snapshotFingerprint(p)
	if err != nil {
		return FlushBothFailed, lastCRC, err
	}
	sum := crc32.ChecksumIEEE(fingerprint)
	if sum == lastCRC {
		return FlushSkipped, lastCRC, nil
	}
	p.PersistVersion++ // 同一次落盘 Redis/Mongo 共用同一版本（只自增一次）
	snap := models.SnapshotFromPlayer(p)
	b, err := snapshotEncode(snap)
	if err != nil {
		p.PersistVersion-- // 编码失败：回退版本自增
		return FlushBothFailed, lastCRC, err
	}
	if err := s.cache.SetEncoded(ctx, p.PlayerID, b, defaultSnapshotTTL); err == nil {
		return FlushRedisOK, sum, nil
	}
	// Redis 失败：降级 Mongo（同一版本，内存聚合根此时为权威）。
	if err := s.persist.Upsert(ctx, p); err != nil {
		p.PersistVersion-- // 双失败：版本自增随落盘失败一并回退
		return FlushBothFailed, lastCRC, err
	}
	return FlushMongoOK, sum, nil
}

// snapshotFingerprint 计算快照的数据指纹（persistVersion 置零后编码，
// 保证「数据没变 + 版本没变」时指纹稳定，CRC 跳过不被版本自增破坏）。
func snapshotFingerprint(p *models.Player) ([]byte, error) {
	detached := models.SnapshotFromPlayer(p)
	detached.PersistVersion = 0
	return bson.Marshal(detached)
}

// SaveSnapshot 实现 redis 快照直写（TTL 72h；PERSIST 场景另行调用 PersistSnapshot）。
func (s *PlayerStore) SaveSnapshot(ctx context.Context, p *models.Player, ttl time.Duration) error {
	return s.cache.Set(ctx, models.SnapshotFromPlayer(p), ttl)
}

// SavePlayer 实现 mongo 落库。
func (s *PlayerStore) SavePlayer(ctx context.Context, p *models.Player) error {
	return s.persist.Upsert(ctx, p)
}

// PersistSnapshot 实现玩家快照 PERSIST（去 TTL：未落库权威副本语义）。
func (s *PlayerStore) PersistSnapshot(ctx context.Context, playerID string) error {
	return s.cache.Persist(ctx, playerID)
}

// TTLOfSnapshot 实现快照 TTL 查询（-1 = PERSIST 过；miss = ErrPlayerNotFound）。
func (s *PlayerStore) TTLOfSnapshot(ctx context.Context, playerID string) (time.Duration, error) {
	return s.cache.TTLOf(ctx, playerID)
}

// ExpireSnapshot 实现恢复快照 TTL（登录补写 Mongo 成功后的 EXPIRE）。
func (s *PlayerStore) ExpireSnapshot(ctx context.Context, playerID string, ttl time.Duration) error {
	return s.cache.Expire(ctx, playerID, ttl)
}

// overwriteRedis 用 mongo 侧数据覆盖 redis 旧缓存（登录选源 Mongo 时的对齐）。
func (s *PlayerStore) overwriteRedis(ctx context.Context, mp *models.Player) error {
	return s.cache.Set(ctx, models.SnapshotFromPlayer(mp), defaultSnapshotTTL)
}

// snapshotToPlayer 把快照视图回填为 Player（认证字段为零值；口令校验走 LoadCredential）。
func snapshotToPlayer(snap *models.PlayerSnapshot) *models.Player {
	p := &models.Player{
		PlayerID:       snap.PlayerID,
		Account:        snap.Account,
		Nickname:       snap.Nickname,
		Level:          snap.Level,
		CreatedAt:      snap.CreatedAt,
		PersistVersion: snap.PersistVersion,
	}
	if len(snap.Items) > 0 {
		p.Items = make([]*models.Item, len(snap.Items))
		copy(p.Items, snap.Items)
	}
	return p
}
