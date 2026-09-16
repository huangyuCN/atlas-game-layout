package repo

// 本文件是玩家聚合根的 redis 快照存储（压缩 BSON）：耐久性缓冲层。
// 编码 = 1 字节格式版本头 + s2 压缩的 BSON（PlayerSnapshot：剥离认证字段，
// 与 mongo 文档同构）。TTL 语义：
//   - 正常态 TTL 72h（mongo 已有权威数据，过期无妨）；
//   - 下线/在线 Mongo 失败 → Persist 转永不过期（该存档成为未落库权威副本）；
//   - 登录补写 Mongo 成功后 EXPIRE 恢复 72h；禁止 DEL 玩家存档 key。
//
// 运维约束：该 redis 的 maxmemory-policy 必须 noeviction 或 volatile-*
// （allkeys-* 淘汰策略会清掉无 TTL 的未落库副本）。

import (
	"context"
	"errors"
	"fmt"
	"time"

	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/klauspost/compress/s2"
	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
)

// 缓存键与编码约定。
const (
	// playerCachePrefix 是玩家快照缓存键前缀：atlas:player:<playerID>。
	playerCachePrefix = "atlas:player:"
	// snapFormatVer 是快照编码格式版本（1 = s2 压缩的 BSON；演进留位）。
	snapFormatVer byte = 1
	// defaultSnapshotTTL 是玩家快照的正常租期（72h；Mongo 故障期间的保底窗口）。
	defaultSnapshotTTL = 72 * time.Hour
)

// ErrSnapshotFormat 表示快照格式版本不认识（升级回退场景，拒绝解析）。
var ErrSnapshotFormat = errors.New("repo: 未知快照格式版本")

// snapshotEncode 把快照视图编码为压缩二进制（版本头 + s2(BSON)）。
func snapshotEncode(snap *models.PlayerSnapshot) ([]byte, error) {
	raw, err := bson.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("repo: 快照 BSON 编码失败: %w", err)
	}
	return append([]byte{snapFormatVer}, s2.Encode(nil, raw)...), nil
}

// snapshotDecode 解码压缩二进制为快照视图。
func snapshotDecode(b []byte) (*models.PlayerSnapshot, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("repo: 快照为空")
	}
	if b[0] != snapFormatVer {
		return nil, fmt.Errorf("%w: %d", ErrSnapshotFormat, b[0])
	}
	raw, err := s2.Decode(nil, b[1:])
	if err != nil {
		return nil, fmt.Errorf("repo: 快照解压失败: %w", err)
	}
	var snap models.PlayerSnapshot
	if err := bson.Unmarshal(raw, &snap); err != nil {
		return nil, fmt.Errorf("repo: 快照 BSON 解码失败: %w", err)
	}
	return &snap, nil
}

// RedisPlayerCache 是玩家聚合根的 redis 快照存储（耐久性缓冲层）。
type RedisPlayerCache struct {
	cli *pkredis.Client
}

// NewRedisPlayerCache 构造玩家快照缓存。
func NewRedisPlayerCache(cli *pkredis.Client) *RedisPlayerCache {
	return &RedisPlayerCache{cli: cli}
}

// Get 读取玩家快照；不存在返回 ErrPlayerNotFound。
func (c *RedisPlayerCache) Get(ctx context.Context, playerID string) (*models.PlayerSnapshot, error) {
	b, err := c.cli.Raw().Get(ctx, playerCachePrefix+playerID).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, ErrPlayerNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("repo: 读取玩家缓存失败: %w", err)
	}
	return snapshotDecode(b)
}

// Set 写玩家快照（压缩 BSON；ttl <= 0 即永不过期——未落库副本语义）。
func (c *RedisPlayerCache) Set(ctx context.Context, p *models.PlayerSnapshot, ttl time.Duration) error {
	b, err := snapshotEncode(p)
	if err != nil {
		return err
	}
	return c.cli.Raw().Set(ctx, playerCachePrefix+p.PlayerID, b, ttl).Err()
}

// SetEncoded 写入已编码的快照字节（统一落盘路径：编码只做一次）。
func (c *RedisPlayerCache) SetEncoded(ctx context.Context, playerID string, b []byte, ttl time.Duration) error {
	return c.cli.Raw().Set(ctx, playerCachePrefix+playerID, b, ttl).Err()
}

// Expire 恢复玩家快照的 TTL（登录补写 Mongo 成功后的 EXPIRE）。
func (c *RedisPlayerCache) Expire(ctx context.Context, playerID string, ttl time.Duration) error {
	return c.cli.Raw().Expire(ctx, playerCachePrefix+playerID, ttl).Err()
}

// Persist 取消玩家快照的 TTL（转永不过期：该存档成为未落库权威副本）。
func (c *RedisPlayerCache) Persist(ctx context.Context, playerID string) error {
	return c.cli.Raw().Persist(ctx, playerCachePrefix+playerID).Err()
}

// TTLOf 查询玩家快照剩余 TTL；键不存在返回 ErrPlayerNotFound，
// 无 TTL（PERSIST 过）返回 -1，其余返回剩余时长。
func (c *RedisPlayerCache) TTLOf(ctx context.Context, playerID string) (time.Duration, error) {
	d, err := c.cli.Raw().TTL(ctx, playerCachePrefix+playerID).Result()
	if errors.Is(err, goredis.Nil) {
		return 0, ErrPlayerNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("repo: 查询快照 TTL 失败: %w", err)
	}
	return d, nil
}
