package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// Store 是路由表存储抽象（redis 实现见 RedisStore；测试可注入内存实现）。
type Store interface {
	// GetSet 原子写入新路由并返回旧路由（nil 表示首次登记）。
	GetSet(ctx context.Context, playerID string, r *Route, ttl time.Duration) (*Route, error)
	// Get 读取路由（不存在返回 nil）。
	Get(ctx context.Context, playerID string) (*Route, error)
	// Delete 删除路由。
	Delete(ctx context.Context, playerID string) error
	// Expire 续租 TTL。
	Expire(ctx context.Context, playerID string, ttl time.Duration) error
}

// RedisStore 是路由表的 redis 实现（值编码为 JSON）。
// 兼容 M2 旧版纯实例 ID 字符串值：解析失败时按旧格式降级处理。
type RedisStore struct {
	cli *pkredis.Client
}

// NewRedisStore 构造 redis 路由存储。
func NewRedisStore(cli *pkredis.Client) *RedisStore { return &RedisStore{cli: cli} }

// routeKey 生成路由键：atlas:gw:<playerID>（与 pkg/redis 的 gatewaySessionKey 约定一致）。
func routeKey(playerID string) string { return "atlas:gw:" + playerID }

// GetSet 实现 Store：SET key value EX ttl GET 单命令原子「写新值 + 设 TTL + 取旧值」。
// 相比已弃用的 GETSET 命令（go-redis GetSet 已标注 Deprecated），
// SET 的 GET 选项（Redis 6.2+）一步完成且天然携带 TTL。
func (s *RedisStore) GetSet(ctx context.Context, playerID string, r *Route, ttl time.Duration) (*Route, error) {
	if playerID == "" {
		return nil, fmt.Errorf("session: playerID 不能为空")
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("session: 路由编码失败: %w", err)
	}
	old, err := s.cli.Raw().SetArgs(ctx, routeKey(playerID), string(b), goredis.SetArgs{TTL: ttl, Get: true}).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return nil, err
	}
	if errors.Is(err, goredis.Nil) || old == "" {
		return nil, nil
	}
	return parseRouteValue(old), nil
}

// Get 实现 Store。
func (s *RedisStore) Get(ctx context.Context, playerID string) (*Route, error) {
	v, err := s.cli.Raw().Get(ctx, routeKey(playerID)).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return parseRouteValue(v), nil
}

// Delete 实现 Store。
func (s *RedisStore) Delete(ctx context.Context, playerID string) error {
	return s.cli.Raw().Del(ctx, routeKey(playerID)).Err()
}

// Expire 实现 Store。
func (s *RedisStore) Expire(ctx context.Context, playerID string, ttl time.Duration) error {
	return s.cli.Raw().Expire(ctx, routeKey(playerID), ttl).Err()
}

// parseRouteValue 解析路由值：JSON 优先；失败时按 M2 旧格式（纯实例 ID）降级。
func parseRouteValue(v string) *Route {
	var r Route
	if err := json.Unmarshal([]byte(v), &r); err == nil {
		return &r
	}
	return &Route{InstanceID: v}
}
