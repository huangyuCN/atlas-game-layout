// Package redis 提供游戏模板的 Redis 客户端与常用封装：
// 玩家会话 token 存取（单点登录路由表）、通用键值便捷方法。
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Options 是 Redis 连接选项。
type Options struct {
	// Addr 是单点 Redis 地址（Mode 为空或 "single" 时使用，host:port）。
	Addr string
	// Mode 是部署形态："single"（默认）/ "sentinel"（主从哨兵）/ "cluster"（集群分片）。
	Mode string
	// MasterName 是哨兵监控的主库名（Mode=sentinel 时必填）。
	MasterName string
	// Addrs 是哨兵或集群节点地址列表（Mode=sentinel/cluster 时必填）。
	Addrs []string
	// Password 是可选密码。
	Password string
	// DB 是数据库编号（仅 single/sentinel 形态；cluster 按 key 哈希分片，不支持 DB 选择）。
	DB int
}

// Client 包装 go-redis 客户端（UniversalClient 兼容单点/哨兵/集群三种形态）。
type Client struct {
	inner goredis.UniversalClient
}

// NewClient 按 Mode 构造 Redis 客户端（惰性连接，不验证）：
//   - 空 / "single"：直连 Addr；
//   - "sentinel"：经哨兵发现主库（MasterName + Addrs），主从切换自动跟随；
//   - "cluster"：集群分片客户端（Addrs 为任一节点引导地址）。
func NewClient(opts Options) (*Client, error) {
	var inner goredis.UniversalClient
	switch opts.Mode {
	case "", "single":
		if opts.Addr == "" {
			return nil, fmt.Errorf("redis: addr 不能为空")
		}
		inner = goredis.NewClient(&goredis.Options{
			Addr:     opts.Addr,
			Password: opts.Password,
			DB:       opts.DB,
		})
	case "sentinel":
		if opts.MasterName == "" || len(opts.Addrs) == 0 {
			return nil, fmt.Errorf("redis: sentinel 形态需要 master_name 与哨兵地址")
		}
		inner = goredis.NewFailoverClient(&goredis.FailoverOptions{
			MasterName:    opts.MasterName,
			SentinelAddrs: opts.Addrs,
			Password:      opts.Password,
			DB:            opts.DB,
		})
	case "cluster":
		if len(opts.Addrs) == 0 {
			return nil, fmt.Errorf("redis: cluster 形态需要节点地址")
		}
		inner = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:    opts.Addrs,
			Password: opts.Password,
			ReadOnly: true, // 读请求可落副本（写仍走主库）
		})
	default:
		return nil, fmt.Errorf("redis: 未知 mode %q（支持 single/sentinel/cluster）", opts.Mode)
	}
	return &Client{inner: inner}, nil
}

// Close 关闭客户端。
func (c *Client) Close() error { return c.inner.Close() }

// Ping 探测连通性（集成测试与启动探活用）。
func (c *Client) Ping(ctx context.Context) error {
	return c.inner.Ping(ctx).Err()
}

// Raw 返回底层客户端（供 matchmaker 队列等复用；UniversalClient 接口）。
func (c *Client) Raw() goredis.UniversalClient { return c.inner }

// playerSessionKey 生成玩家会话路由键：atlas:session:<playerID>。
func playerSessionKey(playerID string) string {
	return "atlas:session:" + playerID
}

// SetPlayerSession 写入玩家会话令牌（TTL 过期自动清理）。
func (c *Client) SetPlayerSession(ctx context.Context, playerID, token string, ttl time.Duration) error {
	if playerID == "" || token == "" {
		return fmt.Errorf("redis: playerID/token 不能为空")
	}
	return c.inner.Set(ctx, playerSessionKey(playerID), token, ttl).Err()
}

// GetPlayerSession 读取玩家会话令牌（不存在时返回空串）。
func (c *Client) GetPlayerSession(ctx context.Context, playerID string) (string, error) {
	v, err := c.inner.Get(ctx, playerSessionKey(playerID)).Result()
	if errors.Is(err, goredis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return v, nil
}

// DelPlayerSession 删除玩家会话（登出/挤下线）。
func (c *Client) DelPlayerSession(ctx context.Context, playerID string) error {
	return c.inner.Del(ctx, playerSessionKey(playerID)).Err()
}

// gatewaySessionKey 生成 gateway 分布式路由键：atlas:gw:<playerID>。
func gatewaySessionKey(playerID string) string {
	return "atlas:gw:" + playerID
}

// SetGatewayRoute 写入玩家连接所在 gateway 实例（D13 分布式路由表）。
func (c *Client) SetGatewayRoute(ctx context.Context, playerID, instanceID string, ttl time.Duration) error {
	if playerID == "" {
		return fmt.Errorf("redis: playerID 不能为空")
	}
	return c.inner.Set(ctx, gatewaySessionKey(playerID), instanceID, ttl).Err()
}

// GetGatewayRoute 读取玩家连接所在 gateway 实例（不存在返回空串）。
func (c *Client) GetGatewayRoute(ctx context.Context, playerID string) (string, error) {
	v, err := c.inner.Get(ctx, gatewaySessionKey(playerID)).Result()
	if errors.Is(err, goredis.Nil) {
		return "", nil
	}
	return v, err
}

// DelGatewayRoute 删除 gateway 路由（连接断开）。
func (c *Client) DelGatewayRoute(ctx context.Context, playerID string) error {
	return c.inner.Del(ctx, gatewaySessionKey(playerID)).Err()
}
