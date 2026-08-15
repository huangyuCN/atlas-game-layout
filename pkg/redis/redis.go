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
	// Addr 是 Redis 地址（host:port）。
	Addr string
	// Password 是可选密码。
	Password string
	// DB 是数据库编号。
	DB int
}

// Client 包装 go-redis 客户端。
type Client struct {
	inner *goredis.Client
}

// NewClient 构造 Redis 客户端（惰性连接，不验证）。
func NewClient(opts Options) (*Client, error) {
	if opts.Addr == "" {
		return nil, fmt.Errorf("redis: addr 不能为空")
	}
	cli := goredis.NewClient(&goredis.Options{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	})
	return &Client{inner: cli}, nil
}

// Close 关闭客户端。
func (c *Client) Close() error { return c.inner.Close() }

// Ping 探测连通性（集成测试与启动探活用）。
func (c *Client) Ping(ctx context.Context) error {
	return c.inner.Ping(ctx).Err()
}

// Raw 返回底层客户端（供 matchmaker 队列等复用）。
func (c *Client) Raw() *goredis.Client { return c.inner }

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
