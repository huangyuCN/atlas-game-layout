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

// Mode 是 redis 部署形态（枚举；零值即单点，Options{} 与单点语义一致）。
type Mode int

// 部署形态的全部取值。
const (
	ModeSingle   Mode = iota // 单点（默认）
	ModeSentinel             // 主从哨兵
	ModeCluster              // 集群分片
)

// String 返回形态名（错误信息与日志用）；越界值返回 unknown(n) 以便自解释。
func (m Mode) String() string {
	switch m {
	case ModeSingle:
		return "single"
	case ModeSentinel:
		return "sentinel"
	case ModeCluster:
		return "cluster"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

// Options 是 Redis 连接选项。
type Options struct {
	// Addrs 是节点地址列表（host:port）：single 恰好 1 个；sentinel/cluster 为全部节点。
	Addrs []string
	// Mode 是部署形态（零值 = ModeSingle）。
	Mode Mode
	// MasterName 是哨兵监控的主库名（ModeSentinel 必填）。
	MasterName string
	// Password 是可选密码。
	Password string
	// DB 是数据库编号（仅 single/sentinel 形态；cluster 按 key 哈希分片，不支持 DB 选择）。
	DB int
	// Namespace 是业务键命名空间（空 = consts.EnvDefault）：共用同一 redis 的
	// 多套部署靠它隔离会话路由/玩家快照/撮合票据，键形态 atlas:<ns>:<域>:<id>。
	Namespace string
}

// Client 包装 go-redis 客户端（UniversalClient 兼容单点/哨兵/集群三种形态）。
type Client struct {
	inner goredis.UniversalClient
	keys  Keys
}

// Keys 返回该客户端的业务键构造器（命名空间在构造期确定，调用方不再各自拼前缀）。
func (c *Client) Keys() Keys { return c.keys }

// NewClient 按 Mode 构造 Redis 客户端（惰性连接，不验证）：
//   - ModeSingle：直连 Addrs[0]（必须恰好 1 个地址）；
//   - ModeSentinel：经哨兵发现主库（MasterName + Addrs），主从切换自动跟随；
//   - ModeCluster：集群分片客户端（Addrs 为任一节点引导地址）。
func NewClient(opts Options) (*Client, error) {
	var inner goredis.UniversalClient
	switch opts.Mode {
	case ModeSingle:
		if len(opts.Addrs) != 1 {
			return nil, fmt.Errorf("redis: single 形态需要且只接受 1 个地址，收到 %d 个", len(opts.Addrs))
		}
		inner = goredis.NewClient(&goredis.Options{
			Addr:     opts.Addrs[0],
			Password: opts.Password,
			DB:       opts.DB,
		})
	case ModeSentinel:
		if opts.MasterName == "" {
			return nil, fmt.Errorf("redis: sentinel 形态需要 master_name")
		}
		if len(opts.Addrs) == 0 {
			return nil, fmt.Errorf("redis: sentinel 形态需要至少 1 个哨兵地址")
		}
		inner = goredis.NewFailoverClient(&goredis.FailoverOptions{
			MasterName:    opts.MasterName,
			SentinelAddrs: opts.Addrs,
			Password:      opts.Password,
			DB:            opts.DB,
		})
	case ModeCluster:
		if len(opts.Addrs) == 0 {
			return nil, fmt.Errorf("redis: cluster 形态需要至少 1 个节点地址")
		}
		inner = goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:    opts.Addrs,
			Password: opts.Password,
			ReadOnly: true, // 读请求可落副本（写仍走主库）
		})
	default:
		return nil, fmt.Errorf("redis: 未知形态 %s", opts.Mode)
	}
	return &Client{inner: inner, keys: NewKeys(opts.Namespace)}, nil
}

// Close 关闭客户端。
func (c *Client) Close() error { return c.inner.Close() }

// Ping 探测连通性（集成测试与启动探活用）。
func (c *Client) Ping(ctx context.Context) error {
	return c.inner.Ping(ctx).Err()
}

// Raw 返回底层客户端（供 matchmaker 队列等复用；UniversalClient 接口）。
func (c *Client) Raw() goredis.UniversalClient { return c.inner }

// playerSessionKey 生成玩家会话路由键（命名空间化，见 Keys）。
func (c *Client) playerSessionKey(playerID string) string { return c.keys.PlayerSession(playerID) }

// SetPlayerSession 写入玩家会话令牌（TTL 过期自动清理）。
func (c *Client) SetPlayerSession(ctx context.Context, playerID, token string, ttl time.Duration) error {
	if playerID == "" || token == "" {
		return fmt.Errorf("redis: playerID/token 不能为空")
	}
	return c.inner.Set(ctx, c.playerSessionKey(playerID), token, ttl).Err()
}

// GetPlayerSession 读取玩家会话令牌（不存在时返回空串）。
func (c *Client) GetPlayerSession(ctx context.Context, playerID string) (string, error) {
	v, err := c.inner.Get(ctx, c.playerSessionKey(playerID)).Result()
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
	return c.inner.Del(ctx, c.playerSessionKey(playerID)).Err()
}

// gatewaySessionKey 生成 gateway 分布式路由键（命名空间化，见 Keys）。
func (c *Client) gatewaySessionKey(playerID string) string { return c.keys.GatewayRoute(playerID) }

// SetGatewayRoute 写入玩家连接所在 gateway 实例（D13 分布式路由表）。
func (c *Client) SetGatewayRoute(ctx context.Context, playerID, instanceID string, ttl time.Duration) error {
	if playerID == "" {
		return fmt.Errorf("redis: playerID 不能为空")
	}
	return c.inner.Set(ctx, c.gatewaySessionKey(playerID), instanceID, ttl).Err()
}

// GetGatewayRoute 读取玩家连接所在 gateway 实例（不存在返回空串）。
func (c *Client) GetGatewayRoute(ctx context.Context, playerID string) (string, error) {
	v, err := c.inner.Get(ctx, c.gatewaySessionKey(playerID)).Result()
	if errors.Is(err, goredis.Nil) {
		return "", nil
	}
	return v, err
}

// DelGatewayRoute 删除 gateway 路由（连接断开）。
func (c *Client) DelGatewayRoute(ctx context.Context, playerID string) error {
	return c.inner.Del(ctx, c.gatewaySessionKey(playerID)).Err()
}
