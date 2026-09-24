// Package mongo 提供游戏模板的 MongoDB 客户端与仓储基类：
// 连接、数据库/集合选择、索引初始化。
package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Options 是 MongoDB 连接选项。
type Options struct {
	// URI 是连接串（mongodb://host:port）。
	URI string
	// Database 是默认数据库名。
	Database string
	// ConnectTimeout 是建连超时（默认 5s）。
	ConnectTimeout time.Duration
}

// Client 包装 mongo-driver 客户端。
type Client struct {
	inner    *mongo.Client
	database string
}

// NewClient 建立 MongoDB 连接（同步 Ping 验证）。
func NewClient(ctx context.Context, opts Options) (*Client, error) {
	if opts.URI == "" {
		return nil, fmt.Errorf("mongo: uri 不能为空")
	}
	if opts.Database == "" {
		return nil, fmt.Errorf("mongo: database 不能为空")
	}
	timeout := opts.ConnectTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cli, err := mongo.Connect(cctx, options.Client().ApplyURI(opts.URI))
	if err != nil {
		return nil, fmt.Errorf("mongo: 连接失败: %w", err)
	}
	if err := cli.Ping(cctx, nil); err != nil {
		return nil, fmt.Errorf("mongo: ping 失败: %w", err)
	}
	return &Client{inner: cli, database: opts.Database}, nil
}

// Close 关闭客户端。
func (c *Client) Close(ctx context.Context) error { return c.inner.Disconnect(ctx) }

// Database 返回默认数据库。
func (c *Client) Database() *mongo.Database { return c.inner.Database(c.database) }

// Collection 返回默认数据库下的集合。
func (c *Client) Collection(name string) *mongo.Collection {
	return c.inner.Database(c.database).Collection(name)
}

// Raw 返回底层客户端（复杂查询场景）。
func (c *Client) Raw() *mongo.Client { return c.inner }

// Index 描述一个索引。
type Index struct {
	// Keys 是索引字段与方向（**有序**：复合索引的字段顺序即前缀顺序）。
	// 必须是 bson.D 而非 map——mongo 驱动的索引键要求有序，多键 map 会在建索引时
	// 报 "multi-key map passed in for ordered parameter keys"。
	Keys bson.D
	// Unique 是否唯一索引。
	Unique bool
	// Sparse 是否稀疏索引：文档缺该字段时不进索引，故多条缺字段文档互不冲突。
	// 用途：管理面审计的幂等键唯一索引——dry-run 记录不写幂等键，必须稀疏。
	Sparse bool
	// Name 索引名（空则自动生成）。
	Name string
}

// EnsureIndexes 为集合创建索引（幂等：已存在则跳过）。
func (c *Client) EnsureIndexes(ctx context.Context, coll string, indexes []Index) error {
	if len(indexes) == 0 {
		return nil
	}
	if _, err := c.Collection(coll).Indexes().CreateMany(ctx, buildIndexModels(indexes)); err != nil {
		return fmt.Errorf("mongo: 创建集合 %s 索引失败: %w", coll, err)
	}
	return nil
}

// buildIndexModels 把 Index 描述转换为驱动索引模型（选项透传的唯一落点）。
func buildIndexModels(indexes []Index) []mongo.IndexModel {
	models := make([]mongo.IndexModel, 0, len(indexes))
	for _, idx := range indexes {
		keys := make(bson.D, len(idx.Keys))
		copy(keys, idx.Keys) // 复制一份：不与被调用方共享底层数组
		opt := options.Index()
		if idx.Unique {
			opt.SetUnique(true)
		}
		if idx.Sparse {
			opt.SetSparse(true)
		}
		if idx.Name != "" {
			opt.SetName(idx.Name)
		}
		models = append(models, mongo.IndexModel{Keys: keys, Options: opt})
	}
	return models
}
