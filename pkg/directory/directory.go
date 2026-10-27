// Package directory 是 actor 目录后端的统一装配点。
//
// 为什么必须统一：目录是「一处写、多处读」的共享状态——托管方（battle/game）经
// pkg/actor 写归属记录，接入层（edge）只读属主（属主变更即拆流）。两端必须
// **同一个后端 + 同一个键前缀**，否则会静默错配成「写入正常、接入层查不到属主」。
// 因此后端选择（etcd | redis）、前缀派生、Redis 连接构造、QuiesceGrace 与复制滞后的
// 装配期校验都收口在本包，读写两侧只调用 Parse + Open。
package directory

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
	etcdlocator "github.com/huangyuCN/atlas/contrib/locator/etcd"
	redislocator "github.com/huangyuCN/atlas/contrib/locator/redis"
	"github.com/huangyuCN/atlas/locator"
	"github.com/huangyuCN/atlas/namespace"
	goredis "github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Backend 是 actor 目录后端类型（配置字段 actor.directory.backend）。
type Backend string

// 目录后端取值：etcd（缺省，保持现网行为）与 redis。
const (
	// BackendEtcd 是 etcd 后端（缺省）。
	BackendEtcd Backend = "etcd"
	// BackendRedis 是 Redis 后端（pub/sub 监听 + TTL 租约）。
	BackendRedis Backend = "redis"
)

// defaultSyncTimeout 是关键写 WAIT 的缺省等待上限。
const defaultSyncTimeout = 2 * time.Second

// Options 是 actor 目录后端配置（由 actor.directory 配置段解析）。
type Options struct {
	Backend        Backend       // 后端类型
	Redis          RedisOptions  // Redis 连接配置（仅 backend=redis 使用）
	QuiesceGrace   time.Duration // Claim 保护窗（0 = 框架缺省 5s）
	ReplicationLag time.Duration // 主从复制滞后上界（0 = 不声明）
}

// RedisOptions 是 Redis 目录后端的连接参数。
type RedisOptions struct {
	Endpoints          []string      // 节点地址：单点 1 个；哨兵为全部哨兵地址
	MasterName         string        // 哨兵监控的主库名（空 = 单点直连）
	Password           string        // 可选密码
	DB                 int           // 数据库编号（单点/哨兵）
	TLS                bool          // 启用 TLS（系统根证书校验）
	InsecureSkipVerify bool          // TLS 下跳过证书校验（仅测试环境）
	SyncReplicas       int           // 关键写 WAIT 的从库数（0 = 不同步）
	SyncTimeout        time.Duration // WAIT 等待上限（0 = 2s）
}

// Parse 解析 actor.directory 配置：缺段或 backend 为空 = etcd（保持现网行为）；
// 未知后端、Redis 缺 endpoints、时长非法、Redis 后端 grace < 复制滞后都在这里报错——
// 装配期失败优于运行期静默错配。
func Parse(a *configspb.Actor) (Options, error) {
	dir := a.GetDirectory()
	opts := Options{Backend: Backend(strings.ToLower(strings.TrimSpace(dir.GetBackend())))}
	if opts.Backend == "" {
		opts.Backend = BackendEtcd
	}
	grace, err := config.ParseDuration(dir.GetQuiesceGrace())
	if err != nil {
		return Options{}, fmt.Errorf("directory: actor.directory.quiesce_grace 非法: %w", err)
	}
	lag, err := config.ParseDuration(dir.GetReplicationLag())
	if err != nil {
		return Options{}, fmt.Errorf("directory: actor.directory.replication_lag 非法: %w", err)
	}
	opts.QuiesceGrace, opts.ReplicationLag = grace, lag

	switch opts.Backend {
	case BackendEtcd:
		return opts, nil
	case BackendRedis:
		if err := opts.parseRedis(dir.GetRedis()); err != nil {
			return Options{}, err
		}
		if err := cluster.ValidateReplicationGrace(EffectiveGrace(opts.QuiesceGrace), opts.ReplicationLag); err != nil {
			return Options{}, fmt.Errorf("directory: %w", err)
		}
		return opts, nil
	default:
		return Options{}, fmt.Errorf("directory: 未知 actor 目录后端 %q（只支持 etcd | redis）", opts.Backend)
	}
}

// parseRedis 解析 Redis 连接段（endpoints 必填，其余可选）。
func (o *Options) parseRedis(r *configspb.DirectoryRedis) error {
	o.Backend = normalize(o.Backend)
	endpoints := make([]string, 0, len(r.GetEndpoints()))
	for _, ep := range r.GetEndpoints() {
		if ep = strings.TrimSpace(ep); ep != "" {
			endpoints = append(endpoints, ep)
		}
	}
	if len(endpoints) == 0 {
		return fmt.Errorf("directory: backend=redis 时 actor.directory.redis.endpoints 不能为空")
	}
	timeout, err := config.ParseDuration(r.GetSyncTimeout())
	if err != nil {
		return fmt.Errorf("directory: actor.directory.redis.sync_timeout 非法: %w", err)
	}
	if timeout == 0 {
		timeout = defaultSyncTimeout
	}
	if r.GetSyncReplicas() < 0 {
		return fmt.Errorf("directory: actor.directory.redis.sync_replicas 不能为负（当前 %d）", r.GetSyncReplicas())
	}
	o.Redis = RedisOptions{
		Endpoints:          endpoints,
		MasterName:         strings.TrimSpace(r.GetMasterName()),
		Password:           r.GetPassword(),
		DB:                 int(r.GetDb()),
		TLS:                r.GetTls(),
		InsecureSkipVerify: r.GetInsecureSkipVerify(),
		SyncReplicas:       int(r.GetSyncReplicas()),
		SyncTimeout:        timeout,
	}
	return nil
}

// EffectiveGrace 返回实际生效的 Claim 保护窗：0 表示未配置，取框架缺省。
func EffectiveGrace(grace time.Duration) time.Duration {
	if grace > 0 {
		return grace
	}
	return cluster.DefaultLease().QuiesceGrace
}

// normalize 把零值归一为 etcd：直接构造 Options 的调用方（示例/测试/嵌入）不设置 Backend，
// 与「缺省 etcd、保持现网行为」同义；未知非空值不在这里兜底（由 Open/Parse 报错）。
func normalize(backend Backend) Backend {
	if backend == "" {
		return BackendEtcd
	}
	return backend
}

// Prefix 返回目录键前缀：etcd 用 /atlas/actors/<ns>，redis 用 atlas:<ns>:actors。
// 两者语义一一对应（命名空间内的一块目录空间），全部由 namespace.Derive 派生，
// 本包不拼任何前缀字面量。
func Prefix(backend Backend, derived namespace.Derived) string {
	if normalize(backend) == BackendRedis {
		return derived.RedisKeyPrefix + "actors"
	}
	return derived.EtcdDirectory
}

// Open 按配置构造目录 locator 并返回释放函数。
//
// etcd 后端复用装配层已建好的客户端（避免同配置开两条连接池）；
// Redis 后端自建客户端，释放函数负责关闭它（调用方在进程/组件退出时调用）。
func Open(opts Options, etcdClient *clientv3.Client, derived namespace.Derived) (locator.Locator, func(), error) {
	switch normalize(opts.Backend) {
	case BackendEtcd:
		if etcdClient == nil {
			return nil, nil, fmt.Errorf("directory: backend=etcd 时 etcd 客户端不能为空")
		}
		loc, err := etcdlocator.NewLocator(etcdClient, etcdlocator.WithPrefix(Prefix(BackendEtcd, derived)))
		if err != nil {
			return nil, nil, fmt.Errorf("directory: 构造 etcd locator: %w", err)
		}
		return loc, func() {}, nil
	case BackendRedis:
		client, err := NewRedisClient(opts.Redis)
		if err != nil {
			return nil, nil, err
		}
		locOpts := []redislocator.Option{redislocator.WithPrefix(Prefix(BackendRedis, derived))}
		if opts.Redis.SyncReplicas > 0 {
			locOpts = append(locOpts, redislocator.WithSyncReplication(opts.Redis.SyncReplicas, opts.Redis.SyncTimeout))
		}
		loc, err := redislocator.NewLocator(client, locOpts...)
		if err != nil {
			_ = client.Close()
			return nil, nil, fmt.Errorf("directory: 构造 redis locator: %w", err)
		}
		return loc, func() { _ = client.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("directory: 未知 actor 目录后端 %q", opts.Backend)
	}
}

// NewRedisClient 按配置构造 Redis 客户端（惰性连接：构造期不验证可达性）。
// MasterName 非空走哨兵（主从切换自动跟随），否则单点直连 Endpoints[0]。
func NewRedisClient(o RedisOptions) (goredis.UniversalClient, error) {
	if len(o.Endpoints) == 0 {
		return nil, fmt.Errorf("directory: redis endpoints 不能为空")
	}
	opts := &goredis.UniversalOptions{
		Addrs:      o.Endpoints,
		MasterName: o.MasterName,
		Password:   o.Password,
		DB:         o.DB,
	}
	if o.TLS {
		// #nosec G402 —— 跳过证书校验只在测试环境显式开启（配置项 insecure_skip_verify）。
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: o.InsecureSkipVerify}
	}
	return goredis.NewUniversalClient(opts), nil
}
