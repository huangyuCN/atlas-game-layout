// Package confbase 定义各服务配置的公共底座：
// 服务配置结构内嵌 Base（yaml inline），共享服务名/中间件/日志字段。
package confbase

// Base 是各服务配置的公共字段底座。
type Base struct {
	// Name 是服务名（注册中心中的服务名）。
	Name string `yaml:"name"`
	// ID 是实例 ID。
	ID string `yaml:"id"`
	// Etcd 是 etcd 注册中心配置。
	Etcd EtcdConf `yaml:"etcd"`
	// Redis 是 Redis 配置。
	Redis RedisConf `yaml:"redis"`
	// Nats 是 NATS 配置。
	Nats NatsConf `yaml:"nats"`
	// Log 是日志配置。
	Log LogConf `yaml:"log"`
}

// EtcdConf 是 etcd 连接配置。
type EtcdConf struct {
	Endpoints []string `yaml:"endpoints"`
}

// RedisConf 是 Redis 连接配置。
type RedisConf struct {
	Addr string `yaml:"addr"`
}

// NatsConf 是 NATS 连接配置。
type NatsConf struct {
	URL string `yaml:"url"`
}

// LogConf 是日志配置。
type LogConf struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}
