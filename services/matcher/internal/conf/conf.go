// Package conf 定义 matcher 服务的配置结构。
package conf

import "github.com/huangyuCN/atlas-game-layout/lib/confbase"

// Config 是 matcher 服务配置：内嵌公共底座 + 服务特有字段。
type Config struct {
	confbase.Base `yaml:",inline"`

	GRPC AddrConf `yaml:"grpc"`
	HTTP AddrConf `yaml:"http"`
}

// AddrConf 是监听地址配置。
type AddrConf struct {
	Addr string `yaml:"addr"`
}

// BaseConfig 返回公共底座（bootstrap 装配使用）。
func (c *Config) BaseConfig() *confbase.Base { return &c.Base }
