// Package conf 定义 gateway 服务的配置结构。
package conf

import "github.com/huangyuCN/atlas-game-layout/lib/confbase"

// Config 是 gateway 服务配置：内嵌公共底座 + 多协议接入端口。
type Config struct {
	confbase.Base `yaml:",inline"`

	// HTTP 是健康/管理端口。
	HTTP AddrConf `yaml:"http"`
	// TCP/WebSocket/KCP/UDP 是客户端接入协议端口（M4 里程碑启用）。
	TCP       AddrConf `yaml:"tcp"`
	WebSocket AddrConf `yaml:"websocket"`
	KCP       AddrConf `yaml:"kcp"`
	UDP       AddrConf `yaml:"udp"`
}

// AddrConf 是监听地址配置。
type AddrConf struct {
	Addr string `yaml:"addr"`
}

// BaseConfig 返回公共底座（bootstrap 装配使用）。
func (c *Config) BaseConfig() *confbase.Base { return &c.Base }
