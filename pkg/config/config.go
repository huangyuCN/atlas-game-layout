// Package config 提供游戏模板统一的配置加载约定：
// 各服务配置以 YAML 存放于 services/<service>/configs/config.yaml（服务内自包含），
// 经 Atlas 编码注册表解码，FromService 相对仓库根定位。
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/huangyuCN/atlas/encoding"
	_ "github.com/huangyuCN/atlas/encoding/yaml" // 注册 yaml 编解码器
)

// Load 从 path 读取 YAML 配置并解组到 v（v 须为结构体指针）。
func Load(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: 读取配置 %s 失败: %w", path, err)
	}
	codec := encoding.GetCodec("yaml")
	if codec == nil {
		return fmt.Errorf("config: yaml 编解码器未注册")
	}
	if err := codec.Unmarshal(data, v); err != nil {
		return fmt.Errorf("config: 解析配置 %s 失败: %w", path, err)
	}
	return nil
}

// FromService 按服务名加载 services/<name>/configs/config.yaml
//（相对仓库根运行；服务内配置自包含，路径约定见包注释）。
func FromService(name string, v any) error {
	if name == "" {
		return fmt.Errorf("config: 服务名不能为空")
	}
	return Load(filepath.Join("services", name, "configs", "config.yaml"), v)
}
