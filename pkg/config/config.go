// Package config 提供游戏模板统一的配置加载约定：
// 各服务配置以 YAML 存放于 services/<service>/configs/config.yaml，
// 结构由服务 internal/conf/conf.proto 定义（Bootstrap 引用公共配置消息），
// 加载时经 yaml → json → protojson 解组到 Bootstrap。
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Load 从 path 读取 YAML 配置并解组到 v（v 须为 proto.Message，如 *conf.Bootstrap）。
func Load(path string, v proto.Message) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: 读取配置 %s 失败: %w", path, err)
	}
	// yaml → map → json → protojson：YAML 是 JSON 的超集，先归一化为 JSON。
	var m map[string]any
	if err := yaml.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("config: 解析配置 %s 失败: %w", path, err)
	}
	j, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("config: 归一化配置 %s 失败: %w", path, err)
	}
	if err := protojson.Unmarshal(j, v); err != nil {
		return fmt.Errorf("config: 解组配置 %s 失败: %w", path, err)
	}
	return nil
}

// FromService 按服务名加载 services/<name>/configs/config.yaml
// （相对仓库根运行；服务内配置自包含，路径约定见包注释）。
func FromService(name string, v proto.Message) error {
	if name == "" {
		return fmt.Errorf("config: 服务名不能为空")
	}
	return Load(filepath.Join("services", name, "configs", "config.yaml"), v)
}
