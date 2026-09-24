package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	atlasconfig "github.com/huangyuCN/atlas/config"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// ErrWatchUnsupported 是 Provider.Watch 的哨兵错误：自包含 YAML 只读、没有热重载后端，
// 明确报错而不是静默 no-op——静默 no-op 会让调用方以为订阅成功（与框架适配器
// 「空 key/空回调不监听」的宽松语义不同，本包按「无后端即报错」处理）。
var ErrWatchUnsupported = errors.New("config: 自包含 YAML 只读，无热重载后端，不支持 Watch")

// Provider 是框架 github.com/huangyuCN/atlas/config 接口的只读适配器：
// Load 映射到「按节读取自己那一份自包含 YAML」（key 为段路径，如 runtime / server.grpc / data.redis），
// 不引入任何合并层（不读 configs/common.yaml、不做深合并）。
type Provider struct {
	path string
}

// 静态保证实现框架 Config 接口（框架接口是终态，本仓只做适配）。
var _ atlasconfig.Config = (*Provider)(nil)

// NewProvider 构造只读 path 的配置提供者；path 为空即报错（避免「读了个空路径」到调用期才暴露）。
func NewProvider(path string) (*Provider, error) {
	if path == "" {
		return nil, fmt.Errorf("config: 配置路径不能为空")
	}
	return &Provider{path: path}, nil
}

// NewServiceProvider 按服务名构造提供者：路径约定与 FromService 同源
// （services/<name>/configs/config.yaml，相对仓库根运行）。
func NewServiceProvider(name string) (*Provider, error) {
	if name == "" {
		return nil, fmt.Errorf("config: 服务名不能为空")
	}
	return NewProvider(filepath.Join("services", name, "configs", "config.yaml"))
}

// Load 读取本服务 YAML 的一个配置节点并解组到 v：
// key 是段路径（如 "runtime"、"server.grpc"、"data.redis"），空 key 或节点不存在返回
// atlasconfig.ErrKeyNotFound（与框架适配器语义一致，可用 errors.Is 判定）；
// v 为 proto.Message 时走 protojson（不忽略未知字段，拼错即报错），否则走 encoding/json。
func (p *Provider) Load(key string, v any) error {
	if key == "" {
		return atlasconfig.ErrKeyNotFound
	}
	node, err := p.node(key)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("%w: %s", atlasconfig.ErrKeyNotFound, key)
	}
	raw, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("config: 归一化配置节点 %s 失败: %w", key, err)
	}
	if msg, ok := v.(proto.Message); ok {
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("config: 解组配置节点 %s 失败: %w", key, err)
		}
		return nil
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("config: 解组配置节点 %s 失败: %w", key, err)
	}
	return nil
}

// Watch 明确报错：自包含 YAML 没有热重载后端，本实现不提供订阅能力
// （返回 ErrWatchUnsupported，绝不静默 no-op）。
func (p *Provider) Watch(_ string, _ func(data []byte)) error {
	return fmt.Errorf("%w（%s）", ErrWatchUnsupported, p.path)
}

// node 按段路径取出 YAML 节点（路径不存在返回 nil, nil；读取/解析失败原样报错）。
func (p *Provider) node(key string) (any, error) {
	data, err := os.ReadFile(p.path)
	if err != nil {
		return nil, fmt.Errorf("config: 读取配置 %s 失败: %w", p.path, err)
	}
	var root map[string]any
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("config: 解析配置 %s 失败: %w", p.path, err)
	}
	var cur any = root
	for _, seg := range strings.Split(key, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, nil
		}
		if cur, ok = m[seg]; !ok {
			return nil, nil
		}
	}
	return cur, nil
}
