package config

import (
	"errors"
	"path/filepath"
	"testing"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	atlasconfig "github.com/huangyuCN/atlas/config"
)

// 静态保证 Provider 实现框架 config.Config（接口是终态，本仓只做适配）。
var _ atlasconfig.Config = (*Provider)(nil)

// TestProviderLoadUnknownKey 验证未知 key（与空 key）返回框架的 ErrKeyNotFound，
// 语义与框架适配器一致（可用 errors.Is 判定）。
func TestProviderLoadUnknownKey(t *testing.T) {
	p, err := NewProvider(writeConf(t, "runtime:\n  name: demo\n"))
	if err != nil {
		t.Fatalf("NewProvider() 错误 = %v", err)
	}
	for _, key := range []string{"", "nope", "runtime.nope", "server.grpc.nope"} {
		var v configspb.Runtime
		err := p.Load(key, &v)
		if !errors.Is(err, atlasconfig.ErrKeyNotFound) {
			t.Errorf("Load(%q) 错误 = %v，期望 ErrKeyNotFound", key, err)
		}
	}
}

// TestProviderLoadProtoAndStruct 验证 Load 两条解组路径：
// proto.Message 走 protojson（不忽略未知字段），普通结构体走 encoding/json。
func TestProviderLoadProtoAndStruct(t *testing.T) {
	p, err := NewProvider(writeConf(t, "server:\n  grpc:\n    edge_addr: 127.0.0.1:9100\n    internal_addr: 127.0.0.1:9101\n"))
	if err != nil {
		t.Fatalf("NewProvider() 错误 = %v", err)
	}
	var grpc configspb.Server_GRPC
	if err := p.Load("server.grpc", &grpc); err != nil {
		t.Fatalf("Load(server.grpc) 错误 = %v", err)
	}
	if grpc.GetEdgeAddr() != "127.0.0.1:9100" || grpc.GetInternalAddr() != "127.0.0.1:9101" {
		t.Fatalf("proto 解组结果不符: %+v", &grpc)
	}
	var plain struct {
		EdgeAddr string `json:"edge_addr"`
	}
	if err := p.Load("server.grpc", &plain); err != nil {
		t.Fatalf("Load(server.grpc, struct) 错误 = %v", err)
	}
	if plain.EdgeAddr != "127.0.0.1:9100" {
		t.Fatalf("结构体解组结果不符: %+v", plain)
	}
}

// TestProviderLoadRejectsUnknownField 验证节点内拼错的字段不被静默忽略（protojson 严格解组）。
func TestProviderLoadRejectsUnknownField(t *testing.T) {
	p, err := NewProvider(writeConf(t, "runtime:\n  name: demo\n  typo_field: x\n"))
	if err != nil {
		t.Fatalf("NewProvider() 错误 = %v", err)
	}
	var v configspb.Runtime
	if err := p.Load("runtime", &v); err == nil {
		t.Fatal("未知字段期望报错（静默忽略会让「配了没生效」从缝里溜进来）")
	}
}

// TestProviderLoadRejectsBadYAML 验证 YAML 解析失败与文件缺失都在读取期报错。
func TestProviderLoadRejectsBadYAML(t *testing.T) {
	p, err := NewProvider(writeConf(t, "server: [unbalanced"))
	if err != nil {
		t.Fatalf("NewProvider() 错误 = %v", err)
	}
	var v configspb.Server
	if err := p.Load("server", &v); err == nil {
		t.Fatal("非法 YAML 期望报错")
	}
	missing, err := NewProvider(filepath.Join(t.TempDir(), "nope.yaml"))
	if err != nil {
		t.Fatalf("NewProvider() 错误 = %v", err)
	}
	if err := missing.Load("runtime", &v); err == nil {
		t.Fatal("文件不存在期望报错")
	}
}

// TestProviderWatchUnsupported 验证 Watch 明确报错而不是静默 no-op
// （自包含 YAML 没有热重载后端）。
func TestProviderWatchUnsupported(t *testing.T) {
	p, err := NewProvider(writeConf(t, "runtime:\n  name: demo\n"))
	if err != nil {
		t.Fatalf("NewProvider() 错误 = %v", err)
	}
	if err := p.Watch("runtime", func([]byte) {}); !errors.Is(err, ErrWatchUnsupported) {
		t.Fatalf("Watch() 错误 = %v，期望 ErrWatchUnsupported", err)
	}
}

// TestNewProviderRejectsEmpty 验证空路径/空服务名在构造期报错，服务路径约定与 FromService 同源。
func TestNewProviderRejectsEmpty(t *testing.T) {
	if _, err := NewProvider(""); err == nil {
		t.Error("空路径期望报错")
	}
	if _, err := NewServiceProvider(""); err == nil {
		t.Error("空服务名期望报错")
	}
	p, err := NewServiceProvider("game")
	if err != nil {
		t.Fatalf("NewServiceProvider(game) 错误 = %v", err)
	}
	if p.path != filepath.Join("services", "game", "configs", "config.yaml") {
		t.Fatalf("服务配置路径 = %q", p.path)
	}
}
