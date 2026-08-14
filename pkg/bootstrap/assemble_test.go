package bootstrap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/lib/confbase"
)

// testConf 是内嵌配置底座的测试配置。
type testConf struct {
	confbase.Base `yaml:",inline"`
}

// BaseConfig 返回配置底座。
func (c *testConf) BaseConfig() *confbase.Base { return &c.Base }

// writeConf 在 dir 下生成 services/demo/configs/config.yaml 并切换工作目录。
func writeConf(t *testing.T, dir, content string) {
	t.Helper()
	confDir := filepath.Join(dir, "services", "demo", "configs")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatalf("创建服务配置目录失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(confDir, "config.yaml"), []byte(content), 0o644); err != nil {
		t.Fatalf("写入配置失败: %v", err)
	}
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("获取当前目录失败: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("切换目录失败: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })
}

// TestAssembleLoadsConfig 验证配置加载、日志初始化与模块组装。
func TestAssembleLoadsConfig(t *testing.T) {
	dir := t.TempDir()
	writeConf(t, dir, "name: demo\nid: demo-1\nlog:\n  level: error\n")

	var cfg testConf
	opts, err := Assemble("demo", &cfg)
	if err != nil {
		t.Fatalf("Assemble() 错误 = %v", err)
	}
	if len(opts) < 2 {
		t.Fatalf("Assemble() 应至少返回供应与 App 模块，实际 %d 个", len(opts))
	}
	if cfg.Name != "demo" || cfg.ID != "demo-1" {
		t.Fatalf("配置未正确加载: %+v", cfg)
	}
}

// TestAssembleWithEtcd 验证配置 etcd 后追加客户端与注册器模块。
func TestAssembleWithEtcd(t *testing.T) {
	dir := t.TempDir()
	writeConf(t, dir, "name: demo\netcd:\n  endpoints:\n    - 127.0.0.1:12379\n")

	var cfg testConf
	opts, err := Assemble("demo", &cfg)
	if err != nil {
		t.Fatalf("Assemble() 错误 = %v", err)
	}
	if len(opts) != 4 {
		t.Fatalf("含 etcd 时应为 4 个模块（供应/etcd/registry/App 打包模块），实际 %d 个", len(opts))
	}
}

// TestAssembleEmptyName 验证空服务名报错。
func TestAssembleEmptyName(t *testing.T) {
	var cfg testConf
	if _, err := Assemble("", &cfg); err == nil {
		t.Fatal("Assemble() 期望错误，实际为 nil")
	}
}

// TestAssembleMissingFile 验证配置缺失报错。
func TestAssembleMissingFile(t *testing.T) {
	var cfg testConf
	if _, err := Assemble("demo", &cfg); err == nil {
		t.Fatal("Assemble() 期望配置加载错误，实际为 nil")
	}
}
