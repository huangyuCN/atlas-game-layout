package config

import (
	"os"
	"path/filepath"
	"testing"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
)

// TestLoad 验证 YAML 配置解组到 proto 消息（含嵌套与数组）。
func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conf.yaml")
	data := `name: demo
id: demo-1
version: v1.0.0
env: test
`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("写入测试配置失败: %v", err)
	}

	var got configspb.Runtime
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load() 错误 = %v", err)
	}
	if got.Name != "demo" || got.Id != "demo-1" || got.Version != "v1.0.0" || got.Env != "test" {
		t.Fatalf("解组结果不符合预期: %+v", &got)
	}
}

// TestLoadNested 验证嵌套消息（Registry.Etcd.Endpoints）解组。
func TestLoadNested(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conf.yaml")
	data := `etcd:
  endpoints:
    - 127.0.0.1:12379
    - 127.0.0.1:22379
`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("写入测试配置失败: %v", err)
	}

	var got configspb.Registry
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load() 错误 = %v", err)
	}
	eps := got.GetEtcd().GetEndpoints()
	if len(eps) != 2 || eps[0] != "127.0.0.1:12379" {
		t.Fatalf("嵌套解组结果不符合预期: %+v", &got)
	}
}

// TestLoadMissingFile 验证文件不存在时报错。
func TestLoadMissingFile(t *testing.T) {
	var v configspb.Runtime
	if err := Load(filepath.Join(t.TempDir(), "nope.yaml"), &v); err == nil {
		t.Fatal("Load() 期望错误，实际为 nil")
	}
}

// TestLoadBadFormat 验证非法 YAML 报错。
func TestLoadBadFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conf.yaml")
	if err := os.WriteFile(path, []byte("server: [unbalanced"), 0o644); err != nil {
		t.Fatalf("写入坏配置失败: %v", err)
	}
	var v configspb.Runtime
	if err := Load(path, &v); err == nil {
		t.Fatal("Load() 期望解析错误，实际为 nil")
	}
}

// TestLoadUnknownField 验证未知字段报错（protojson 默认拒绝未知字段，防配置笔误）。
func TestLoadUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conf.yaml")
	if err := os.WriteFile(path, []byte("name: demo\ntypo_field: x\n"), 0o644); err != nil {
		t.Fatalf("写入配置失败: %v", err)
	}
	var v configspb.Runtime
	if err := Load(path, &v); err == nil {
		t.Fatal("Load() 期望未知字段错误，实际为 nil")
	}
}

// TestFromService 验证按服务名从服务目录加载的路径约定。
func TestFromService(t *testing.T) {
	dir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("获取当前目录失败: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("切换目录失败: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(oldWD) })

	confDir := filepath.Join("services", "demo", "configs")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatalf("创建服务配置目录失败: %v", err)
	}
	data := "name: demo\nid: demo-1\n"
	if err := os.WriteFile(filepath.Join(confDir, "config.yaml"), []byte(data), 0o644); err != nil {
		t.Fatalf("写入服务配置失败: %v", err)
	}

	var got configspb.Runtime
	if err := FromService("demo", &got); err != nil {
		t.Fatalf("FromService() 错误 = %v", err)
	}
	if got.Name != "demo" {
		t.Fatalf("FromService 加载结果不符合预期: %+v", &got)
	}
}
