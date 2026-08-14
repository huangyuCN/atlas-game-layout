package config

import (
	"os"
	"path/filepath"
	"testing"
)

// 测试配置结构：覆盖基础类型与嵌套结构。
type testConf struct {
	Name string `yaml:"name"`
	ID   string `yaml:"id"`
	HTTP struct {
		Addr string `yaml:"addr"`
	} `yaml:"http"`
	Endpoints []string `yaml:"endpoints"`
}

// TestLoad 验证 YAML 配置加载到结构体。
func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conf.yaml")
	data := `name: demo
id: demo-1
http:
  addr: 0.0.0.0:8080
endpoints:
  - grpc://127.0.0.1:9000
`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("写入测试配置失败: %v", err)
	}

	var got testConf
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load() 错误 = %v", err)
	}
	if got.Name != "demo" || got.ID != "demo-1" {
		t.Fatalf("基础字段不符合预期: %+v", got)
	}
	if got.HTTP.Addr != "0.0.0.0:8080" {
		t.Fatalf("嵌套字段不符合预期: %+v", got)
	}
	if len(got.Endpoints) != 1 || got.Endpoints[0] != "grpc://127.0.0.1:9000" {
		t.Fatalf("切片字段不符合预期: %+v", got)
	}
}

// TestLoadMissingFile 验证文件不存在时报错。
func TestLoadMissingFile(t *testing.T) {
	var v testConf
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
	var v testConf
	if err := Load(path, &v); err == nil {
		t.Fatal("Load() 期望解析错误，实际为 nil")
	}
}

// TestFromService 验证按服务名从 configs 目录加载的路径约定。
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

	if err := os.MkdirAll("configs", 0o755); err != nil {
		t.Fatalf("创建 configs 目录失败: %v", err)
	}
	data := "name: demo\nid: demo-1\n"
	if err := os.WriteFile("configs/demo.yaml", []byte(data), 0o644); err != nil {
		t.Fatalf("写入服务配置失败: %v", err)
	}

	var got testConf
	if err := FromService("demo", &got); err != nil {
		t.Fatalf("FromService() 错误 = %v", err)
	}
	if got.Name != "demo" {
		t.Fatalf("FromService 加载结果不符合预期: %+v", got)
	}
}
