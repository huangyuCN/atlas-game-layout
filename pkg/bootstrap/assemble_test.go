package bootstrap

import (
	"os"
	"path/filepath"
	"testing"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
)

// testBootstrap 是手写的测试配置：内嵌 Runtime 提供 proto.Message，
// 另存 Registry/Log 实现 ConfigLike（无需 protojson 解组）。
type testBootstrap struct {
	*configspb.Runtime
	reg *configspb.Registry
	lg  *configspb.Log
}

func (b *testBootstrap) GetRuntime() *configspb.Runtime   { return b.Runtime }
func (b *testBootstrap) GetRegistry() *configspb.Registry { return b.reg }
func (b *testBootstrap) GetLog() *configspb.Log           { return b.lg }

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

// TestAssembleLoaded 验证装配分支：日志初始化、模块数量。
// 注册中心装配已下沉到各服务 fx 模块（pkg/fxkit），
// 因此无论是否配置 registry.etcd，bootstrap 均产出固定数量的模块。
func TestAssembleLoaded(t *testing.T) {
	cfg := &testBootstrap{
		Runtime: &configspb.Runtime{Name: "demo", Id: "demo-1"},
		lg:      &configspb.Log{Level: "error"},
	}
	opts, err := AssembleLoaded(cfg)
	if err != nil {
		t.Fatalf("AssembleLoaded() 错误 = %v", err)
	}
	if len(opts) != 2 {
		t.Fatalf("应为 2 个模块（供应/App 打包模块），实际 %d 个", len(opts))
	}
}

// TestAssembleLoadedIgnoresRegistry 验证 bootstrap 不再处理 etcd 配置段：
// 含 registry.etcd 的配置同样只产出固定模块（etcd 由服务模块自行声明）。
func TestAssembleLoadedIgnoresRegistry(t *testing.T) {
	cfg := &testBootstrap{
		Runtime: &configspb.Runtime{Name: "demo"},
		reg: &configspb.Registry{
			Etcd: &configspb.Registry_Etcd{Endpoints: []string{"127.0.0.1:12379"}},
		},
	}
	opts, err := AssembleLoaded(cfg)
	if err != nil {
		t.Fatalf("AssembleLoaded() 错误 = %v", err)
	}
	if len(opts) != 2 {
		t.Fatalf("registry 段不应改变模块数量，期望 2 个，实际 %d 个", len(opts))
	}
}

// TestAssembleLoadedMissingRuntime 验证缺少 runtime.name 报错。
func TestAssembleLoadedMissingRuntime(t *testing.T) {
	cfg := &testBootstrap{Runtime: &configspb.Runtime{}}
	if _, err := AssembleLoaded(cfg); err == nil {
		t.Fatal("AssembleLoaded() 期望缺少 runtime.name 错误，实际为 nil")
	}
}

// TestAssembleLoadedBadType 验证未引用公共配置消息的类型报错。
func TestAssembleLoadedBadType(t *testing.T) {
	if _, err := AssembleLoaded(&configspb.Runtime{Name: "x"}); err == nil {
		t.Fatal("AssembleLoaded() 期望类型错误，实际为 nil")
	}
}

// TestAssembleEmptyName 验证空服务名报错。
func TestAssembleEmptyName(t *testing.T) {
	var cfg testBootstrap
	if _, err := Assemble("", &cfg); err == nil {
		t.Fatal("Assemble() 期望错误，实际为 nil")
	}
}

// TestAssembleMissingFile 验证配置缺失报错。
func TestAssembleMissingFile(t *testing.T) {
	var cfg testBootstrap
	if _, err := Assemble("demo", &cfg); err == nil {
		t.Fatal("Assemble() 期望配置加载错误，实际为 nil")
	}
}
