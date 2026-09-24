package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
)

// writeConf 把 YAML 写入临时文件并返回路径（运行时配置测试共用）。
func writeConf(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "conf.yaml")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("写入测试配置失败: %v", err)
	}
	return path
}

// TestLoadRuntimeClientVersionGate 验证 M1 客户端版本门槛字段可解组（枚举按名往返）。
func TestLoadRuntimeClientVersionGate(t *testing.T) {
	path := writeConf(t, `name: gateway
min_client_version: 1.4.0
min_client_version_mode: MIN_CLIENT_VERSION_MODE_ENFORCE
`)
	var got configspb.Runtime
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load() 错误 = %v", err)
	}
	if got.GetMinClientVersion() != "1.4.0" {
		t.Fatalf("min_client_version = %q, 期望 1.4.0", got.GetMinClientVersion())
	}
	want := configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_ENFORCE
	if got.GetMinClientVersionMode() != want {
		t.Fatalf("min_client_version_mode = %v, 期望 %v", got.GetMinClientVersionMode(), want)
	}
}

// TestLoadRuntimeClientVersionModeDefault 验证缺省模式为 OFF 且门槛为空（不设门槛、不误拒）。
func TestLoadRuntimeClientVersionModeDefault(t *testing.T) {
	var got configspb.Runtime
	if err := Load(writeConf(t, "name: gateway\n"), &got); err != nil {
		t.Fatalf("Load() 错误 = %v", err)
	}
	if got.GetMinClientVersion() != "" {
		t.Fatalf("min_client_version 缺省 = %q, 期望空", got.GetMinClientVersion())
	}
	if got.GetMinClientVersionMode() != configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_OFF {
		t.Fatalf("缺省模式 = %v, 期望 OFF", got.GetMinClientVersionMode())
	}
}

// TestLoadRuntimeClientVersionModeInvalid 验证非法枚举名报错（不静默回落为 OFF）。
func TestLoadRuntimeClientVersionModeInvalid(t *testing.T) {
	path := writeConf(t, "min_client_version_mode: MIN_CLIENT_VERSION_MODE_FAST\n")
	var got configspb.Runtime
	err := Load(path, &got)
	if err == nil {
		t.Fatal("Load() 期望错误，实际为 nil")
	}
	// protojson 报错用 JSON 名（camelCase），两种写法都接受，只要求指出是哪个字段。
	if !strings.Contains(err.Error(), "minClientVersionMode") &&
		!strings.Contains(err.Error(), "min_client_version_mode") {
		t.Fatalf("错误信息应指出字段名，实际 = %v", err)
	}
}

// TestLoadRuntimeNamespace 验证唯一命名空间字段 runtime.namespace 可解组（改名后的正例）。
func TestLoadRuntimeNamespace(t *testing.T) {
	path := writeConf(t, `name: game
id: game-1
version: v9.9.9
env: test
namespace: testns
`)
	var got configspb.Runtime
	if err := Load(path, &got); err != nil {
		t.Fatalf("Load() 错误 = %v", err)
	}
	if got.GetName() != "game" || got.GetId() != "game-1" || got.GetVersion() != "v9.9.9" ||
		got.GetEnv() != "test" || got.GetNamespace() != "testns" {
		t.Fatalf("存量字段解组结果不符合预期: %+v", &got)
	}
}

// TestLoadRuntimeRejectsLegacyNamespaceKeys 验证旧命名空间字段名不再被接受：
// protojson 不忽略未知字段，故旧名 actor_namespace/actorNamespace 与已删除的
// registry.namespace 都必须**解组失败**——静默忽略会让「配了没生效」从缝里溜进来（R9）。
func TestLoadRuntimeRejectsLegacyNamespaceKeys(t *testing.T) {
	runtimeCases := []struct{ name, yaml string }{
		{"旧名 actor_namespace", "name: game\nactor_namespace: testns\n"},
		{"旧 JSON 名 actorNamespace", "name: game\nactorNamespace: testns\n"},
	}
	for _, c := range runtimeCases {
		t.Run(c.name, func(t *testing.T) {
			var got configspb.Runtime
			if err := Load(writeConf(t, c.yaml), &got); err == nil {
				t.Fatalf("Load() 期望未知字段报错，实际 nil（%s 被静默接受）", c.name)
			}
		})
	}

	// registry.namespace 已删除（与 runtime.namespace 重复）：注册段里出现它必须报错。
	t.Run("已删字段 registry.namespace", func(t *testing.T) {
		var got configspb.Registry
		if err := Load(writeConf(t, "namespace: /atlas/services/test\n"), &got); err == nil {
			t.Fatal("Load() 期望未知字段报错，实际 nil（registry.namespace 被静默接受）")
		}
	})
}

// TestLoadRegistryEtcdUnchanged 验证注册段存量字段零变化（删 namespace 不影响 etcd/ttl）。
func TestLoadRegistryEtcdUnchanged(t *testing.T) {
	var got configspb.Registry
	if err := Load(writeConf(t, "etcd:\n  endpoints:\n    - 127.0.0.1:12379\nttl: 15s\n"), &got); err != nil {
		t.Fatalf("Load() 错误 = %v", err)
	}
	if eps := got.GetEtcd().GetEndpoints(); len(eps) != 1 || eps[0] != "127.0.0.1:12379" {
		t.Fatalf("registry.etcd.endpoints = %v", eps)
	}
	if got.GetTtl() != "15s" {
		t.Fatalf("registry.ttl = %q, 期望 15s", got.GetTtl())
	}
}
