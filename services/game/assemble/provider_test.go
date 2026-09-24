package assemble

import (
	"path/filepath"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestProviderEquivalentToFullLoad 是自包含 YAML 的回归金标准：逐节（config.Provider.Load）
// 读出的 Bootstrap 必须与改造前的一次性 Load（yaml → JSON → protojson 解整份）**逐字段等价**。
// 放在服务装配包而非 pkg/config：services/*/internal/conf 是 internal 包，只有服务自身能引用。
func TestProviderEquivalentToFullLoad(t *testing.T) {
	// 测试工作目录是本包（services/game/assemble）。
	path := filepath.Join("..", "configs", "config.yaml")
	want := &conf.Bootstrap{}
	if err := config.Load(path, want); err != nil {
		t.Fatalf("Load() 错误 = %v", err)
	}
	p, err := config.NewProvider(path)
	if err != nil {
		t.Fatalf("NewProvider() 错误 = %v", err)
	}
	got := &conf.Bootstrap{}
	// 段名 = YAML 顶层键 = Bootstrap 字段 JSON 名；缺节即 ErrKeyNotFound 失败（不允许静默漏节）。
	fds := got.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fds.Len(); i++ {
		fd := fds.Get(i)
		child := got.ProtoReflect().NewField(fd).Message().Interface()
		if err := p.Load(fd.JSONName(), child); err != nil {
			t.Fatalf("Provider.Load(%q) 错误 = %v", fd.JSONName(), err)
		}
		got.ProtoReflect().Set(fd, protoreflect.ValueOfMessage(child.ProtoReflect()))
	}
	if !proto.Equal(got, want) {
		t.Fatalf("Provider 逐节读取与一次性 Load 不等价:\n got = %v\nwant = %v", got, want)
	}
}
