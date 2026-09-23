package actor

import (
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"testing"

	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// TestNewRuntimeValidation 验证装配参数校验（无需外部服务）。
func TestNewRuntimeValidation(t *testing.T) {
	cases := []struct {
		name string
		opts Options
	}{
		{"空 NodeID", Options{EtcdEndpoints: []string{"x"}, NatsURL: "nats://x"}},
		{"空 EtcdEndpoints", Options{NodeID: "n1", NatsURL: "nats://x"}},
		{"空 NatsURL", Options{NodeID: "n1", EtcdEndpoints: []string{"x"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRuntime(tc.opts); err == nil {
				t.Fatal("NewRuntime() 期望参数校验错误，实际为 nil")
			}
		})
	}
}

// TestDefaultTellChainRecovery 验证 panic 保护：panic 转错误（nil ctx 安全）。
func TestDefaultTellChainRecovery(t *testing.T) {
	chain := core.ChainTell(DefaultTellChain()...)
	h := chain(func(core.ActorContext, any) error { panic("boom") })
	if err := h(nil, "msg"); err == nil {
		t.Fatal("panic 应被转换为错误")
	}
}

// TestDefaultTellChainPass 验证正常路径透传。
func TestDefaultTellChainPass(t *testing.T) {
	chain := core.ChainTell(DefaultTellChain()...)
	h := chain(func(core.ActorContext, any) error { return nil })
	if err := h(nil, "msg"); err != nil {
		t.Fatalf("正常路径错误 = %v", err)
	}
}

// TestDefaultAskChainRecovery 验证 Ask panic 保护。
func TestDefaultAskChainRecovery(t *testing.T) {
	chain := core.ChainAsk(DefaultAskChain()...)
	h := chain(func(core.ActorContext, any) (any, error) { panic("ask boom") })
	if _, err := h(nil, "req"); err == nil {
		t.Fatal("Ask panic 应被转换为错误")
	}
}

// TestDefaultChainsLength 验证默认链顺序（m[0] 最外层）。
func TestDefaultChainsLength(t *testing.T) {
	if got := len(DefaultTellChain()); got != 2 {
		t.Fatalf("DefaultTellChain 长度 = %d, 期望 2（logging/recovery）", got)
	}
	if got := len(DefaultAskChain()); got != 2 {
		t.Fatalf("DefaultAskChain 长度 = %d, 期望 2（logging/recovery）", got)
	}
}

// TestCountByType 验证按类型统计活跃 PID（拉取式在线数指标的基础）。
func TestCountByType(t *testing.T) {
	pids := []types.PID{
		mustPID(t, "player", "p1"),
		mustPID(t, "player", "p2"),
		mustPID(t, "battle", "b1"),
	}
	if got := countByType(pids, "player"); got != 2 {
		t.Fatalf("player 计数 = %d, 期望 2", got)
	}
	if got := countByType(pids, "battle"); got != 1 {
		t.Fatalf("battle 计数 = %d, 期望 1", got)
	}
	if got := countByType(pids, "ghost"); got != 0 {
		t.Fatalf("未知类型计数 = %d, 期望 0", got)
	}
	if got := countByType(nil, "player"); got != 0 {
		t.Fatalf("空快照计数 = %d, 期望 0", got)
	}
}

// mustPID 构造测试 PID（非法即失败）。
func mustPID(t *testing.T, typ, uid string) types.PID {
	t.Helper()
	pid, err := types.NewPID(typ, uid)
	if err != nil {
		t.Fatalf("NewPID(%s,%s): %v", typ, uid, err)
	}
	return pid
}

// TestNamespaceOf 验证 actor 命名空间取值优先级：runtime.actor_namespace 优先，
// 缺省取 runtime.env（两者都空时由 types.NormalizeNamespace 归一为 default）。
func TestNamespaceOf(t *testing.T) {
	tests := []struct {
		name string
		rt   *configspb.Runtime
		want string
	}{
		{"显式 actor_namespace 优先", &configspb.Runtime{Env: "test", ActorNamespace: "iso"}, "iso"},
		{"缺省取 env", &configspb.Runtime{Env: "prod"}, "prod"},
		{"两者都空", &configspb.Runtime{}, ""},
		{"runtime 为 nil", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NamespaceOf(tt.rt); got != tt.want {
				t.Fatalf("NamespaceOf() = %q, want %q", got, tt.want)
			}
		})
	}
}
