package bootstrap

import "testing"

// TestActorNamespaceOf 验证 actor 命名空间派生：取注册中心前缀的叶子段；
// 非法字符**报错而非归一**（静默归一会把两套应隔离的部署塌缩到同一命名空间）；
// 空前缀回落 default。
func TestActorNamespaceOf(t *testing.T) {
	tests := []struct {
		name       string
		registryNS string
		want       string
		wantErr    bool
	}{
		{"取叶子段", "/atlas/services/it-1790", "it-1790", false},
		{"无斜杠按整体", "env-a", "env-a", false},
		{"空回落默认", "", DefaultActorNamespace, false},
		{"斜杠结尾取空段回落默认", "/atlas/services/", DefaultActorNamespace, false},
		{"点号非法（不归一）", "/atlas/services/e2e.1790", "", true},
		{"空格非法", "/atlas/services/e2e 1790", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ActorNamespaceOf(tt.registryNS)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ActorNamespaceOf(%q) 期望报错，实际 %q", tt.registryNS, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ActorNamespaceOf(%q): %v", tt.registryNS, err)
			}
			if got != tt.want {
				t.Fatalf("ActorNamespaceOf(%q) = %q, want %q", tt.registryNS, got, tt.want)
			}
		})
	}
}
