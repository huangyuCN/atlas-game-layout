package bootstrap

import (
	"fmt"
	"strings"

	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// DefaultActorNamespace 是 actor 平面命名空间的缺省值（= types.DefaultNamespace）。
const DefaultActorNamespace = types.DefaultNamespace

// ActorNamespaceOf 从注册中心命名空间派生 actor 平面命名空间（供进程内/测试形态使用）：
// 取注册中心命名空间的**叶子段**——它是 etcd 键路径形态（如 /atlas/services/it-123，含 `/`），
// 不能直接当 subject token（subject 段用点号分隔，斜杠/点号会造成寻址纠缠）。
//
// 非法字符**一律报错而不是归一**：把 `e2e.1790` 静默改写成 `e2e-1790` 会让本应隔离的两套
// 部署塌缩到同一命名空间（正是隔离要防的串台），因此宁可启动失败。
// 显式给定时由调用方（services/*/assemble）直接使用，本函数只处理派生路径。
func ActorNamespaceOf(registryNamespace string) (string, error) {
	leaf := registryNamespace
	if i := strings.LastIndex(leaf, "/"); i >= 0 {
		leaf = leaf[i+1:]
	}
	ns, err := types.NormalizeNamespace(leaf)
	if err != nil {
		return "", fmt.Errorf("bootstrap: 由注册中心前缀 %q 派生的 actor 命名空间非法（请显式配置 runtime.actor_namespace）: %w", registryNamespace, err)
	}
	return ns, nil
}
