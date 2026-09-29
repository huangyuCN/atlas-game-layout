// Command check-deps 断言「网关与 matcher 不依赖 actor 集群运行时」。
//
// 依据（统一设计 v2 阶段 2「网关/matcher 退出 actor 面」）：这两者都是 actor 平面的
// **调用方**——历史上它们各自装配 etcd 目录 + NATS 传输的集群运行时，并注册「只发不接」
// 的懒激活副本来支持发送侧判定。v2 起调用方改走域 rpc/ 平面的 gRPC 面（客户端 op 走
// Edge、服务间调用走 Internal），目标 PID 的解析与懒激活在托管方完成，调用方不再需要
// 集群运行时——一旦回归即两条投递路径并存（框架最想消灭的漂移面）。
//
// 用法：go run ./scripts/check-deps；违规以退出码 1 失败。
package main

import (
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
)

// forbidden 是禁止出现在调用方依赖图里的包（前缀匹配）。
var forbidden = []string{
	"github.com/huangyuCN/atlas/contrib/actor/cluster", // actor 集群运行时（目录 + NATS 传输）
	"github.com/huangyuCN/atlas-game-layout/pkg/actor", // 模板侧集群运行时装配
}

// callers 是必须以 gRPC 面调用域服务的服务（不再持有集群运行时）。
var callers = []string{"./services/gateway/...", "./services/matcher/..."}

func main() {
	deps, err := listDeps(callers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "check-deps: 取依赖图失败: %v\n", err)
		os.Exit(1)
	}
	violations := matchForbidden(deps)
	if len(violations) > 0 {
		fmt.Fprintf(os.Stderr, "check-deps: 调用方不得依赖 actor 集群运行时（阶段 2「退出 actor 面」）:\n")
		for _, v := range violations {
			fmt.Fprintf(os.Stderr, "  %s\n", v)
		}
		os.Exit(1)
	}
	fmt.Printf("check-deps: OK（%d 个调用方包，未命中 %d 条禁止依赖）\n", len(callers), len(forbidden))
}

// listDeps 返回调用方的全部依赖包（**含测试依赖**：`-test` 让测试文件里偷偷 import
// 集群运行时也会被门禁抓住——测试代码同样会把依赖带进构建图），去重升序。
func listDeps(pkgs []string) ([]string, error) {
	args := append([]string{"list", "-deps", "-test"}, pkgs...)
	out, err := exec.Command("go", args...).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("%v: %s", err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, err
	}
	seen := make(map[string]struct{})
	var deps []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if _, ok := seen[line]; ok {
			continue
		}
		seen[line] = struct{}{}
		deps = append(deps, line)
	}
	sort.Strings(deps)
	return deps, nil
}

// matchForbidden 返回命中禁止清单的依赖包。
func matchForbidden(deps []string) []string {
	var bad []string
	for _, dep := range deps {
		for _, f := range forbidden {
			if dep == f || strings.HasPrefix(dep, f+"/") {
				bad = append(bad, dep)
				break
			}
		}
	}
	return bad
}
