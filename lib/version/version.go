// Package version 提供服务构建版本信息：默认值可被构建期 -ldflags -X 覆盖，
// 由 Makefile（git describe / rev-parse / 构建时间）与 CI（tag）注入，
// 避免版本号在配置或代码里手工维护。
package version

import "fmt"

// 构建信息（未注入时的缺省值；注入路径见 Makefile 的 LDFLAGS）。
var (
	// Version 是构建版本（git describe --tags --always --dirty；缺省 dev）。
	Version = "dev"
	// Commit 是构建提交短 SHA（缺省 unknown）。
	Commit = "unknown"
	// BuildTime 是构建时间（UTC，RFC3339；缺省 unknown）。
	BuildTime = "unknown"
)

// String 返回一行构建摘要（启动日志用）："<version> (commit <sha>, built <time>)"。
func String() string {
	return fmt.Sprintf("%s (commit %s, built %s)", Version, Commit, BuildTime)
}
