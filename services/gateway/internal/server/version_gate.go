// Package server 的客户端版本门槛（M1 协议演进 + 阶段 3 战斗帧直连）。
package server

import (
	"context"
	"strings"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/version"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	atlaslog "github.com/huangyuCN/atlas/log"
)

// VersionGate 是客户端版本门槛：按运行时配置拒绝不支持战斗帧直连的旧客户端。
// 阶段 3 起战斗帧由客户端凭票据直连接入层（需新 SDK），老客户端会「匹配成功但连不上」，
// 故门槛在**会话建立之前**判定（网关 Login/Resume，见 handler.go）：被拒即不建会话、不进匹配。
type VersionGate struct {
	min  string                         // 最低版本（semver；空 = 不设门槛，仅记录）
	mode configspb.MinClientVersionMode // 执行模式：NEGOTIATE = 仅记录（灰度逃生门）；其余（含缺省）= 强制
}

// NewVersionGate 从运行时配置构造门槛（装配期一次；配置热更不回溯已建连接）。
func NewVersionGate(cfg *configspb.Runtime) VersionGate {
	if cfg == nil {
		return VersionGate{}
	}
	return VersionGate{min: cfg.GetMinClientVersion(), mode: cfg.GetMinClientVersionMode()}
}

// Check 校验客户端上报版本，返回 nil 即放行：
//   - 门槛为空 ⇒ 仅记录（记录客户端上报版本，不拒绝）——未设门槛的部署行为不变；
//   - 门槛非空且模式为 NEGOTIATE ⇒ 仅记录（灰度期只观察，逃生门）；
//   - 其余（含未配 mode 的缺省值）⇒ 强制：低于门槛 / 未上报 / 版本串非法一律拒绝。
//
// 「设置门槛即强制」是刻意的：门槛与 mode 分开配时，漏配 mode 会静默退化成只记录，
// 正是「匹配成功但连不上」的成因；需要只记录请显式写 NEGOTIATE。
func (v VersionGate) Check(ctx context.Context, clientVersion string) error {
	if v.min == "" || v.mode == configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_NEGOTIATE {
		v.record(ctx, clientVersion)
		return nil
	}
	if strings.TrimSpace(clientVersion) == "" {
		return v.reject("未上报", clientVersion)
	}
	cmp, err := version.Compare(clientVersion, v.min)
	switch {
	case err != nil:
		return v.reject("无法识别（需形如 1.2.3）", clientVersion)
	case cmp < 0:
		return v.reject("过低", clientVersion)
	}
	return nil
}

// reject 组装升级提示：reason 取常量 CLIENT_VERSION_TOO_LOW，message 统一写出
// 「成因 + 当前版本 + 最低要求版本 + 升级动作」，客户端提示与客服话术同源。
func (v VersionGate) reject(cause, clientVersion string) error {
	return errorv1.ErrClientVersionTooLow("客户端版本%s：当前 %q，最低要求 %q，请升级客户端",
		cause, clientVersion, v.min)
}

// record 记录一次「仅记录」判定（未设门槛与显式 NEGOTIATE 共用同一口径）。
func (v VersionGate) record(ctx context.Context, clientVersion string) {
	atlaslog.GetLogger().WithContext(ctx).Info("客户端版本门槛（仅记录不拒绝）",
		"client_version", clientVersion, "min_client_version", v.min)
}
