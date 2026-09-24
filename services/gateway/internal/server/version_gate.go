// Package server 的客户端版本门槛（M1：协议演进协商）。
package server

import (
	"context"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/version"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	atlaslog "github.com/huangyuCN/atlas/log"
)

// VersionGate 是客户端版本门槛：按运行时配置对客户端上报版本做校验/协商。
// 客户端是长生命周期二进制，删改 op 前必须先能协商与废弃（统一设计 v2 的 M1）。
type VersionGate struct {
	min  string                         // 最低版本（semver；空 = 不设门槛）
	mode configspb.MinClientVersionMode // OFF / NEGOTIATE / ENFORCE
}

// NewVersionGate 从运行时配置构造门槛（装配期一次；配置热更不回溯已建连接）。
func NewVersionGate(cfg *configspb.Runtime) VersionGate {
	if cfg == nil {
		return VersionGate{}
	}
	return VersionGate{min: cfg.GetMinClientVersion(), mode: cfg.GetMinClientVersionMode()}
}

// Check 校验客户端上报版本：OFF 或不设门槛直接放行；NEGOTIATE 只记录不拒；
// ENFORCE 下「低于门槛 / 未上报 / 版本串非法」一律拒（失败即不满足，不静默放行）。
func (v VersionGate) Check(ctx context.Context, clientVersion string) error {
	if v.mode == configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_OFF || v.min == "" {
		return nil
	}
	if cmp, err := version.Compare(clientVersion, v.min); err == nil && cmp >= 0 {
		return nil
	}
	if v.mode == configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_NEGOTIATE {
		atlaslog.GetLogger().WithContext(ctx).Info("客户端版本协商（仅记录不拒绝）",
			"client_version", clientVersion, "min_client_version", v.min)
		return nil
	}
	return errorv1.ErrClientVersionTooLow("客户端版本过低：当前 %q，最低要求 %q，请升级客户端",
		clientVersion, v.min)
}
