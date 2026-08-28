// Package app 是 game 服务的唯一装配之家：
// Module 列出全部组件清单（infra → data → biz → actor → server 分层），
// 进程形态（cmd/main + atlas App 驱动启停）与进程内形态
// （assemble + serverutil.ServeAsync 驱动启停）共用同一张依赖图。
package app

import "time"

// Tuning 是 game 服务时间类调优参数的单点定义
// （原 server/deps.go 与 assemble 各声明一份，收敛至此杜绝漂移）。
type Tuning struct {
	// SessionTTL 是玩家在线会话租期。
	SessionTTL time.Duration
	// SnapshotTTL 是玩家快照缓存租期。
	SnapshotTTL time.Duration
	// SnapshotTick 是定时快照周期（<=0 关闭定时快照）。
	SnapshotTick time.Duration
}

// DefaultTuning 提供默认调优参数（与 gateway 会话租期对齐）。
func DefaultTuning() Tuning {
	return Tuning{
		SessionTTL:   30 * time.Second,
		SnapshotTTL:  5 * time.Minute,
		SnapshotTick: 10 * time.Second,
	}
}
