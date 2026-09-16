// Package app 是 game 服务的唯一装配之家：
// Module 列出全部组件清单（infra → data → biz → actor → server 分层），
// 进程形态（cmd/main + atlas App 驱动启停）与进程内形态
// （assemble + serverutil.ServeAsync 驱动启停）共用同一张依赖图。
package app

import "time"

// Tuning 是 game 服务时间类调优参数的单点定义
// （原 server/deps.go 与 assemble 各声明一份，收敛至此杜绝漂移）。
// 快照租期（72h）由 redis 仓储层常量统一管理（耐久性语义，不外露调优）。
type Tuning struct {
	// SnapshotTick 是统一落盘周期（3 分钟；丢失窗口上限；<=0 关闭定时落盘）。
	SnapshotTick time.Duration
}

// DefaultTuning 提供默认调优参数。
func DefaultTuning() Tuning {
	return Tuning{
		SnapshotTick: 3 * time.Minute,
	}
}
