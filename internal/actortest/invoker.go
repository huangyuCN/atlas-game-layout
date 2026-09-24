// Package actortest 提供 actor 接入层测试共用的桩件：
// 避免各 api 包重复实现 core.ActorInvoker 假件（接线的包多起来后重复会扩散）。
package actortest

import (
	"context"

	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// Invoker 是记录型投递桩：记录最近一次投递的目标 PID、选项个数与投递形态，
// 并按预设值返回回执/错误（实现 core.ActorInvoker）。
type Invoker struct {
	// AskedPID / ToldPID 分别是最近一次 Ask / Tell 的目标 PID。
	AskedPID types.PID
	ToldPID  types.PID
	// Opts 是最近一次投递的选项个数（选项本身不透明，行为断言看 PID 与错误）。
	Opts int
	// Reply 是 Ask 的预设回执（可为具体消息，也可为跨节点 wire 字节）。
	Reply any
	// Err 是预设错误（Ask/Tell 共用）。
	Err error
}

// Ask 记录目标 PID 与选项个数并返回预设回执。
func (f *Invoker) Ask(_ context.Context, pid types.PID, _ any, opts ...core.SendOption) (any, error) {
	f.AskedPID, f.Opts = pid, len(opts)
	return f.Reply, f.Err
}

// Tell 记录目标 PID 与选项个数并返回预设错误。
func (f *Invoker) Tell(_ context.Context, pid types.PID, _ any, opts ...core.SendOption) error {
	f.ToldPID, f.Opts = pid, len(opts)
	return f.Err
}
