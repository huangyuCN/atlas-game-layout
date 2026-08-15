package server

import (
	"context"
	"errors"

	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// errActorNotWired 是集群运行时未接入的占位错误。
var errActorNotWired = errors.New("actor: 集群运行时未接入（M5/M7 里程碑装配 pkg/actor）")

// noopRuntime 是 actorclient 的占位运行时：
// M4 阶段 gateway 不依赖 actor 集群即可启动，M5（game 接入）与
// M7（battle 接入）时替换为 pkg/actor.Runtime。
type noopRuntime struct{}

func (noopRuntime) Tell(_ context.Context, _ types.PID, _ any) error {
	return errActorNotWired
}

func (noopRuntime) Ask(_ context.Context, _ types.PID, _ any) (any, error) {
	return nil, errActorNotWired
}
