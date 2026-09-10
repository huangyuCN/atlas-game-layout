// Package actorclient 提供 gateway 侧的远程 actor 调用装配：
// 业务消息路由 game 玩家 actor（player:<id>），战斗消息路由 battle 战斗 actor
// （battle:<id>），底层集群寻址与懒激活由 pkg/actor Runtime 承担（D5/D9）。
package actorclient

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// Runtime 是 actorclient 依赖的最小运行时接口（pkg/actor.Runtime 满足，测试可注入实现）。
type Runtime interface {
	Tell(ctx context.Context, pid types.PID, msg any) error
	Ask(ctx context.Context, pid types.PID, req any, opts ...core.SendOption) (any, error)
}

// Client 是 gateway 的远程 actor 客户端。
type Client struct {
	rt Runtime
}

// NewClient 构造远程 actor 客户端。
func NewClient(rt Runtime) *Client { return &Client{rt: rt} }

// PlayerPID 构造玩家 actor PID（路由 game 服务的 PlayerActor）。
func PlayerPID(playerID string) (types.PID, error) {
	return newPID(consts.ActorTypePlayer, playerID)
}

// BattlePID 构造战斗 actor PID（路由 battle 服务的战斗 actor）。
func BattlePID(battleID string) (types.PID, error) {
	return newPID(consts.ActorTypeBattle, battleID)
}

// newPID 构造并校验 PID。
func newPID(typ, uid string) (types.PID, error) {
	pid, err := types.NewPID(typ, uid)
	if err != nil {
		return types.PID{}, fmt.Errorf("actorclient: 非法 PID %s:%s: %w", typ, uid, err)
	}
	return pid, nil
}

// TellPlayer 向玩家 actor 投递消息（不等待响应）。
func (c *Client) TellPlayer(ctx context.Context, playerID string, msg any) error {
	pid, err := PlayerPID(playerID)
	if err != nil {
		return err
	}
	return c.rt.Tell(ctx, pid, msg)
}

// AskPlayer 向玩家 actor 发起请求并等待响应。
func (c *Client) AskPlayer(ctx context.Context, playerID string, req any) (any, error) {
	pid, err := PlayerPID(playerID)
	if err != nil {
		return nil, err
	}
	return c.rt.Ask(ctx, pid, req)
}

// Starter 是带生命周期能力的运行时（pkg/actor.Runtime 满足；测试桩可忽略）。
type Starter interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// Start 启动底层集群运行时（未实现 Starter 时为空操作）。
func (c *Client) Start(ctx context.Context) error {
	if s, ok := c.rt.(Starter); ok {
		return s.Start(ctx)
	}
	return nil
}

// Shutdown 停止底层集群运行时（未实现 Starter 时为空操作）。
func (c *Client) Shutdown(ctx context.Context) error {
	if s, ok := c.rt.(Starter); ok {
		return s.Shutdown(ctx)
	}
	return nil
}

// TellBattle 向战斗 actor 投递消息（帧输入等，不等待响应）。
func (c *Client) TellBattle(ctx context.Context, battleID string, msg any) error {
	pid, err := BattlePID(battleID)
	if err != nil {
		return err
	}
	return c.rt.Tell(ctx, pid, msg)
}

// AskBattle 向战斗 actor 发起请求并等待响应。
func (c *Client) AskBattle(ctx context.Context, battleID string, req any) (any, error) {
	pid, err := BattlePID(battleID)
	if err != nil {
		return nil, err
	}
	return c.rt.Ask(ctx, pid, req)
}

// invoker 适配 Player/Battle 两个目标：把 actorclient 的「按业务 ID 构造 PID +
// 调用 Runtime」封装为 core.ActorInvoker（protoc-gen-atlas-actor 生成的 client
// stub 依赖；Ask 的 SendOption 变参在本层无消费方，直接忽略）。
type invoker struct {
	newPID func(id string) (types.PID, error)
	ask    func(ctx context.Context, pid types.PID, req any, opts ...core.SendOption) (any, error)
	tell   func(ctx context.Context, pid types.PID, msg any) error
}

// Ask 实现 core.ActorInvoker：转发到构造时注入的运行时 Ask（SendOption 变参原样透传）。
func (i invoker) Ask(ctx context.Context, pid types.PID, req any, opts ...core.SendOption) (any, error) {
	return i.ask(ctx, pid, req, opts...)
}

// Tell 实现 core.ActorInvoker：转发到构造时注入的运行时 Tell。
func (i invoker) Tell(ctx context.Context, pid types.PID, msg any) error {
	return i.tell(ctx, pid, msg)
}

// PlayerInvoker 返回玩家 actor 的 core.ActorInvoker 适配（生成 client stub 用）。
func (c *Client) PlayerInvoker() core.ActorInvoker {
	return invoker{newPID: PlayerPID, ask: c.rt.Ask, tell: c.rt.Tell}
}

// BattleInvoker 返回战斗 actor 的 core.ActorInvoker 适配（生成 client stub 用）。
func (c *Client) BattleInvoker() core.ActorInvoker {
	return invoker{newPID: BattlePID, ask: c.rt.Ask, tell: c.rt.Tell}
}
