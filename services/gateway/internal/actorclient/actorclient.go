// Package actorclient 提供 gateway 侧的远程 actor 调用装配：
// 业务消息路由 game 玩家 actor（player:<id>），战斗消息路由 battle 战斗 actor
// （battle:<id>），底层集群寻址与懒激活由 pkg/actor Runtime 承担（D5/D9）。
package actorclient

import (
	"context"
	"fmt"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// Runtime 是 actorclient 依赖的最小运行时接口（pkg/actor.Runtime 满足，测试可注入实现）。
type Runtime interface {
	Tell(ctx context.Context, pid types.PID, msg any) error
	Ask(ctx context.Context, pid types.PID, req any) (any, error)
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
