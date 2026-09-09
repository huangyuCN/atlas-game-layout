// Package actor 认证域业务方法：Register/Login/Logout（实现 PlayerActorServer 接口的认证部分）。
// 业务逻辑经 svc（PlayerHandler）承接；本文件只保留 actor 壳：
// 错误上抛（不再包 Ok:false 回执——错误经集群 error 通道往返，code/reason 保留）。
package actor

import (
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// Register 实现 gamev1.PlayerActorServer（两段式注册，D8）：
// 玩家数据不存在则创建并回执，客户端仍需 Login 建立会话；回执后自停。
func (p *PlayerActor) Register(ctx core.ActorContext, req *gamev1.RegisterActorReq) (*gamev1.RegisterActorReply, error) {
	reply, berr := p.svc.Register(ctx.Context(), req)
	if berr != nil {
		return nil, berr // 错误上抛：集群 error 通道往返（code/reason 保留）
	}
	// 注册是两段式：建号回执后即自停，登录按 player_id 另起实例。
	ctx.Stop(types.ExitNormal())
	return reply, nil
}

// Login 实现 gamev1.PlayerActorServer（D10 会话令牌裁决）：业务裁决成功后
// 加载聚合根到内存（redis→mongo 三级链路）。
func (p *PlayerActor) Login(ctx core.ActorContext, req *gamev1.LoginActorReq) (*gamev1.LoginActorReply, error) {
	reply, berr := p.svc.Login(ctx.Context(), req)
	if berr != nil {
		return nil, berr
	}
	player, err := p.store.LoadPlayer(ctx.Context(), req.GetPlayerId())
	if err != nil {
		return nil, errorv1.ErrInternal("加载玩家失败")
	}
	p.player = player
	return reply, nil
}

// Logout 实现 gamev1.PlayerActorServer（Tell，单向）：令牌校验后停止自身
// （Locator 移除，集群目录归属释放，OnStop 落库）。
func (p *PlayerActor) Logout(ctx core.ActorContext, msg *gamev1.LogoutActorMsg) error {
	_ = p.svc.Logout(ctx.Context(), p.pid.UID(), msg.GetToken())
	ctx.Stop(types.ExitNormal())
	return nil
}
