// Package actor 认证域业务方法：Register/Login/Logout（实现 PlayerServiceServer 接口的认证部分）。
// 业务逻辑经 svc（PlayerHandler）承接；本文件只保留 actor 壳：
// 错误上抛（错误经集群 error 通道往返，code/reason 保留）。
package actor

import (
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/usecase"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// Register 实现 gamev1.PlayerServiceServer（两段式注册）：
// 玩家数据不存在则创建并回执，客户端仍需 Login 建立会话；回执后自停。
func (p *PlayerActor) Register(ctx core.ActorContext, req *gamev1.RegisterReq) (*gamev1.RegisterReply, error) {
	reply, berr := p.svc.Register(ctx.Context(), req)
	if berr != nil {
		return nil, berr // 错误上抛：集群 error 通道往返（code/reason 保留）
	}
	// 注册是两段式：建号回执后即自停，登录按 player_id 另起实例。
	ctx.Stop(types.ExitNormal())
	return reply, nil
}

// Login 实现 gamev1.PlayerServiceServer：首登加载聚合根到内存（redis→mongo
// 选源链路，含双向对齐补写）。会话建立与令牌裁决由 Gateway 单点承担（game
// 侧不落 token）；重复登录（本 actor 已持聚合根）时复用内存权威态、不重载：
// 在线期间内存比存储新（定时快照 + 下线落库），重载会用陈旧快照覆盖快照
// 间隙的内存写（如顶号重登丢失刚发放的道具）。
// 回执摘要一律由本方法从选源后的聚合根组装（biz 只做口令校验，不做选源）。
func (p *PlayerActor) Login(ctx core.ActorContext, req *gamev1.LoginReq) (*gamev1.LoginReply, error) {
	reply, berr := p.svc.Login(ctx.Context(), req)
	if berr != nil {
		return nil, berr
	}
	if p.player != nil {
		reply.Player = usecase.PlayerSummary(p.player) // 以内存聚合根为准
		return reply, nil
	}
	player, err := p.store.LoadPlayer(ctx.Context(), req.GetPlayerId())
	if err != nil {
		return nil, errorv1.ErrInternal("加载玩家失败")
	}
	p.player = player
	reply.Player = usecase.PlayerSummary(player)
	return reply, nil
}

// Logout 实现 gamev1.PlayerServiceServer（Tell，单向）：收到即保存并停止自身
// （Locator 移除，集群目录归属释放，OnStop 联动撮合域 + 落库）。
// 旧「token 匹配才停」的裁决已移除：会话裁决单点收敛到 Gateway（会话管理器），
// game 侧不持 token 副本，LogoutMsg 也不再有 token 字段（reason 仅事件语义）。
func (p *PlayerActor) Logout(ctx core.ActorContext, msg *gamev1.LogoutMsg) error {
	// 下线流程反转：进入下线编排（冻结玩法 → 落库短重试 → 成功才真正 Stop），
	// 不再「先停再在 OnStop 里赌落库成功」。
	p.beginStopFlow(ctx, types.ExitNormal())
	return nil
}
