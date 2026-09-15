// Package handler 提供 biz 各接口的实现（分文件，与协议一一对应）。
package handler

import (
	"context"
	"errors"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/idgen"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/usecase"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
)

// PlayerHandler 是 biz.PlayerService 的实现（注册/登录）。
// 会话裁决已单点收敛到 Gateway：game 侧只校验玩家数据，不持久化 token 副本
// （LoginReq.Token 仅供 Gateway 预签发链路使用，此处不落库）。
type PlayerHandler struct {
	store repo.PlayerRepo
	opts  biz.PlayerServiceOptions
}

// NewPlayerHandler 构造玩家业务实现。
func NewPlayerHandler(store repo.PlayerRepo, opts biz.PlayerServiceOptions) *PlayerHandler {
	if opts.NewPlayerID == nil {
		opts.NewPlayerID = idgen.Player
	}
	return &PlayerHandler{store: store, opts: opts}
}

// Register 实现 biz.PlayerService（两段式：建号回执，不建会话）。
func (h *PlayerHandler) Register(ctx context.Context, req *gamev1.RegisterReq) (*gamev1.RegisterReply, error) {
	if req.GetAccount() == "" || req.GetPassword() == "" {
		return nil, errorv1.ErrInvalidParams("账号与口令不能为空")
	}
	if _, err := h.store.FindByAccount(ctx, req.GetAccount()); err == nil {
		return nil, errorv1.ErrPlayerAlreadyExists("账号已存在")
	} else if !errors.Is(err, repo.ErrPlayerNotFound) {
		return nil, errorv1.ErrInternal("查询账号失败")
	}
	salt, hash, err := data.HashPassword(req.GetPassword())
	if err != nil {
		return nil, errorv1.ErrInvalidParams("口令不合法")
	}
	player := &models.Player{
		PlayerID: h.opts.NewPlayerID(),
		Account:  req.GetAccount(),
		Salt:     salt,
		Password: hash,
		Nickname: req.GetNickname(),
		Level:    1,
	}
	if err := h.store.CreatePlayer(ctx, player); err != nil {
		return nil, errorv1.ErrInternal("创建玩家失败")
	}
	return &gamev1.RegisterReply{
		PlayerId: player.PlayerID,
		Player:   usecase.PlayerSummary(player),
	}, nil
}

// Login 实现 biz.PlayerService（redis→mongo 三级加载 + 口令校验）。
// 会话建立与令牌裁决由 Gateway 单点承担：本方法不再写会话表、不做新旧令牌比对。
func (h *PlayerHandler) Login(ctx context.Context, req *gamev1.LoginReq) (*gamev1.LoginReply, error) {
	if req.GetPlayerId() == "" || req.GetPassword() == "" {
		return nil, errorv1.ErrInvalidParams("玩家与口令不能为空")
	}
	player, err := h.store.LoadPlayer(ctx, req.GetPlayerId())
	if errors.Is(err, repo.ErrPlayerNotFound) {
		return nil, errorv1.ErrPlayerNotFound("玩家不存在，请先注册")
	}
	if err != nil {
		return nil, errorv1.ErrInternal("加载玩家失败")
	}
	if !data.VerifyPassword(req.GetPassword(), player.Salt, player.Password) {
		return nil, errorv1.ErrPasswordWrong("口令错误")
	}
	return &gamev1.LoginReply{
		Player: usecase.PlayerSummary(player),
	}, nil
}

// 静态保证 PlayerHandler 实现 biz.PlayerService。
var _ biz.PlayerService = (*PlayerHandler)(nil)
