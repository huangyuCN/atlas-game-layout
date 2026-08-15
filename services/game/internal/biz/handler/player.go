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

// PlayerHandler 是 biz.PlayerService 的实现（注册/登录/登出）。
type PlayerHandler struct {
	store    repo.PlayerRepo
	sessions repo.SessionStore
	opts     biz.PlayerServiceOptions
}

// NewPlayerHandler 构造玩家业务实现。
func NewPlayerHandler(store repo.PlayerRepo, sessions repo.SessionStore, opts biz.PlayerServiceOptions) *PlayerHandler {
	if opts.NewPlayerID == nil {
		opts.NewPlayerID = idgen.Player
	}
	return &PlayerHandler{store: store, sessions: sessions, opts: opts}
}

// Register 实现 biz.PlayerService（两段式：建号回执，不建会话）。
func (h *PlayerHandler) Register(ctx context.Context, req *gamev1.RegisterActorReq) (*gamev1.RegisterActorReply, error) {
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
	return &gamev1.RegisterActorReply{
		Ok:       true,
		PlayerId: player.PlayerID,
		Player:   usecase.PlayerSummary(player),
	}, nil
}

// Login 实现 biz.PlayerService（redis→mongo 三级加载 + 会话令牌裁决）。
func (h *PlayerHandler) Login(ctx context.Context, req *gamev1.LoginActorReq) (*gamev1.LoginActorReply, error) {
	if req.GetPlayerId() == "" || req.GetPassword() == "" || req.GetToken() == "" {
		return nil, errorv1.ErrInvalidParams("玩家/口令/令牌不能为空")
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
	// 会话令牌裁决：覆盖旧令牌（旧连接在 gateway 侧被挤下线处理）。
	if err := h.sessions.Set(ctx, player.PlayerID, req.GetToken(), h.opts.SessionTTL); err != nil {
		return nil, errorv1.ErrInternal("写入会话失败")
	}
	return &gamev1.LoginActorReply{
		Ok:     true,
		Player: usecase.PlayerSummary(player),
	}, nil
}

// Logout 实现 biz.PlayerService（令牌匹配才清理，裁决竞态兜底）。
func (h *PlayerHandler) Logout(ctx context.Context, playerID, token string) error {
	if playerID == "" || token == "" {
		return errorv1.ErrInvalidParams("玩家与令牌不能为空")
	}
	current, err := h.sessions.Get(ctx, playerID)
	if err != nil {
		return errorv1.ErrInternal("读取会话失败")
	}
	if current != token {
		return nil // 会话已归属新登录，静默跳过
	}
	if err := h.sessions.Del(ctx, playerID); err != nil {
		return errorv1.ErrInternal("清理会话失败")
	}
	return nil
}

// 静态保证 PlayerHandler 实现 biz.PlayerService。
var _ biz.PlayerService = (*PlayerHandler)(nil)
