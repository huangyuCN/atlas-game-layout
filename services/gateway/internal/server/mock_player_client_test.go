package server

import (
	"context"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// 本文件实现 gamev1rpc.PlayerServiceClient（网关会话联动走的 internal 面类型化客户端）：
// 与 Invoke（Edge 面按方法寻址）共用同一份域桩状态，模拟同一批 actor 的两个入口。
// 调用身份一律取自 ctx 的 CallInfo——网关写入 metadata、客户端拦截器搬运，
// 桩这里读 ctx 即等价于服务端 opgrpc 拦截器的还原结果。

// typed 记录一次类型化调用并按请求类型分派到域桩。
func (m *mockDomain) typed(ctx context.Context, method string, req proto.Message) (proto.Message, error) {
	info, _ := opcall.CallInfoFrom(ctx)
	m.record(method, info)
	return m.handle(info.PlayerID, req)
}

// Register 实现 gamev1rpc.PlayerServiceClient：账号即调用身份（网关按账号组装 metadata）。
func (m *mockDomain) Register(ctx context.Context, req *gamev1.RegisterReq) (*gamev1.RegisterReply, error) {
	out, err := m.typed(ctx, "/game.v1.PlayerService/Register", req)
	if err != nil {
		return nil, err
	}
	rep, _ := out.(*gamev1.RegisterReply)
	return rep, nil
}

// Login 实现 gamev1rpc.PlayerServiceClient：登录裁决（玩家身份来自 metadata）。
func (m *mockDomain) Login(ctx context.Context, req *gamev1.LoginReq) (*gamev1.LoginReply, error) {
	out, err := m.typed(ctx, "/game.v1.PlayerService/Login", req)
	if err != nil {
		return nil, err
	}
	rep, _ := out.(*gamev1.LoginReply)
	return rep, nil
}

// Logout 实现 gamev1rpc.PlayerServiceClient：登出为 Tell 语义，回执是 Empty。
func (m *mockDomain) Logout(ctx context.Context, req *gamev1.LogoutMsg) (*emptypb.Empty, error) {
	if _, err := m.typed(ctx, "/game.v1.PlayerService/Logout", req); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// GrantItem 实现 gamev1rpc.PlayerServiceClient（管理面链路用，单测不触达具体发放）。
func (m *mockDomain) GrantItem(ctx context.Context, req *gamev1.GrantItemReq) (*gamev1.GrantItemReply, error) {
	out, err := m.typed(ctx, "/game.v1.PlayerService/GrantItem", req)
	if err != nil {
		return nil, err
	}
	rep, _ := out.(*gamev1.GrantItemReply)
	return rep, nil
}

// GetPlayer 实现 gamev1rpc.PlayerServiceClient：玩家摘要查询。
func (m *mockDomain) GetPlayer(ctx context.Context, req *gamev1.GetPlayerReq) (*gamev1.PlayerReply, error) {
	out, err := m.typed(ctx, "/game.v1.PlayerService/GetPlayer", req)
	if err != nil {
		return nil, err
	}
	rep, _ := out.(*gamev1.PlayerReply)
	return rep, nil
}
