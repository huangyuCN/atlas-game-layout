package server

import (
	"context"

	gamev1actor "github.com/huangyuCN/atlas-game-layout/api/game/v1/actor"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// senderOf 组装「玩家发起」的发起者 PID 文本（player:<UID>）。
// 身份非法（空串/含分隔符）返回空串：下发空的发起者只是"不带发起者"，
// 而伪造一个身份会让接收侧的同源校验失去意义，故宁可缺省也不冒名。
func senderOf(playerID string) string {
	pid, err := types.NewPID(gamev1actor.PlayerServiceActorType, playerID)
	if err != nil {
		return ""
	}
	return pid.String()
}

// callInfoOf 组装域调用的身份三键（经 opgrpc 客户端拦截器写出 metadata）：
//   - PlayerID：调用方身份——会话寻址（UID_SOURCE_SESSION）的目标 actor 由它决定；
//   - SenderPID：发起者身份——客户端 op 由玩家发起（withSender=true）；
//     网关自身的会话联动（Register/Login/Logout）不下发发起者（网关不是玩家）；
//   - RequestID：帧携带的请求 ID——所有 op 都作观测头，幂等 op 由接收侧按条目作去重键。
func callInfoOf(ctx context.Context, playerID string, withSender bool) opcall.CallInfo {
	info := opcall.CallInfo{PlayerID: playerID, RequestID: requestIDOf(ctx)}
	if withSender {
		info.SenderPID = senderOf(playerID)
	}
	return info
}
