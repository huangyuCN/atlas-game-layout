// fault.go 是容错与顶号形态：异常下线联动（-mode fault）与顶号/断线恢复（-mode kick）。
package main

import (
	"context"
	"fmt"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	"google.golang.org/protobuf/encoding/protojson"
)

// runFault 验证「排队中杀进程」的异常下线兜底：
// A 登录入队 → 断开连接（模拟杀进程，不发 Logout）→ gateway 会话过期清扫
// 联动 PlayerActor（SESSION_EXPIRED）→ 撮合域取消票据；A 重登后查询状态应归零（NONE）。
func runFault(ctx context.Context, a addrs) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1) 登录（记录凭据供重登）+ 入队。
	p, err := newBizPlayer(a.tcp, sdkclient.WithAutoReconnect(false))
	if err != nil {
		return err
	}
	if err := p.registerLogin(ctx, 0); err != nil {
		return err
	}
	playerID := p.id
	if _, err := p.players.EnterMatchQueue(ctx, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}); err != nil {
		return fmt.Errorf("入队: %w", err)
	}
	st, err := p.players.GetMatchStatus(ctx, &gamev1.GetMatchStatusReq{})
	if err != nil {
		return fmt.Errorf("入队后查询: %w", err)
	}
	fmt.Printf("[容错] 入队 ok（state=%s ticket=%s）\n", st.GetState(), st.GetTicketId())

	// 2) 断开连接（模拟杀进程；不做任何显式取消/登出）。
	fmt.Println("[容错] 断开连接（模拟杀进程）")
	if err := p.disconnect(); err != nil {
		return err
	}
	return awaitFaultRecovery(ctx, a.tcp, playerID)
}

// awaitFaultRecovery 等待会话过期清扫联动（ttl 30s + 清扫周期 15s）后重登验证状态归零。
func awaitFaultRecovery(ctx context.Context, tcpAddr, playerID string) error {
	fmt.Println("[容错] 等待会话过期联动（≤75s）…")
	deadline := time.Now().Add(75 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Second)
		st, ok, err := reloginProbe(ctx, tcpAddr, playerID)
		if err != nil {
			return err
		}
		if !ok {
			continue // sweep 尚未完成时重登可能被旧路由挤下线，稍后重试
		}
		if st.GetState() != matcherv1.MatchState_MATCH_STATE_NONE {
			continue // 联动尚未生效，继续等待
		}
		fmt.Println("[容错] 会话过期联动生效：票据已取消，重登状态归零（NONE）")
		return nil
	}
	return fmt.Errorf("重登验证超时")
}

// reloginProbe 重登一次并查询匹配状态（sweep 未完成时登录失败返回 ok=false）。
func reloginProbe(ctx context.Context, tcpAddr, playerID string) (*gamev1.MatchStatusReply, bool, error) {
	p2, err := newBizPlayer(tcpAddr, sdkclient.WithAutoReconnect(false))
	if err != nil {
		return nil, false, err
	}
	if _, lerr := p2.sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: playerID, Password: "pw"}); lerr != nil {
		_ = p2.disconnect()
		return nil, false, nil
	}
	st, qerr := p2.players.GetMatchStatus(ctx, &gamev1.GetMatchStatusReq{})
	_ = p2.disconnect()
	if qerr != nil {
		return nil, false, fmt.Errorf("重登查询: %w", qerr)
	}
	return st, true, nil
}

// runKick 顶号与断线恢复闭环：
// A 注册登录 → B 同账号新登录挤下 A（A 收 KickedNotify，旧凭据的请求与恢复均被拒）
// → B 断开（模拟杀进程）→ 新客户端凭 token Resume 免密恢复 → 业务请求恢复正常。
func runKick(ctx context.Context, a addrs) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1) A 注册登录，挂接被挤下线推送。
	a1, err := newBizPlayer(a.tcp, sdkclient.WithAutoReconnect(false))
	if err != nil {
		return err
	}
	if err := a1.registerLogin(ctx, 0); err != nil {
		return err
	}
	staleToken := a1.sess.Token()
	kicked := make(chan *gatewayv1.KickedNotify, 1)
	a1.cli.On(consts.PushOpKickedOffline, func(_ string, payload []byte) {
		var kn gatewayv1.KickedNotify
		if protojson.Unmarshal(payload, &kn) == nil {
			push(kicked, &kn)
		}
	})

	// 2) B 同账号新登录：挤下 A。
	b1, err := newBizPlayer(a.tcp, sdkclient.WithAutoReconnect(false))
	if err != nil {
		return err
	}
	if _, err := b1.sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: a1.id, Password: "pw"}); err != nil {
		return fmt.Errorf("B 顶号登录: %w", err)
	}
	select {
	case kn := <-kicked:
		fmt.Printf("[顶号] A 收到被挤下线通知 ok（reason=%s）\n", kn.GetReason())
	case <-time.After(3 * time.Second):
		return fmt.Errorf("A 未收到被挤下线通知")
	}
	if err := assertKickedRejected(ctx, a1, staleToken); err != nil {
		return err
	}

	// 3) B 断开（模拟杀进程）→ 新客户端凭 token Resume 免密恢复。
	b1Token, b1ID := b1.sess.Token(), b1.sess.PlayerID()
	if err := b1.disconnect(); err != nil {
		return err
	}
	fmt.Println("[顶号] B 已断开，新客户端凭 token 恢复会话…")
	p3, err := newBizPlayer(a.tcp)
	if err != nil {
		return err
	}
	var rep gatewayv1.ResumeReply
	if err := p3.cli.Invoke(ctx, sdkclient.OpSessionResume,
		&gatewayv1.ResumeRequest{Token: b1Token, PlayerId: b1ID}, &rep); err != nil {
		return fmt.Errorf("Resume 恢复失败: %w", err)
	}
	if rep.GetPlayerId() != b1ID {
		return fmt.Errorf("Resume 回执不符: %s != %s", rep.GetPlayerId(), b1ID)
	}
	st, err := p3.players.GetMatchStatus(ctx, &gamev1.GetMatchStatusReq{})
	if err != nil {
		return fmt.Errorf("恢复后查询: %w", err)
	}
	fmt.Printf("[顶号] Resume 免密恢复 ok（player=%s state=%s）\n", rep.GetPlayerId(), st.GetState())
	return nil
}

// assertKickedRejected 断言被顶号端：旧凭据的业务请求与 Resume 均被业务拒绝。
func assertKickedRejected(ctx context.Context, a1 *player, staleToken string) error {
	if _, err := a1.players.GetMatchStatus(ctx, &gamev1.GetMatchStatusReq{}); err == nil {
		return fmt.Errorf("A 被顶号后业务请求应被拒")
	}
	var rep gatewayv1.ResumeReply
	err := a1.cli.Invoke(ctx, sdkclient.OpSessionResume,
		&gatewayv1.ResumeRequest{Token: staleToken, PlayerId: a1.id}, &rep)
	if err == nil {
		return fmt.Errorf("A 被顶号后旧凭据 Resume 应被拒")
	}
	fmt.Printf("[顶号] A 旧凭据请求与 Resume 均被拒 ok（err=%v）\n", err)
	return nil
}
