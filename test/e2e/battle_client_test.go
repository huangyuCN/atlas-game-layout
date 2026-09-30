// 一端战斗客户端夹具（阶段 3 批次 5 后的形态）：业务链路走 SDK（注册/登录/心跳 + 成局通知取票），
// 战斗帧走直连帧面（框架传输客户端 + 帧槽票据）；本文件只放夹具，用例见 battle_test.go。

package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gamev1opclient "github.com/huangyuCN/atlas-game-layout/api/game/v1/opclient"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"google.golang.org/protobuf/encoding/protojson"
)

// battleClient 是一端战斗客户端（阶段 3 批次 5 后的形态）：
// 业务链路走 SDK（注册/登录/心跳 + 接收成局通知取本人票据），战斗帧走**直连帧面**
// （框架传输客户端 + 帧槽票据）——战斗 op 不再经网关。
type battleClient struct {
	sess     *sdkclient.Session
	cli      *sdkclient.Client
	frame    *directFrame
	kind     frameKind
	playerID string
	token    string

	mu     sync.Mutex
	ticket []byte // 成局通知里**本人**那张票（帧槽凭据；逐人扇出，不群发）
	frames []*battlev1.FrameBroadcast
	ends   []*battlev1.BattleEndNotify
	outs   []*battlev1.PlayerOutNotify
}

// newBattleClient 注册登录（业务走网关 WS）并挂接推送监听；战斗帧直连 battle 帧面。
// 直连在拿到票据**之前**建立（服务端按帧验票，故连接本身不需要票），
// 票据到达后由帧槽提供者逐帧带上。
func newBattleClient(t *testing.T, ctx context.Context, gw *gwassemble.Gateway, b *battleassemble.Battle, kind frameKind) *battleClient {
	t.Helper()
	sess, cli := dialWSSession(t, gw.WSURL)
	loginFlow(t, ctx, sess)
	c := &battleClient{
		sess:     sess,
		cli:      cli,
		kind:     kind,
		playerID: sess.PlayerID(),
		token:    sess.Token(),
	}
	c.watchNotifies()
	c.frame = dialDirectFrame(t, ctx, kind, frameAddrOf(b, kind), c.ticketOf)
	c.frame.notify(c.watchDirect)
	return c
}

// ticketOf 返回本人当前的入场票据（尚未收到成局通知时为空 → 匿名帧，服务端拒绝）。
func (c *battleClient) ticketOf() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ticket
}

// watchNotifies 挂接业务通道推送：成局通知携带**本人**票据（直连入场凭据的唯一来源）。
func (c *battleClient) watchNotifies() {
	c.cli.On(gamev1opclient.PlayerServicePushOps.MatchStartedNotify, func(_ string, payload []byte) {
		var n gamev1.MatchStartedNotify
		if protojson.Unmarshal(payload, &n) != nil {
			return
		}
		c.mu.Lock()
		c.ticket = append([]byte(nil), n.GetBattleTicket()...)
		c.mu.Unlock()
	})
}

// watchDirect 记录直连帧面上的战斗域通知（帧广播、战斗结束与出局）。
func (c *battleClient) watchDirect(operation string, payload []byte) {
	switch operation {
	case battlev1opclient.BattleServicePushOps.FrameBroadcast:
		var fb battlev1.FrameBroadcast
		if protojson.Unmarshal(payload, &fb) != nil {
			return
		}
		c.mu.Lock()
		c.frames = append(c.frames, &fb)
		c.mu.Unlock()
	case battlev1opclient.BattleServicePushOps.BattleEndNotify:
		var end battlev1.BattleEndNotify
		if protojson.Unmarshal(payload, &end) != nil {
			return
		}
		c.mu.Lock()
		c.ends = append(c.ends, &end)
		c.mu.Unlock()
	case battlev1opclient.BattleServicePushOps.PlayerOutNotify:
		var out battlev1.PlayerOutNotify
		if protojson.Unmarshal(payload, &out) != nil {
			return
		}
		c.mu.Lock()
		c.outs = append(c.outs, &out)
		c.mu.Unlock()
	}
}

// setTicket 直接注入本人票据（用例自行出票时用；与成局推送写入的是同一个字段）。
func (c *battleClient) setTicket(raw []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ticket = append([]byte(nil), raw...)
}

// waitOut 等待收到关于 outPlayer 的出局广播（断言超时失败）。
func (c *battleClient) waitOut(t *testing.T, outPlayer string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		c.mu.Lock()
		found := false
		for _, o := range c.outs {
			if o.GetPlayerId() == outPlayer {
				found = true
				break
			}
		}
		c.mu.Unlock()
		if found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s 未收到 %s 的出局广播", c.playerID, outPlayer)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// assertNoOut 断言未收到任何出局广播（窗口内回座：不得误判掉线）。
func (c *battleClient) assertNoOut(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.outs) != 0 {
		t.Fatalf("%s 收到不该有的出局广播: %+v", c.playerID, c.outs)
	}
}

// assertNoEnd 断言未收到战斗结束通知（回座用例：对局不应提前结算）。
func (c *battleClient) assertNoEnd(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.ends) != 0 {
		t.Fatalf("%s 收到不该有的结束通知: %+v", c.playerID, c.ends)
	}
}

// waitTicket 等待成局通知送达（拿到本人票据才能入局）。
func (c *battleClient) waitTicket(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if len(c.ticketOf()) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s 未收到成局通知（无票不入局）", c.playerID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// joinBattle 直连入局（battle actor 懒激活期间重试）。
func (c *battleClient) joinBattle(t *testing.T, ctx context.Context, battleID string) *battlev1.JoinBattleReply {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		var join battlev1.JoinBattleReply
		err := c.frame.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
			&battlev1.JoinBattleReq{BattleId: battleID}, &join)
		if err == nil {
			return &join
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("JoinBattle 重试耗尽: %v", lastErr)
	return nil
}

// syncFrames 补帧：从 lastSeen 之后拉缺失帧（重连后按断点补）。
func (c *battleClient) syncFrames(ctx context.Context, battleID string, lastSeen uint64) (*battlev1.SyncFramesReply, error) {
	var sync battlev1.SyncFramesReply
	err := c.frame.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SyncFrames,
		&battlev1.SyncFramesReq{BattleId: battleID, LastSeenFrame: lastSeen}, &sync)
	return &sync, err
}

// sendFrames 发送 [from,to] 帧输入（Tell 无回执；payload 单字节步进值）。
// 载荷里**不填** player_id：身份由帧槽票据经投递 sender 注入（伪造载荷字段无意义）。
func (c *battleClient) sendFrames(t *testing.T, ctx context.Context, battleID string, from, to uint64, step byte) {
	t.Helper()
	for i := from; i <= to; i++ {
		req := &battlev1.FrameInputReq{
			BattleId: battleID,
			Input:    &locksteppb.LockstepInput{FrameId: i, Payload: []byte{step}},
		}
		if err := c.frame.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SendFrameInput, req, nil); err != nil {
			t.Fatalf("SendFrameInput(%d): %v", i, err)
		}
	}
}

// waitFrames 等待收到至少 n 帧广播（断言超时失败）。
func (c *battleClient) waitFrames(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		got := len(c.frames)
		c.mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("仅收到 %d 帧广播", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitEnd 等待战斗结束通知并返回胜者。
func (c *battleClient) waitEnd(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		got := len(c.ends)
		var winner string
		if got > 0 {
			winner = c.ends[0].GetWinnerPlayerId()
		}
		c.mu.Unlock()
		if got > 0 {
			return winner
		}
		if time.Now().After(deadline) {
			t.Fatal("未收到战斗结束通知")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitFrameAfter 等待收到帧号大于 floor 的帧广播（重连回座的判据：直连恢复且帧继续）。
func (c *battleClient) waitFrameAfter(t *testing.T, floor uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if last := c.lastFrame(); last != nil && last.GetFrame().GetFrameId() > floor {
			return last.GetFrame().GetFrameId()
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s 重连后未收到新帧（floor=%d）", c.playerID, floor)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// lastFrame 返回最新一帧广播。
func (c *battleClient) lastFrame() *battlev1.FrameBroadcast {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.frames) == 0 {
		return nil
	}
	return c.frames[len(c.frames)-1]
}
