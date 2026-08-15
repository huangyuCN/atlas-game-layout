package server

import (
	"context"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"google.golang.org/protobuf/encoding/protojson"
)

// TestKickCrossInstance 验证跨实例挤下线：
// 实例 A 登录 → 实例 B 登录 → 经 nats 控制通道 A 的旧连接收到被挤下线通知。
func TestKickCrossInstance(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	envA := newGWEnv(t, "gw-a", mr, natsURL)
	envB := newGWEnv(t, "gw-b", mr, natsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cliA, authA := envA.newTCPAuthClient(t)
	loginA, err := authA.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("A 登录: %v", err)
	}
	kicked := make(chan struct{}, 1)
	cliA.OnNotify(func(operation string, payload []byte) {
		if operation == consts.PushOpKickedOffline {
			var kn gatewayv1.KickedNotify
			if err := protojson.Unmarshal(payload, &kn); err == nil && kn.GetReason() == "logged_in_elsewhere" {
				kicked <- struct{}{}
			}
		}
	})

	// 实例 B 登录同一玩家，触发跨实例挤下线。
	_, authB := envB.newTCPAuthClient(t)
	if _, err := authB.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"}); err != nil {
		t.Fatalf("B 登录: %v", err)
	}
	select {
	case <-kicked:
	case <-time.After(3 * time.Second):
		t.Fatal("A 的旧连接未收到被挤下线通知")
	}

	// 路由已归属 B；A 的旧令牌失效。
	r, err := envA.sess.Route(ctx, "p-1")
	if err != nil || r == nil || r.InstanceID != "gw-b" {
		t.Fatalf("路由 = %+v, err = %v, want gw-b", r, err)
	}
	if _, err := authA.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: "p-1", Token: loginA.GetToken(), Ts: 1}); err == nil {
		t.Fatal("A 旧令牌心跳应失败")
	}
	// A 本地会话已清理（控制通道处理完成）。
	if envA.sess.SessionCount() != 0 {
		t.Fatalf("A 本地会话数 = %d, want 0", envA.sess.SessionCount())
	}
}

// TestPushOnlyOwnerDelivers 验证双实例推送专项：仅持有连接的实例下发。
func TestPushOnlyOwnerDelivers(t *testing.T) {
	mr, natsURL, pub := newSharedBackends(t)
	envA := newGWEnv(t, "gw-a", mr, natsURL)
	envB := newGWEnv(t, "gw-b", mr, natsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cliA, authA := envA.newTCPAuthClient(t)
	if _, err := authA.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"}); err != nil {
		t.Fatalf("A 登录 p-1: %v", err)
	}
	cliB, authB := envB.newTCPAuthClient(t)
	if _, err := authB.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-2", Password: "x"}); err != nil {
		t.Fatalf("B 登录 p-2: %v", err)
	}

	gotA := make(chan struct{}, 1)
	gotB := make(chan struct{}, 1)
	cliA.OnNotify(func(operation string, payload []byte) {
		if operation == consts.PushOpMatchStarted {
			gotA <- struct{}{}
		}
	})
	cliB.OnNotify(func(operation string, payload []byte) {
		if operation == consts.PushOpMatchStarted {
			gotB <- struct{}{}
		}
	})

	// 推送 p-1：仅实例 A 下发。
	if err := PublishPush(ctx, pub, "p-1", consts.PushOpMatchStarted, []byte(`{"match_id":"m-1","battle_id":"b-1"}`)); err != nil {
		t.Fatalf("PublishPush p-1: %v", err)
	}
	select {
	case <-gotA:
	case <-time.After(2 * time.Second):
		t.Fatal("A 未收到 p-1 推送")
	}
	select {
	case <-gotB:
		t.Fatal("B 不应收到 p-1 推送")
	case <-time.After(300 * time.Millisecond):
	}

	// 推送 p-2：仅实例 B 下发。
	if err := PublishPush(ctx, pub, "p-2", consts.PushOpMatchStarted, []byte(`{"match_id":"m-2","battle_id":"b-2"}`)); err != nil {
		t.Fatalf("PublishPush p-2: %v", err)
	}
	select {
	case <-gotB:
	case <-time.After(2 * time.Second):
		t.Fatal("B 未收到 p-2 推送")
	}
}

// TestKickCrossInstanceKeepsNewRoute 验证跨实例挤下线不会误删新实例路由。
func TestKickCrossInstanceKeepsNewRoute(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	envA := newGWEnv(t, "gw-a", mr, natsURL)
	envB := newGWEnv(t, "gw-b", mr, natsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cliA, authA := envA.newTCPAuthClient(t)
	if _, err := authA.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"}); err != nil {
		t.Fatalf("A 登录: %v", err)
	}
	kicked := make(chan struct{}, 1)
	cliA.OnNotify(func(operation string, payload []byte) {
		if operation == consts.PushOpKickedOffline {
			kicked <- struct{}{}
		}
	})
	_, authB := envB.newTCPAuthClient(t)
	if _, err := authB.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"}); err != nil {
		t.Fatalf("B 登录: %v", err)
	}
	select {
	case <-kicked:
	case <-time.After(3 * time.Second):
		t.Fatal("未收到挤下线通知")
	}
	// B 的路由必须完好（A 的控制通道清理不得误删）。
	// 轮询观察一小段时间：若 A 侧误删发生，B 路由会在该窗口内消失。
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		r, err := envB.sess.Route(ctx, "p-1")
		if err != nil || r == nil || r.InstanceID != "gw-b" {
			t.Fatalf("B 路由被误删: r=%+v err=%v", r, err)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// B 心跳仍有效。
	r, err := envB.sess.Route(ctx, "p-1")
	if err != nil || r == nil {
		t.Fatalf("B 路由读取失败: r=%+v err=%v", r, err)
	}
	if _, err := authB.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: "p-1", Token: r.Token, Ts: 1}); err != nil {
		t.Fatalf("B 心跳失败: %v", err)
	}
}
