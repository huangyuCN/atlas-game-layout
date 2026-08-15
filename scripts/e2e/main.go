// e2e 客户端脚本（M5 骨架：注册/登录片段；M8 扩展匹配/战斗/双通道）。
// 用法：go run ./scripts/e2e -gw 127.0.0.1:19001
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
)

func main() {
	gw := flag.String("gw", "127.0.0.1:19001", "gateway TCP 地址")
	account := flag.String("account", "", "账号（空则自动生成）")
	password := flag.String("password", "pw", "口令")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := tcpt.NewClient(*gw)
	if err != nil {
		fatalf("连接 gateway 失败: %v", err)
	}
	defer cli.Close()
	cli.OnNotify(func(operation string, payload []byte) {
		fmt.Printf("[推送] %s %s\n", operation, payload)
	})
	auth := gatewayv1.NewGatewayAuthTCPClient(cli)

	name := *account
	if name == "" {
		name = fmt.Sprintf("e2e-%d", time.Now().UnixMilli())
	}

	// 注册。
	reg, err := auth.Register(ctx, &gatewayv1.RegisterRequest{Account: name, Password: *password, Nickname: name})
	if err != nil {
		fatalf("注册失败: %v", err)
	}
	fmt.Printf("[注册] 玩家 ID: %s\n", reg.GetPlayerId())

	// 登录。
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: *password})
	if err != nil {
		fatalf("登录失败: %v", err)
	}
	fmt.Printf("[登录] token: %s\n", login.GetToken())

	// 心跳。
	if _, err := auth.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: login.GetPlayerId(), Token: login.GetToken(), Ts: time.Now().UnixMilli()}); err != nil {
		fatalf("心跳失败: %v", err)
	}
	fmt.Println("[心跳] ok")

	// 登出。
	if _, err := auth.Logout(ctx, &gatewayv1.LogoutRequest{PlayerId: login.GetPlayerId(), Token: login.GetToken()}); err != nil {
		fatalf("登出失败: %v", err)
	}
	fmt.Println("[登出] ok")
	fmt.Println("e2e 注册/登录片段通过")
}

func fatalf(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
	panic("e2e failed")
}
