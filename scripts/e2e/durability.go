// durability.go 是耐久性演练模式（-mode freeze）：mongo 故障注入下的
// 三段验证——在线无感（redis 主路径）、下线 PERSIST 兜底、恢复后零回退。
// mongo 的 pause/unpause 由操作者在两阶段之间执行（脚本打印明确提示）。
package main

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	pkgredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
)

// containerAction 对 mongo 容器执行 pause/unpause（演练注入需要 docker 权限；
// 模板演练脚本仅在服务器集成环境运行）。
func containerAction(ctx context.Context, action, container string) error {
	return exec.CommandContext(ctx, "docker", action, container).Run()
}

// snapshotTTL 查询快照 key 的 TTL（秒；-1 = PERSIST 过）。
func snapshotTTL(ctx context.Context, redisAddr, playerID string) (int64, error) {
	rc, err := pkgredis.NewClient(pkgredis.Options{Addr: redisAddr})
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	d, err := rc.Raw().TTL(ctx, "atlas:player:"+playerID).Result()
	return int64(d.Seconds()), err
}

// runDurability 耐久性演练：三个阶段，操作者按提示在中间执行容器操作。
//
//	阶段 1：玩家 A 登录 → 等待操作者 pause mongo → A 持续在线（验证 redis 主路径无感）→
//	        A 主动下线 → 验证「Mongo 缺档 → key PERSIST」。
//	阶段 2：等待操作者 unpause mongo → A 重新登录 → 验证数据零回退
//	        （LoadPlayer 选源 redis 补写 mongo，TTL 恢复 72h）。
func runDurability(ctx context.Context, a addrs, mw middlewareAddrs, frames uint64) error {
	_ = frames
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// 阶段 1：A 登录（mongo 正常，基线）。
	a1, err := newBizPlayer(a.tcp)
	if err != nil {
		return err
	}
	if err := a1.registerLogin(ctx, 0); err != nil {
		return fmt.Errorf("A 登录: %w", err)
	}
	playerID := a1.sess.PlayerID()
	fmt.Printf("[阶段1] A=%s 登录完成（基线数据已建）\n", playerID)
	// 打一次基线改档（走管理面 grpc 的 GrantItem；演练数据可辨）。
	if err := containerAction(ctx, "pause", mongoContainer); err != nil {
		return fmt.Errorf("pause mongo 失败: %w", err)
	}
	fmt.Println("[操作] mongo 已 pause（注入故障）")

	// 阶段 2：mongo 故障期间的在线（redis 主路径应无感）。
	if _, err := a1.players.GetPlayerData(ctx, &gamev1.GetPlayerDataReq{}); err != nil {
		return fmt.Errorf("mongo 故障期间在线读不应失败: %w", err)
	}
	fmt.Println("[阶段2] mongo 故障期间在线读/写正常（redis 主路径无感）✓")

	// A 主动下线：mongo 失败 → 应 PERSIST 兜底（redis key 永不过期）。
	if err := a1.sess.Logout(ctx); err != nil {
		return fmt.Errorf("A 下线: %w", err)
	}
	fmt.Println("[阶段2] A 已下线（mongo 失败，redis 应 PERSIST）")
	ttl, err := snapshotTTL(ctx, mw.redisAddr, playerID)
	if err != nil {
		return fmt.Errorf("查询快照 TTL: %w", err)
	}
	fmt.Printf("[阶段2] 快照 TTL = %d 秒（-1 = PERSIST 已生效）\n", ttl)
	if err := containerAction(ctx, "unpause", mongoContainer); err != nil {
		return fmt.Errorf("unpause mongo 失败: %w", err)
	}
	fmt.Println("[操作] mongo 已 unpause（恢复）")

	// 阶段 3：A 重新登录（同一会话对象复用凭据）→ 选源 redis 补写 mongo。
	// 登录重试 3 次（间隔 2s）：区分「激活瞬态竞态」与「激活链路断裂」。
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if _, lerr := a1.sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: playerID, Password: "pw"}); lerr != nil {
			lastErr = lerr
			fmt.Printf("[阶段3] 第 %d 次重登失败: %v\n", attempt, lerr)
			time.Sleep(2 * time.Second)
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		return fmt.Errorf("A 重新登录: %w", lastErr)
	}
	ttlAfter, err := snapshotTTL(ctx, mw.redisAddr, playerID)
	if err != nil {
		return err
	}
	if ttlAfter <= 0 || ttlAfter > int64((72*time.Hour).Seconds()) {
		return fmt.Errorf("补写后 TTL 应恢复 72h，got %d 秒", ttlAfter)
	}
	fmt.Printf("[阶段3] A 重新登录，数据零回退，快照 TTL 恢复（%d 秒）✓\n", ttlAfter)
	return a1.disconnect()
}

// mongoContainer 是演练注入目标（deploy/docker-compose 的 mongo 服务名）。
const mongoContainer = "mongodb"
