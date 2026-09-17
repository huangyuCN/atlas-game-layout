// durability.go 是耐久性演练模式（-mode freeze）：mongo 故障注入下的
// 四段验证——
//  1. 基线：登录 → 管理面发放基线道具 → mongo pause（注入故障）→ 在线读/写无感
//     （redis 主路径）→ 追加发放；
//  2. 下线：mongo 失败 → 四组合表「成功|失败」→ redis 快照 PERSIST 兜底（TTL=-1）；
//  3. 恢复：unpause → 重登 → 登录选源以 redis 为准补写 mongo → 快照 TTL 恢复 72h，
//     数据零回退（追加道具存活）；
//  4. 认证保留：第二次下线（双写成功）→ 第三次登录必须成功
//     （回归：mongo 更新写不携带认证字段，凭据不被内存副本覆盖为空）。
//
// mongo 的 pause/unpause 由脚本自动执行（docker 权限；模板演练仅在服务器集成环境运行）。
package main

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	pkgredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// 演练改档数据（可辨：基线 7 + 追加 3 = 10）。
const (
	grantItemID     = 1001
	grantBaseCount  = 7
	grantExtraCount = 3
)

// mongoContainer 是演练注入目标（deploy/docker-compose 的 mongo 服务名）。
const mongoContainer = "mongodb"

// containerAction 对 mongo 容器执行 pause/unpause（演练注入需要 docker 权限）。
func containerAction(ctx context.Context, action, container string) error {
	return exec.CommandContext(ctx, "docker", action, container).Run()
}

// snapshotTTL 查询快照 key 的 TTL；PERSIST 过返回 -1ns，键不存在返回 -2ns
// （go-redis 语义；直接返回 Duration，避免秒级截断把负值归零）。
func snapshotTTL(ctx context.Context, redisAddr, playerID string) (time.Duration, error) {
	rc, err := pkgredis.NewClient(pkgredis.Options{Addr: redisAddr})
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	return rc.Raw().TTL(ctx, "atlas:player:"+playerID).Result()
}

// assertPersistedTTL 轮询等待快照 key PERSIST（TTL = -1ns：未落库权威副本语义）。
// Logout 是异步 Tell：actor 侧下线落库（含 mongo 30s 服务选择超时的重试）在后台
// 完成，PERSIST 生效有延迟，必须轮询。
func assertPersistedTTL(ctx context.Context, redisAddr, playerID, what string) error {
	deadline := time.Now().Add(150 * time.Second)
	var last time.Duration
	for {
		d, err := snapshotTTL(ctx, redisAddr, playerID)
		if err == nil {
			last = d
			if d == -1*time.Nanosecond {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: 快照应 PERSIST（TTL=-1），got %v（等待超时）", what, last)
		}
		time.Sleep(2 * time.Second)
	}
}

// assertExpiringTTL 断言快照 TTL 已恢复正常租期（0 < TTL ≤ 72h）。
func assertExpiringTTL(ctx context.Context, redisAddr, playerID, what string) error {
	d, err := snapshotTTL(ctx, redisAddr, playerID)
	if err != nil {
		return fmt.Errorf("%s: 查询快照 TTL: %w", what, err)
	}
	if d <= 0 || d > 72*time.Hour {
		return fmt.Errorf("%s: 快照 TTL 应恢复 72h 租期，got %v", what, d)
	}
	return nil
}

// grantItems 管理面发放道具（集群互调，不经 gateway 路由）：演练的可辨改档数据，
// 重登后据此断言数据零回退。
func grantItems(ctx context.Context, mw middlewareAddrs, playerID string, itemID, count uint32) error {
	rt, err := pkgactor.NewRuntime(pkgactor.Options{
		NodeID: "e2e-admin", ServiceName: consts.ServiceGame,
		EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL,
	})
	if err != nil {
		return fmt.Errorf("e2e: 构造管理面运行时失败: %w", err)
	}
	if err := rt.Start(ctx); err != nil {
		return fmt.Errorf("e2e: 管理面运行时启动失败: %w", err)
	}
	defer func() { _ = rt.Shutdown(ctx) }()
	pid, err := types.NewPID(consts.ActorTypePlayer, playerID)
	if err != nil {
		return fmt.Errorf("e2e: 构造玩家 PID 失败: %w", err)
	}
	cli := gamev1.NewPlayerServiceClusterClient(rt)
	if _, err := cli.GrantItem(ctx, pid, &gamev1.GrantItemReq{ItemId: itemID, Count: count}); err != nil {
		return fmt.Errorf("e2e: 发放道具失败（item=%d×%d）: %w", itemID, count, err)
	}
	return nil
}

// backpackCount 读指定道具的背包数量（缺失记 0）。
func backpackCount(items []*gamev1.BackpackItem, itemID uint32) uint32 {
	for _, it := range items {
		if it.GetItemId() == itemID {
			return it.GetCount()
		}
	}
	return 0
}

// relogin 重新登录（重试 3 次间隔 2s：区分激活瞬态竞态与激活链路断裂）。
func relogin(ctx context.Context, p *player, playerID string) error {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if _, lerr := p.sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: playerID, Password: "pw"}); lerr != nil {
			lastErr = lerr
			fmt.Printf("[重登] 第 %d 次失败: %v\n", attempt, lerr)
			time.Sleep(2 * time.Second)
			continue
		}
		return nil
	}
	return fmt.Errorf("重新登录: %w", lastErr)
}

// runDurability 耐久性演练：四段验证（详见文件头）。
//
//	阶段 1：A 登录（mongo 正常，基线）→ 管理面发放基线道具 → pause mongo →
//	        故障期间在线读/写无感（redis 主路径）→ 追加发放；
//	阶段 2：A 下线 → mongo 失败 → 快照 PERSIST（TTL=-1）断言；
//	阶段 3：unpause → A 重登 → 选源 redis 补写 mongo → TTL 恢复 + 数据零回退断言；
//	阶段 4：二次下线（双写成功）→ 第三次登录 → 认证保留断言。
func runDurability(ctx context.Context, a addrs, mw middlewareAddrs, frames uint64) error {
	_ = frames
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// 演练失败兜底：mongo 恢复运行（幂等），避免注入态遗留影响后续演练/环境。
	defer func() { _ = containerAction(context.WithoutCancel(ctx), "unpause", mongoContainer) }()

	// 阶段 1：A 登录（mongo 正常，基线）+ 基线道具发放。
	a1, err := newBizPlayer(a.tcp)
	if err != nil {
		return err
	}
	if err := a1.registerLogin(ctx, 0); err != nil {
		return fmt.Errorf("A 登录: %w", err)
	}
	playerID := a1.sess.PlayerID()
	if err := grantItems(ctx, mw, playerID, grantItemID, grantBaseCount); err != nil {
		return err
	}
	fmt.Printf("[阶段1] A=%s 登录完成，基线道具 %d×%d 已发放\n", playerID, grantItemID, grantBaseCount)

	// pause mongo（注入故障）。
	if err := containerAction(ctx, "pause", mongoContainer); err != nil {
		return fmt.Errorf("pause mongo 失败: %w", err)
	}
	fmt.Println("[操作] mongo 已 pause（注入故障）")

	// 阶段 2：mongo 故障期间在线无感（redis 主路径）+ 追加发放（仅内存/redis）。
	if _, err := a1.players.GetPlayerData(ctx, &gamev1.GetPlayerDataReq{}); err != nil {
		return fmt.Errorf("mongo 故障期间在线读不应失败: %w", err)
	}
	if err := grantItems(ctx, mw, playerID, grantItemID, grantExtraCount); err != nil {
		return err
	}
	fmt.Println("[阶段2] mongo 故障期间在线读/写正常（redis 主路径无感），追加道具已发放 ✓")

	// A 下线：redis 成功 + mongo 失败 → 四组合表「成功|失败」→ PERSIST 兜底。
	if err := a1.sess.Logout(ctx); err != nil {
		return fmt.Errorf("A 下线: %w", err)
	}
	if err := assertPersistedTTL(ctx, mw.redisAddr, playerID, "阶段2"); err != nil {
		return err
	}
	fmt.Println("[阶段2] A 已下线，快照 PERSIST 兜底（TTL=-1）✓")

	// unpause mongo（恢复）。
	if err := containerAction(ctx, "unpause", mongoContainer); err != nil {
		return fmt.Errorf("unpause mongo 失败: %w", err)
	}
	fmt.Println("[操作] mongo 已 unpause（恢复）")

	// 阶段 3：A 重登 → 选源 redis 胜 → 补写 mongo → EXPIRE 恢复 72h。
	if err := relogin(ctx, a1, playerID); err != nil {
		return err
	}
	if err := assertExpiringTTL(ctx, mw.redisAddr, playerID, "阶段3"); err != nil {
		return err
	}
	// 数据零回退断言：追加道具存活（redis 最新态覆盖 mongo 旧档）。
	data, err := a1.players.GetPlayerData(ctx, &gamev1.GetPlayerDataReq{})
	if err != nil {
		return fmt.Errorf("阶段3 数据同步: %w", err)
	}
	if got := backpackCount(data.GetItems(), grantItemID); got != grantBaseCount+grantExtraCount {
		return fmt.Errorf("阶段3 数据零回退失败: item=%d 期望 %d 实得 %d", grantItemID, grantBaseCount+grantExtraCount, got)
	}
	fmt.Printf("[阶段3] A 重登，数据零回退（item=%d×%d），快照 TTL 恢复 ✓\n", grantItemID, grantBaseCount+grantExtraCount)

	// 阶段 4：二次下线（mongo 已恢复，双写成功）→ 第三次登录必须成功
	// （认证保留回归：mongo 更新写不携带凭据，内存副本不覆盖 mongo 凭据）。
	if err := a1.sess.Logout(ctx); err != nil {
		return fmt.Errorf("A 二次下线: %w", err)
	}
	if err := assertExpiringTTL(ctx, mw.redisAddr, playerID, "阶段4"); err != nil {
		return err
	}
	if err := relogin(ctx, a1, playerID); err != nil {
		return err
	}
	fmt.Println("[阶段4] 第三次登录成功（mongo 凭据未被数据更新抹除）✓")
	return a1.disconnect()
}
