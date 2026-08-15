// Package e2e 提供跨服务的端到端集成测试（真 etcd/redis/nats/mongo，
// 本地 Docker 端口与集成服务器约定一致；不可达时跳过，Skipf 风格）。
package e2e

import (
	"context"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	gameassemble "github.com/huangyuCN/atlas-game-layout/services/game/assemble"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// 集成环境地址（本地 Docker 端口约定，与 10.10.9.36 集成服务器一致）。
const (
	itEtcdEndpoints = "127.0.0.1:12379"
	itRedisAddr     = "127.0.0.1:16379"
	itNatsURL       = "nats://127.0.0.1:14222"
	itMongoURI      = "mongodb://127.0.0.1:27018"
	itMongoDB       = "game_it"
)

// probeCore 探测基础中间件（etcd/redis/nats）；不可用返回 skip 原因。
func probeCore(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := pkredis.NewClient(pkredis.Options{Addr: itRedisAddr})
	if err != nil {
		return "redis 构造失败: " + err.Error()
	}
	if err := cli.Ping(ctx); err != nil {
		_ = cli.Close()
		return "redis 不可用: " + err.Error()
	}
	_ = cli.Close()

	nc, err := pkgnats.Connect(pkgnats.Options{URL: itNatsURL, Name: "probe"})
	if err != nil {
		return "nats 不可用: " + err.Error()
	}
	nc.Close()

	ec, err := etcd.NewClient(etcd.Options{Endpoints: []string{itEtcdEndpoints}})
	if err != nil {
		return "etcd 不可用: " + err.Error()
	}
	_ = ec.Close()
	return ""
}

// probeMiddlewares 探测四中间件（含 mongo）；不可用返回 skip 原因。
func probeMiddlewares(t *testing.T) string {
	t.Helper()
	if reason := probeCore(t); reason != "" {
		return reason
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mc, err := pkgmongo.NewClient(ctx, pkgmongo.Options{URI: itMongoURI, Database: itMongoDB})
	if err != nil {
		return "mongo 不可用: " + err.Error()
	}
	_ = mc.Close(ctx)
	return ""
}

// newGame 起 game 服务进程内装配。
func newGame(t *testing.T) *gameassemble.Game {
	t.Helper()
	g, err := gameassemble.New(context.Background(), gameassemble.Options{
		NodeID:        "game-it",
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		RedisAddr:     itRedisAddr,
		MongoURI:      itMongoURI,
		MongoDB:       itMongoDB,
	})
	if err != nil {
		t.Fatalf("game 装配: %v", err)
	}
	t.Cleanup(func() { _ = g.Stop(context.Background()) })
	return g
}

// newGateway 起一个 gateway 实例进程内装配。
func newGateway(t *testing.T, id string) *gwassemble.Gateway {
	t.Helper()
	gw, err := gwassemble.New(context.Background(), gwassemble.Options{
		ID:            id,
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		RedisAddr:     itRedisAddr,
	})
	if err != nil {
		t.Fatalf("gateway 装配: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop(context.Background()) })
	return gw
}

// newBattle 起 battle 服务进程内装配（cfg 可覆盖战斗参数，nil 用默认）。
func newBattle(t *testing.T, cfg *battleassemble.BattleConfig) *battleassemble.Battle {
	t.Helper()
	b, err := battleassemble.New(context.Background(), battleassemble.Options{
		NodeID:        "battle-it",
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		MongoURI:      itMongoURI,
		MongoDB:       itMongoDB,
		BattleCfg:     cfg,
	})
	if err != nil {
		t.Fatalf("battle 装配: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b
}

// seedBattle 预置战斗成员：直接开局战斗 actor（懒激活），供战斗通道测试使用。
func seedBattle(t *testing.T, ctx context.Context, b *battleassemble.Battle, battleID string, playerIDs ...string) {
	t.Helper()
	pid, err := types.NewPID(consts.ActorTypeBattle, battleID)
	if err != nil {
		t.Fatalf("seedBattle PID: %v", err)
	}
	reply := new(battlev1.CreateBattleReply)
	err = b.Runtime.AskProto(ctx, pid, &battlev1.BattleActorMsg{
		Kind: &battlev1.BattleActorMsg_Create{Create: &battlev1.CreateBattleRequest{
			MatchId:   "m-seed-" + battleID,
			PlayerIds: playerIDs,
		}},
	}, reply)
	if err != nil {
		t.Fatalf("seedBattle 开局: %v", err)
	}
}
