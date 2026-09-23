// Package e2e 提供跨服务的端到端集成测试（真 etcd/redis/nats/mongo，
// 本地 Docker 端口与集成服务器约定一致；不可达时跳过，Skipf 风格）。
package e2e

import (
	"context"
	"fmt"
	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
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

// itNamespace 是本次测试运行独占的注册中心键前缀：与常驻进程（按 runtime.env 派生）
// 隔离，也不会命中上一次运行残留的实例键（租约未过期）导致注册冲突。
var itNamespace = fmt.Sprintf("%s/it-%d", pkgregistry.NamespacePrefix, time.Now().UnixNano())

// e2eTopics 是本次测试运行独占的业务 topic 命名空间：与进程内装配派生规则同源
// （assemble.Options.Namespace 的叶子段），发布方与订阅方必须一致，否则消息落在不同 subject。
var e2eTopics = consts.NewTopics(mustActorNamespace(itNamespace))

// mustActorNamespace 派生 actor 命名空间；测试夹具里非法值直接 panic（前缀由本文件生成，不会非法）。
func mustActorNamespace(registryNS string) string {
	ns, err := bootstrap.ActorNamespaceOf(registryNS)
	if err != nil {
		panic(err)
	}
	return ns
}

// probeCore 探测基础中间件（etcd/redis/nats）；不可用返回 skip 原因。
func probeCore(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := pkredis.NewClient(pkredis.Options{Addrs: []string{itRedisAddr}})
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
		RedisAddrs:    []string{itRedisAddr},
		MongoURI:      itMongoURI,
		MongoDB:       itMongoDB,
		Namespace:     itNamespace,
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
		RedisAddrs:    []string{itRedisAddr},
		Namespace:     itNamespace,
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
		Namespace:     itNamespace,
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
	cli := battlev1.NewBattleServiceClusterClient(b.Runtime)
	if _, err := cli.Create(ctx, pid, &battlev1.CreateBattleRequest{
		MatchId:   "m-seed-" + battleID,
		PlayerIds: playerIDs,
	}); err != nil {
		t.Fatalf("seedBattle 开局: %v", err)
	}
}
