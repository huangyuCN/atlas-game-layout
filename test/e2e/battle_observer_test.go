package e2e

import (
	"context"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	natsgo "github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/protobuf/encoding/protojson"
)

// subscribeSettled 订阅战斗结算事件（nats 事件总线侧观察点）。
func subscribeSettled(t *testing.T, nc *natsgo.Conn) <-chan *battlev1.BattleSettledEvent {
	t.Helper()
	ch := make(chan *battlev1.BattleSettledEvent, 4)
	if _, err := nats.Subscribe(nc, e2eTopics.Event("battle.settled"), func(_ string, data []byte) {
		var ev battlev1.BattleSettledEvent
		if err := protojson.Unmarshal(data, &ev); err == nil {
			ch <- &ev
		}
	}); err != nil {
		t.Fatalf("订阅结算事件: %v", err)
	}
	return ch
}

// battleObserver 是成局/结算事件订阅（nats 事件总线可观测性）。
type battleObserver struct {
	started <-chan *matcherv1.MatchStartedEvent
	settled <-chan *battlev1.BattleSettledEvent
}

// newBattleObserver 建立观察 nats 连接并订阅成局与结算事件。
func newBattleObserver(t *testing.T) *battleObserver {
	t.Helper()
	nc, err := nats.Connect(nats.Options{URL: itNatsURL, Name: "e2e-battle-observer"})
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	t.Cleanup(nc.Close)
	startedCh := make(chan *matcherv1.MatchStartedEvent, 4)
	if _, err := nats.Subscribe(nc, e2eTopics.MatchStarted(), func(_ string, data []byte) {
		var ev matcherv1.MatchStartedEvent
		if err := protojson.Unmarshal(data, &ev); err == nil {
			startedCh <- &ev
		}
	}); err != nil {
		t.Fatalf("订阅成局事件: %v", err)
	}
	return &battleObserver{started: startedCh, settled: subscribeSettled(t, nc)}
}

// battleResultDoc 是落库结果的读取投影（只取用例断言的字段）。
type battleResultDoc struct {
	Players []struct {
		PlayerID string `bson:"player_id"`
		Win      bool   `bson:"win"`
		Score    int32  `bson:"score"`
	} `bson:"players"`
	TotalFrames uint64 `bson:"total_frames"`
}

// decodeResult 轮询读取战斗落库结果（mongo 写入与结算事件之间存在微小延迟，超时即失败）。
func decodeResult(t *testing.T, ctx context.Context, battleID string) battleResultDoc {
	t.Helper()
	mc, err := pkgmongo.NewClient(ctx, pkgmongo.Options{URI: itMongoURI, Database: itMongoDB})
	if err != nil {
		t.Fatalf("mongo: %v", err)
	}
	defer mc.Close(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var res battleResultDoc
		err := mc.Collection("battle_results").FindOne(ctx, bson.M{"_id": battleID}).Decode(&res)
		if err == nil {
			return res
		}
		if time.Now().After(deadline) {
			t.Fatalf("战斗结果未落库: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// assertResultSaved 断言 mongo 已落库战斗结果且胜者一致（正常打完的对局：胜者得分为 1）。
func assertResultSaved(t *testing.T, ctx context.Context, battleID, winner string) {
	t.Helper()
	for _, p := range decodeResult(t, ctx, battleID).Players {
		if p.PlayerID == winner && (!p.Win || p.Score != 1) {
			t.Fatalf("落库胜者不符: %+v", p)
		}
	}
}

// assertWalkoverSaved 断言掉线判负的落库口径：名单保留掉线者、胜者记 Win=true、掉线者记 Win=false。
// 判负不发赛道得分（Score 保持 0），故不按得分断言。
func assertWalkoverSaved(t *testing.T, ctx context.Context, battleID, winner, loser string) {
	t.Helper()
	res := decodeResult(t, ctx, battleID)
	if len(res.Players) != 2 {
		t.Fatalf("落库名单不符（判负结算必须保留完整名单）: %+v", res.Players)
	}
	seen := make(map[string]bool, len(res.Players))
	for _, p := range res.Players {
		seen[p.PlayerID] = true
		if p.PlayerID == winner && !p.Win {
			t.Fatalf("胜者未记胜: %+v", p)
		}
		if p.PlayerID == loser && p.Win {
			t.Fatalf("掉线者被记胜: %+v", p)
		}
	}
	if !seen[winner] || !seen[loser] {
		t.Fatalf("落库名单缺少玩家: %+v", res.Players)
	}
	if res.TotalFrames == 0 {
		t.Fatalf("判负结算未记录总帧数: %+v", res)
	}
}
