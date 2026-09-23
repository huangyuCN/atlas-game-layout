package server

import (
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/nats-io/nats.go"
)

// testTopics 是单测使用的业务 topic 命名空间（空 = default）：
// 必须与测试内构造 Gateway 时传入的同一个值，否则订阅端与发布端 subject 对不上。
var testTopics = consts.NewTopics("")

// testPublisher 用测试命名空间构造发布器（与 Gateway 构造时传入的一致）。
func testPublisher(nc *nats.Conn) *pkgnats.Publisher { return pkgnats.NewPublisher(nc, testTopics) }
