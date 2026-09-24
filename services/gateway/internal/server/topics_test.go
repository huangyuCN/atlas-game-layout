package server

import (
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas/namespace"
	"github.com/nats-io/nats.go"
)

// testNamespace 是单测使用的命名空间 token（业务 topic 前缀 atlas.test.* 由框架派生）：
// 必须与测试内构造 Gateway 时传入的同一个值，否则订阅端与发布端 subject 对不上。
const testNamespace = "test"

// testTopics 是单测使用的业务 topic 构造器。
var testTopics = mustTopics()

// mustTopics 派生测试用 topic 构造器（夹具里非法值直接 panic）。
func mustTopics() consts.Topics {
	derived, err := namespace.Derive(testNamespace)
	if err != nil {
		panic(err)
	}
	topics, err := consts.NewTopics(derived)
	if err != nil {
		panic(err)
	}
	return topics
}

// testPublisher 用测试命名空间构造发布器（与 Gateway 构造时传入的一致）。
func testPublisher(nc *nats.Conn) *pkgnats.Publisher { return pkgnats.NewPublisher(nc, testTopics) }
