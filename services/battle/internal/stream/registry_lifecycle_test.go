package stream

import (
	"testing"

	"github.com/huangyuCN/atlas/transport"
)

// TestRegistryUnregisterKeepsEndpointIndex 验证推送失败注销只摘当前连接、**保留**端点反查项：
// 真正的掉线信号是帧引擎随后的断开事件，它还要靠反查项找到玩家（摘早了就永远不上报掉线）。
func TestRegistryUnregisterKeepsEndpointIndex(t *testing.T) {
	reg, _, _ := newTestRegistry()
	conn := kcpConn(7, "b-1")
	reg.Register("p-a", conn)

	reg.Unregister("p-a", conn) // 推送失效注销
	if reg.Online("p-a") {
		t.Fatal("注销后仍报在场")
	}
	playerID, battleID, ok := reg.Disconnect(conn.Endpoint())
	if !ok || playerID != "p-a" || battleID != "b-1" {
		t.Fatalf("注销后端点反查 = %q/%q/%v, 期望 p-a/b-1/true（反查项必须保留给引擎断开事件）",
			playerID, battleID, ok)
	}
}

// TestRegistryDisconnectKeepsTakeoverConn 验证接管场景下的端点反查：
// 旧端点断开只摘自己，当前（新）连接不受影响。
func TestRegistryDisconnectKeepsTakeoverConn(t *testing.T) {
	reg, _, _ := newTestRegistry()
	old, fresh := kcpConn(7, "b-1"), kcpConn(8, "b-1")
	reg.Register("p-a", old)
	reg.Register("p-a", fresh)

	playerID, _, ok := reg.Disconnect(old.Endpoint())
	if !ok || playerID != "p-a" {
		t.Fatalf("旧端点反查 = %q/%v, 期望 p-a/true", playerID, ok)
	}
	if !reg.Online("p-a") {
		t.Fatal("旧端点断开摘掉了接管后的新连接")
	}
	if playerID, _, ok := reg.Disconnect(fresh.Endpoint()); !ok || playerID != "p-a" {
		t.Fatalf("新端点反查 = %q/%v, 期望 p-a/true", playerID, ok)
	}
	if reg.Online("p-a") {
		t.Fatal("新端点断开后仍报在场")
	}
}

// TestRegistryDisconnectUnknownEndpoint 验证未登记端点的断开不产生玩家（匿名连接不误报）。
func TestRegistryDisconnectUnknownEndpoint(t *testing.T) {
	reg, _, _ := newTestRegistry()
	if playerID, _, ok := reg.Disconnect(Endpoint{Kind: transport.KindKCP, ID: "404"}); ok || playerID != "" {
		t.Fatalf("未登记端点反查 = %q/%v, 期望空/false", playerID, ok)
	}
}

// TestRegistryUDPPeerReconnect 验证数据报面同一对端键在空闲淘汰后可重新登记，
// 且 `Online` 随之回到真（规格 §9.4 重连窗口内回座）。
func TestRegistryUDPPeerReconnect(t *testing.T) {
	reg, _, _ := newTestRegistry()
	conn := udpConn("1.2.3.4:9", "b-1")
	reg.Register("p-a", conn)
	if _, _, ok := reg.Disconnect(conn.Endpoint()); !ok {
		t.Fatal("UDP 对端淘汰未反查到玩家")
	}
	if reg.Online("p-a") {
		t.Fatal("UDP 对端淘汰后仍报在场")
	}
	reg.Register("p-a", conn)
	if !reg.Online("p-a") {
		t.Fatal("同一对端键重新登记后未报在场")
	}
}

// TestRegistryCloseBattleClearsIndex 验证结算关闭同时清理连接与端点反查项：
// 结算后不该再向已停的 actor 投递无意义的掉线消息（规格 §9.8）。
func TestRegistryCloseBattleClearsIndex(t *testing.T) {
	reg, kcpPort, _ := newTestRegistry()
	reg.Register("p-a", kcpConn(7, "b-1"))
	reg.Register("p-b", kcpConn(8, "b-1"))
	reg.Register("p-c", kcpConn(9, "b-2"))

	reg.CloseBattle("b-1")
	if reg.Count() != 1 || reg.CountBattle("b-1") != 0 {
		t.Fatalf("关闭后连接数 = %d（b-1 剩 %d）, 期望 1/0", reg.Count(), reg.CountBattle("b-1"))
	}
	if len(kcpPort.closed) != 2 {
		t.Fatalf("关闭记录数 = %d, 期望 2", len(kcpPort.closed))
	}
	if _, _, ok := reg.Disconnect(Conn{Kind: transport.KindKCP, ConnID: 7}.Endpoint()); ok {
		t.Fatal("结算关闭后端点仍能被反查命中（反查索引未清理）")
	}
	// 其它对局的连接与反查项不受影响。
	if playerID, _, ok := reg.Disconnect(Conn{Kind: transport.KindKCP, ConnID: 9}.Endpoint()); !ok || playerID != "p-c" {
		t.Fatalf("他局端点被误清理: %q/%v", playerID, ok)
	}
}

// TestRegistryRegisterGuard 验证多面共用同一端点键空间：不同帧面的同号连接互不串号。
func TestRegistryRegisterGuard(t *testing.T) {
	reg, _, _ := newTestRegistry()
	reg.Register("p-a", kcpConn(7, "b-1"))
	if _, _, ok := reg.Disconnect(Conn{Kind: transport.KindWebSocket, ConnID: 7}.Endpoint()); ok {
		t.Fatal("不同帧面的同号连接被误判为同一端点")
	}
	if !reg.Online("p-a") {
		t.Fatal("异面同号端点的断开摘掉了 KCP 连接")
	}
	if _, _, ok := reg.Disconnect(Endpoint{Kind: transport.KindKCP, ID: "7"}); !ok {
		t.Fatal("KCP 端点未命中反查")
	}
}
