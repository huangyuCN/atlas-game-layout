// 接入层实例列表的配置解析用例：edge_instances 与 edge_endpoints 互斥、单元素等价、
// 每个实例各自校验（面不重复/地址非空）。
package app

import (
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
)

// instance 返回一个接入层实例配置（三面地址）。
func instance(name string, wsAddr string) *conf.EdgeInstance {
	return &conf.EdgeInstance{
		Name: name,
		Endpoints: []*battlev1.EdgeEndpoint{
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: wsAddr},
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: wsAddr + "-kcp"},
		},
	}
}

// TestEdgeInstancesOfSingleElement 验证「未配 edge_instances」时把 edge_endpoints 当唯一实例：
// 单元素行为与改造前完全一致（回执/逐人地址都取这一份）。
func TestEdgeInstancesOfSingleElement(t *testing.T) {
	b := &conf.BattleConf{EdgeEndpoints: []*battlev1.EdgeEndpoint{
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.example.com:7100"},
	}}
	got, err := edgeInstancesOf(b)
	if err != nil {
		t.Fatalf("单实例简写应合法: %v", err)
	}
	if len(got) != 1 || len(got[0]) != 1 || got[0][0].GetAddress() != "edge.example.com:7100" {
		t.Fatalf("实例列表 = %v, 期望恰好一个实例一面 edge.example.com:7100", got)
	}
}

// TestEdgeInstancesOfList 验证多实例列表逐实例解析（顺序与地址逐字保留）。
func TestEdgeInstancesOfList(t *testing.T) {
	b := &conf.BattleConf{EdgeInstances: []*conf.EdgeInstance{
		instance("edge-1", "edge-1.example.com:7100"),
		instance("edge-2", "edge-2.example.com:7100"),
	}}
	got, err := edgeInstancesOf(b)
	if err != nil {
		t.Fatalf("多实例列表应合法: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("实例数 = %d, 期望 2", len(got))
	}
	if got[0][0].GetAddress() != "edge-1.example.com:7100" || got[1][0].GetAddress() != "edge-2.example.com:7100" {
		t.Fatalf("实例地址 = %v/%v, 期望逐字保留", got[0], got[1])
	}
}

// TestEdgeInstancesOfMutuallyExclusive 验证两种地址来源同时配置即启动失败，
// 避免「到底按哪份地址下发」的二义。
func TestEdgeInstancesOfMutuallyExclusive(t *testing.T) {
	b := &conf.BattleConf{
		EdgeEndpoints: []*battlev1.EdgeEndpoint{
			{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge.example.com:7100"},
		},
		EdgeInstances: []*conf.EdgeInstance{instance("edge-1", "edge-1.example.com:7100")},
	}
	if _, err := edgeInstancesOf(b); err == nil {
		t.Fatal("edge_endpoints 与 edge_instances 同时配置应启动失败（地址来源必须唯一）")
	}
}

// TestEdgeInstancesOfRejectsBadInstance 验证实例内的面校验沿用单实例规则（面不重复）。
func TestEdgeInstancesOfRejectsBadInstance(t *testing.T) {
	bad := &conf.EdgeInstance{Name: "edge-1", Endpoints: []*battlev1.EdgeEndpoint{
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge-1.example.com:7100"},
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "edge-1.example.com:7200"},
	}}
	if _, err := edgeInstancesOf(&conf.BattleConf{EdgeInstances: []*conf.EdgeInstance{bad}}); err == nil {
		t.Fatal("同一实例内传输面重复应启动失败")
	}
}
