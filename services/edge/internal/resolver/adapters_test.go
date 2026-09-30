package resolver

import (
	"testing"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/registry"
)

// TestInstanceOfMapsMetadataAndEndpoints 覆盖注册实例 → 帧面实例的元数据契约映射。
func TestInstanceOfMapsMetadataAndEndpoints(t *testing.T) {
	inst := instanceOf(&registry.ServiceInstance{
		ID: "battle-node-2-frame",
		Metadata: map[string]string{
			consts.FrameMetaNodeID: "battle-node-2",
			consts.FrameMetaHost:   "10.0.0.7",
			consts.FrameMetaPortWS: "9401", consts.FrameMetaPortKCP: "9402", consts.FrameMetaPortUDP: "9403",
		},
		Endpoints: []string{"tcp://10.0.0.7:9401"},
	})
	if inst.NodeID != "battle-node-2" || inst.Host != "10.0.0.7" || inst.EndpointHost != "10.0.0.7" {
		t.Fatalf("实例映射不对: %+v", inst)
	}
	if inst.Address(consts.FrameMetaPortKCP) != "10.0.0.7:9402" {
		t.Fatalf("kcp 面地址不对: %s", inst.Address(consts.FrameMetaPortKCP))
	}
	if inst.hasPort(consts.FrameMetaPortWS) != true || inst.hasPort("grpc") {
		t.Fatal("端口判定不对")
	}
}

// TestInstanceOfFallsBackToEndpointHost 覆盖未配 host 元数据时取端点主机；无端点则为空。
func TestInstanceOfFallsBackToEndpointHost(t *testing.T) {
	inst := instanceOf(&registry.ServiceInstance{
		ID:        "f1",
		Metadata:  map[string]string{consts.FrameMetaPortWS: "9401"},
		Endpoints: []string{"grpc://10.1.1.1:9300", "tcp://10.2.2.2:9401"},
	})
	if inst.Host != "" || inst.EndpointHost != "10.1.1.1" {
		t.Fatalf("端点主机兜底不对: %+v", inst)
	}
	if inst.Address(consts.FrameMetaPortWS) != "10.1.1.1:9401" {
		t.Fatalf("地址应为端点主机:端口，实际 %s", inst.Address(consts.FrameMetaPortWS))
	}
	empty := instanceOf(&registry.ServiceInstance{ID: "f2"})
	if empty.EndpointHost != "" || empty.hasPort(consts.FrameMetaPortWS) {
		t.Fatalf("空实例不应有主机与端口: %+v", empty)
	}
}

// TestAdaptersValidateDeps 覆盖适配器构造的依赖校验。
func TestAdaptersValidateDeps(t *testing.T) {
	if _, err := NewLocatorDirectory(nil); err == nil {
		t.Fatal("locator 为空应报错")
	}
	if _, err := NewDiscoveryRegistry(nil); err == nil {
		t.Fatal("服务发现为空应报错")
	}
}
