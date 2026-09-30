package resolver

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
)

// stubDirectory 是内存 actor 目录桩（PID → 属主节点）。
type stubDirectory struct {
	owners map[string]string
	err    error
	calls  []string
}

// WatchOwner 实现 Directory：解析用例不需要属主变更监听。
func (d *stubDirectory) WatchOwner(context.Context, string, func(string)) (func(), error) {
	return func() {}, nil
}

// OwnerNode 实现 Directory。
func (d *stubDirectory) OwnerNode(_ context.Context, pid string) (string, error) {
	d.calls = append(d.calls, pid)
	if d.err != nil {
		return "", d.err
	}
	node, ok := d.owners[pid]
	if !ok {
		return "", fmt.Errorf("内存目录查不到 %s", pid)
	}
	return node, nil
}

// stubFrames 是内存帧面实例发现桩。
type stubFrames struct {
	instances []Instance
	err       error
	services  []string
}

// Instances 实现 FrameRegistry。
func (f *stubFrames) Instances(_ context.Context, service string) ([]Instance, error) {
	f.services = append(f.services, service)
	if f.err != nil {
		return nil, f.err
	}
	return f.instances, nil
}

// resolveReq 构造一次解析请求（面名即帧面端口键）。
func resolveReq(battleID, face string) edge.ResolveRequest {
	return edge.ResolveRequest{
		Ticket:   ticket.Ticket{Version: ticket.Version1, PlayerID: "p-1", BattleID: battleID},
		Listener: face, Carrier: edge.CarrierDatagram, RemoteAddr: "127.0.0.1:5000", RemoteIP: "127.0.0.1",
	}
}

// resolveErr 用给定 battle_id 发起一次解析并返回错误（面名取 ws）。
func resolveErr(r *Resolver, battleID string) error {
	_, err := r.Resolve(context.Background(), resolveReq(battleID, "ws"))
	return err
}

// requireUnavailable 断言错误是「后端不可用」拒绝（接入层按该 reason 计数）。
func requireUnavailable(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("应返回拒绝错误，实际 nil")
	}
	var rej *edge.RejectError
	if !errors.As(err, &rej) {
		t.Fatalf("应返回 *edge.RejectError，实际 %T: %v", err, err)
	}
	if rej.Reason != edge.ReasonBackendUnavailable {
		t.Fatalf("拒绝原因应为 %s，实际 %s", edge.ReasonBackendUnavailable, rej.Reason)
	}
}

// TestResolveSelectsOwnerAndFacePort 覆盖命中：目录得属主节点 → 该节点的帧面实例 → 面向端口。
func TestResolveSelectsOwnerAndFacePort(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "battle-node-2"}}
	frames := &stubFrames{instances: []Instance{
		{ID: "f1", NodeID: "battle-node-1", Host: "10.0.0.6", Ports: map[string]string{"ws": "9401"}},
		{ID: "f2", NodeID: "battle-node-2", Host: "10.0.0.7",
			Ports: map[string]string{"ws": "9401", "kcp": "9402", "udp": "9403"}},
	}}
	r, err := New(dir, frames, Options{})
	if err != nil {
		t.Fatalf("构造 Resolver 失败: %v", err)
	}
	cases := map[string]string{"ws": "10.0.0.7:9401", "kcp": "10.0.0.7:9402", "udp": "10.0.0.7:9403"}
	for face, want := range cases {
		backend, err := r.Resolve(context.Background(), resolveReq("b-1", face))
		if err != nil {
			t.Fatalf("面 %s 解析失败: %v", face, err)
		}
		if backend.Address != want {
			t.Fatalf("面 %s 后端应为 %s，实际 %s", face, want, backend.Address)
		}
	}
	if len(dir.calls) != len(cases) || dir.calls[0] != "battle:b-1" {
		t.Fatalf("目录查询入参不对: %v", dir.calls)
	}
	if frames.services[0] != "battle-frame" {
		t.Fatalf("帧面服务名应为 battle-frame，实际 %s", frames.services[0])
	}
}

// TestResolveFallsBackToEndpointHost 覆盖未配 host 元数据时取实例端点主机。
func TestResolveFallsBackToEndpointHost(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	frames := &stubFrames{instances: []Instance{
		{ID: "f1", NodeID: "node-1", EndpointHost: "10.9.9.9", Ports: map[string]string{"ws": "9401"}},
	}}
	r, _ := New(dir, frames, Options{})
	backend, err := r.Resolve(context.Background(), resolveReq("b-1", "ws"))
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if backend.Address != "10.9.9.9:9401" {
		t.Fatalf("后端应为端点主机:9401，实际 %s", backend.Address)
	}
}

// TestResolveOwnerMissing 覆盖目录查不到属主（未开局或已回收）。
func TestResolveOwnerMissing(t *testing.T) {
	r, _ := New(&stubDirectory{owners: map[string]string{}}, &stubFrames{}, Options{})
	requireUnavailable(t, resolveErr(r, "b-404"))
}

// TestResolveDirectoryError 覆盖目录查询失败。
func TestResolveDirectoryError(t *testing.T) {
	dir := &stubDirectory{err: errors.New("etcd 不可用")}
	r, _ := New(dir, &stubFrames{}, Options{})
	requireUnavailable(t, resolveErr(r, "b-1"))
}

// TestResolveNoFrameInstanceOnOwner 覆盖属主节点上没有帧面实例。
func TestResolveNoFrameInstanceOnOwner(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-9"}}
	frames := &stubFrames{instances: []Instance{
		{ID: "f1", NodeID: "node-1", Host: "10.0.0.7", Ports: map[string]string{"ws": "9401"}},
	}}
	r, _ := New(dir, frames, Options{})
	requireUnavailable(t, resolveErr(r, "b-1"))
}

// TestResolveMissingFacePort 覆盖实例存在但该传输面无端口（或端口非数字）。
func TestResolveMissingFacePort(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	for _, ports := range []map[string]string{
		{"kcp": "9402"},
		{"ws": "not-a-port"},
	} {
		frames := &stubFrames{instances: []Instance{{ID: "f1", NodeID: "node-1", Host: "10.0.0.7", Ports: ports}}}
		r, _ := New(dir, frames, Options{})
		requireUnavailable(t, resolveErr(r, "b-1"))
	}
}

// TestResolveFrameDiscoveryError 覆盖帧面发现失败。
func TestResolveFrameDiscoveryError(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}}
	frames := &stubFrames{err: errors.New("注册中心不可用")}
	r, _ := New(dir, frames, Options{})
	requireUnavailable(t, resolveErr(r, "b-1"))
}

// TestNewValidatesDeps 覆盖依赖校验与选项缺省。
func TestNewValidatesDeps(t *testing.T) {
	if _, err := New(nil, &stubFrames{}, Options{}); err == nil {
		t.Fatal("目录为空应报错")
	}
	if _, err := New(&stubDirectory{}, nil, Options{}); err == nil {
		t.Fatal("帧面发现为空应报错")
	}
	r, err := New(&stubDirectory{owners: map[string]string{"battle:b-1": "node-1"}},
		&stubFrames{instances: []Instance{
			{ID: "f1", NodeID: "node-1", Host: "h", Ports: map[string]string{"ws": "1"}},
		}}, Options{ActorType: "battle", FrameService: "battle-frame"})
	if err != nil {
		t.Fatalf("带选项构造失败: %v", err)
	}
	if _, err := r.Resolve(context.Background(), resolveReq("b-1", "ws")); err != nil {
		t.Fatalf("解析失败: %v", err)
	}
}

// TestResolveEmptyBattleID 覆盖票据缺 battle_id（不该发生的输入也要拒绝而不是查空键）。
func TestResolveEmptyBattleID(t *testing.T) {
	r, _ := New(&stubDirectory{}, &stubFrames{}, Options{})
	requireUnavailable(t, resolveErr(r, ""))
}

// TestResolveInvalidBattleID 覆盖 battle_id 非法（PID 段非法 → 不查目录直接拒绝）。
func TestResolveInvalidBattleID(t *testing.T) {
	dir := &stubDirectory{owners: map[string]string{}}
	r, _ := New(dir, &stubFrames{}, Options{})
	requireUnavailable(t, resolveErr(r, "bad/id:with:colon"))
	if len(dir.calls) != 0 {
		t.Fatalf("非法 battle_id 不应查目录，实际 %v", dir.calls)
	}
}
