package session

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
)

// testKeys 是单测用的键构造器：与 newTestEnv 里 redis 客户端一致（未指定命名空间 → default）。
var testKeys = pkredis.NewKeys("")

// newTestManager 起 miniredis 并构造 Manager（租期与清扫周期全默认）。
func newTestManager(t *testing.T, instanceID string) *Manager {
	m, _ := newTestEnv(t, instanceID, Options{})
	return m
}

// newTestManagerWithTTL 同上，但指定会话 TTL（过期清扫用例）。
func newTestManagerWithTTL(t *testing.T, instanceID string, ttl time.Duration) *Manager {
	m, _ := newTestEnv(t, instanceID, Options{TTL: ttl})
	return m
}

// newTestEnv 起 miniredis 并构造 Manager（一并返回 miniredis，供断言路由键剩余 TTL）。
func newTestEnv(t *testing.T, instanceID string, opts Options) (*Manager, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	cli, err := pkredis.NewClient(pkredis.Options{Addrs: []string{mr.Addr()}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return NewManager(NewRedisStore(cli), instanceID, opts), mr
}

// TestOptionsResolve 验证租期/清扫周期的默认回落与下限保护。
func TestOptionsResolve(t *testing.T) {
	tests := []struct {
		name      string
		opts      Options
		wantTTL   time.Duration
		wantSweep time.Duration
	}{
		{"零值取默认", Options{}, DefaultTTL, DefaultTTL / 2},
		{"只配租期则清扫取一半", Options{TTL: 90 * time.Second}, 90 * time.Second, 45 * time.Second},
		{"两者都配原样生效", Options{TTL: 90 * time.Second, SweepInterval: 5 * time.Second}, 90 * time.Second, 5 * time.Second},
		{"负租期回落默认", Options{TTL: -time.Second}, DefaultTTL, DefaultTTL / 2},
		{"负清扫周期按租期一半", Options{TTL: time.Minute, SweepInterval: -time.Second}, time.Minute, 30 * time.Second},
		{"极小租期清扫不低于 1ms", Options{TTL: time.Microsecond}, time.Microsecond, time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ttl, sweep := tt.opts.resolve()
			if ttl != tt.wantTTL || sweep != tt.wantSweep {
				t.Fatalf("resolve() = (%v, %v), want (%v, %v)", ttl, sweep, tt.wantTTL, tt.wantSweep)
			}
		})
	}
}

// fakeConn 是会话视角的测试连接。
type fakeConn struct {
	id   uint64
	kind string
	sent []pushMsg
}

type pushMsg struct {
	operation string
	payload   []byte
}

func (c *fakeConn) send(operation string, payload []byte) error {
	c.sent = append(c.sent, pushMsg{operation: operation, payload: payload})
	return nil
}

func (c *fakeConn) conn() *Conn {
	return &Conn{ID: c.id, Kind: c.kind, Send: c.send}
}

// connWithRef 以身份 Ref 构造测试连接（反向索引测试用）。
func (c *fakeConn) connWithRef(ref string) *Conn {
	return &Conn{ID: c.id, Kind: c.kind, Ref: ref, Send: c.send}
}

// TestPlayerByRef 验证连接反向索引：绑定登记、换绑注销、解绑移除。
func TestPlayerByRef(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	c1 := (&fakeConn{id: 1, kind: "ws"}).connWithRef("conn:1")
	if _, err := m.Bind(ctx, "p-1", c1, ChannelBiz, "t1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if pid, ok := m.PlayerByRef("conn:1"); !ok || pid != "p-1" {
		t.Fatalf("PlayerByRef = %q,%v, want p-1,true", pid, ok)
	}
	if _, ok := m.PlayerByRef("conn:2"); ok {
		t.Fatal("未绑定连接不应命中")
	}
	// 换绑新连接后旧连接身份注销。
	c2 := (&fakeConn{id: 2, kind: "ws"}).connWithRef("conn:2")
	if _, err := m.Bind(ctx, "p-1", c2, ChannelBiz, "t1"); err != nil {
		t.Fatalf("换绑: %v", err)
	}
	if _, ok := m.PlayerByRef("conn:1"); ok {
		t.Fatal("被替换的旧连接身份应注销")
	}
	if pid, ok := m.PlayerByRef("conn:2"); !ok || pid != "p-1" {
		t.Fatalf("新连接身份未登记: %q,%v", pid, ok)
	}
	// 解绑后身份移除。
	m.Unbind(ctx, "p-1", 2)
	if _, ok := m.PlayerByRef("conn:2"); ok {
		t.Fatal("解绑后连接身份应移除")
	}
}

// TestBindBizWritesRoute 验证登录绑定业务通道后路由表内容完整。
func TestBindBizWritesRoute(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	c := (&fakeConn{id: 1, kind: "tcp"}).conn()

	old, err := m.Bind(ctx, "p-1", c, ChannelBiz, "token-1")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if old != nil {
		t.Fatalf("首次绑定旧路由 = %+v, want nil", old)
	}
	r, err := m.Route(ctx, "p-1")
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if r == nil {
		t.Fatal("路由不存在")
	}
	if r.InstanceID != "gw-1" || r.Token != "token-1" || r.BizConnID != 1 || r.BizKind != "tcp" {
		t.Fatalf("路由字段不符: %+v", r)
	}
	if r.BattleConnID != 0 || r.BattleKind != "" {
		t.Fatalf("战斗通道应为空: %+v", r)
	}
}

// TestBindBattleMergesChannels 验证战斗通道绑定与业务通道合并到同一路由。
func TestBindBattleMergesChannels(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	biz := (&fakeConn{id: 1, kind: "ws"}).conn()
	if _, err := m.Bind(ctx, "p-1", biz, ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind biz: %v", err)
	}
	bat := (&fakeConn{id: 2, kind: "kcp"}).conn()
	if _, err := m.Bind(ctx, "p-1", bat, ChannelBattle, "token-1"); err != nil {
		t.Fatalf("Bind battle: %v", err)
	}
	r, err := m.Route(ctx, "p-1")
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if r.BizConnID != 1 || r.BizKind != "ws" || r.BattleConnID != 2 || r.BattleKind != "kcp" {
		t.Fatalf("通道合并失败: %+v", r)
	}
	if r.Token != "token-1" {
		t.Fatalf("战斗绑定不应覆盖 token: %+v", r)
	}
}

// TestBindReturnsOldRoute 验证重复绑定返回旧路由（挤下线判断依据）。
func TestBindReturnsOldRoute(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	c1 := (&fakeConn{id: 1, kind: "tcp"}).conn()
	if _, err := m.Bind(ctx, "p-1", c1, ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind1: %v", err)
	}
	c2 := (&fakeConn{id: 2, kind: "tcp"}).conn()
	old, err := m.Bind(ctx, "p-1", c2, ChannelBiz, "token-2")
	if err != nil {
		t.Fatalf("Bind2: %v", err)
	}
	if old == nil || old.Token != "token-1" || old.BizConnID != 1 {
		t.Fatalf("旧路由不符: %+v", old)
	}
}

// TestHeartbeatRefreshesTTL 验证心跳续租且令牌校验。
func TestHeartbeatRefreshesTTL(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if !m.Heartbeat(ctx, "p-1", "token-1") {
		t.Fatal("Heartbeat 应成功")
	}
	if m.Heartbeat(ctx, "p-1", "wrong-token") {
		t.Fatal("错误令牌心跳应失败")
	}
}

// TestHeartbeatAppliesConfiguredTTL 验证 Bind 与 Heartbeat 两跳都用配置租期（而非内置默认 30s）：
// 先把路由 TTL 压到 5s，只有心跳真的按配置租期续租，才能回到 ≈90s。
func TestHeartbeatAppliesConfiguredTTL(t *testing.T) {
	ctx := context.Background()
	m, mr := newTestEnv(t, "gw-1", Options{TTL: 90 * time.Second})
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if ttl := mr.TTL(testKeys.GatewayRoute("p-1")); ttl <= 80*time.Second || ttl > 90*time.Second {
		t.Fatalf("Bind 后路由 TTL = %v, 期望 ≈90s（配置租期）", ttl)
	}
	if err := m.store.Expire(ctx, "p-1", 5*time.Second); err != nil {
		t.Fatalf("压短路由 TTL: %v", err)
	}
	if !m.Heartbeat(ctx, "p-1", "token-1") {
		t.Fatal("Heartbeat 应成功")
	}
	if ttl := mr.TTL(testKeys.GatewayRoute("p-1")); ttl <= 80*time.Second || ttl > 90*time.Second {
		t.Fatalf("心跳后路由 TTL = %v, 期望 ≈90s（心跳按配置租期续租）", ttl)
	}
}

// TestValidateToken 验证令牌校验的三种状态。
func TestValidateToken(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	ok, err := m.Validate(ctx, "p-1", "token-1")
	if err != nil || !ok {
		t.Fatalf("Validate 匹配令牌 = (%v, %v), want (true, nil)", ok, err)
	}
	ok, err = m.Validate(ctx, "p-1", "wrong")
	if err != nil || ok {
		t.Fatalf("Validate 错误令牌 = (%v, %v), want (false, nil)", ok, err)
	}
	ok, err = m.Validate(ctx, "p-none", "token-1")
	if err != nil || ok {
		t.Fatalf("Validate 无路由 = (%v, %v), want (false, nil)", ok, err)
	}
}

// TestUnbind 验证登出/挤下线清理本地与路由表。
func TestUnbind(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	m.Unbind(ctx, "p-1", 1)
	r, err := m.Route(ctx, "p-1")
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if r != nil {
		t.Fatalf("Unbind 后路由仍存在: %+v", r)
	}
	if _, ok := m.LocalSession("p-1"); ok {
		t.Fatal("Unbind 后本地会话仍存在")
	}
}

// TestUnbindSkipsStaleConn 验证解绑只对当前持有连接生效（挤下线竞态防护）。
func TestUnbindSkipsStaleConn(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind1: %v", err)
	}
	// 新连接挤掉旧连接。
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 2, kind: "tcp"}).conn(), ChannelBiz, "token-2"); err != nil {
		t.Fatalf("Bind2: %v", err)
	}
	// 旧连接的解绑不应影响新会话。
	m.Unbind(ctx, "p-1", 1)
	if _, ok := m.LocalSession("p-1"); !ok {
		t.Fatal("旧 connID 解绑误删新会话")
	}
}

// TestPushRawFallback 验证推送通道选择：battle 优先，缺省回退 biz。
func TestPushRawFallback(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	biz := &fakeConn{id: 1, kind: "ws"}
	if _, err := m.Bind(ctx, "p-1", biz.conn(), ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind biz: %v", err)
	}
	if err := m.PushRaw("p-1", "/push/A", []byte("x")); err != nil {
		t.Fatalf("回退推送失败: %v", err)
	}
	if len(biz.sent) != 1 {
		t.Fatalf("biz 连接应收到 1 条: %d", len(biz.sent))
	}

	bat := &fakeConn{id: 2, kind: "kcp"}
	if _, err := m.Bind(ctx, "p-1", bat.conn(), ChannelBattle, "token-1"); err != nil {
		t.Fatalf("Bind battle: %v", err)
	}
	if err := m.PushRaw("p-1", "/push/B", []byte("y")); err != nil {
		t.Fatalf("优先推送失败: %v", err)
	}
	if len(bat.sent) != 1 {
		t.Fatalf("battle 连接应收到 1 条: %d", len(bat.sent))
	}
	if len(biz.sent) != 1 {
		t.Fatalf("biz 连接不应再收到: %d", len(biz.sent))
	}

	if err := m.PushRaw("p-none", "/push/C", nil); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("无会话推送 = %v, want ErrSessionNotFound", err)
	}
}

// TestSweepExpiredSessions 验证心跳过期清扫：本地与 redis 同步清理。
func TestSweepExpiredSessions(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	// 伪造心跳过期。
	m.mu.Lock()
	sess := m.local["p-1"]
	sess.LastHeartbeat = time.Now().Add(-time.Hour)
	m.mu.Unlock()

	if n := m.SweepOnce(ctx); n != 1 {
		t.Fatalf("SweepOnce = %d, want 1", n)
	}
	if _, ok := m.LocalSession("p-1"); ok {
		t.Fatal("过期会话未清理")
	}
	r, err := m.Route(ctx, "p-1")
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if r != nil {
		t.Fatalf("过期会话路由未删除: %+v", r)
	}
}

// TestSweepSkipsFreshSessions 验证未过期会话不被清扫。
func TestSweepSkipsFreshSessions(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, "gw-1")
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if n := m.SweepOnce(ctx); n != 0 {
		t.Fatalf("SweepOnce = %d, want 0", n)
	}
	if _, ok := m.LocalSession("p-1"); !ok {
		t.Fatal("未过期会话被误删")
	}
}

// TestStartHonorsSweepInterval 验证清扫循环用的是配置周期而非 ttl/2 推导值：
// ttl=2s（推导清扫 1s）配 sweep_interval=10ms 时，过期会话必须在 400ms 内被清掉。
func TestStartHonorsSweepInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, _ := newTestEnv(t, "gw-1", Options{TTL: 2 * time.Second, SweepInterval: 10 * time.Millisecond})
	if _, err := m.Bind(ctx, "p-1", (&fakeConn{id: 1, kind: "tcp"}).conn(), ChannelBiz, "token-1"); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	// 直接把本地心跳拨到过期（免等 2s）：清扫只以本地 LastHeartbeat 判定。
	m.mu.Lock()
	m.local["p-1"].LastHeartbeat = time.Now().Add(-time.Minute)
	m.mu.Unlock()

	m.Start(ctx)
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if m.Count() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("清扫未在 400ms 内执行（Count=%d）：配置的 sweep_interval 未生效", m.Count())
}

// TestRouteLegacyValueCompatibility 验证 M2 旧版纯实例 ID 字符串可解析。
func TestRouteLegacyValueCompatibility(t *testing.T) {
	mr := miniredis.RunT(t)
	cli, err := pkredis.NewClient(pkredis.Options{Addrs: []string{mr.Addr()}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	_ = cli.Raw().Set(context.Background(), testKeys.GatewayRoute("p-old"), "gw-legacy", 0).Err()

	store := NewRedisStore(cli)
	r, err := store.Get(context.Background(), "p-old")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if r == nil || r.InstanceID != "gw-legacy" {
		t.Fatalf("旧值解析失败: %+v", r)
	}
	// 纯乱码旧值不应报错。
	_ = cli.Raw().Set(context.Background(), testKeys.GatewayRoute("p-bad"), "!!!not-json", 0).Err()
	if _, err := store.Get(context.Background(), "p-bad"); err != nil {
		t.Fatalf("乱码旧值应容错: %v", err)
	}
}

// TestRouteJSONRoundtrip 验证路由 JSON 序列化往返。
func TestRouteJSONRoundtrip(t *testing.T) {
	r := &Route{
		InstanceID:   "gw-2",
		Token:        "token-x",
		BizConnID:    3,
		BizKind:      "tcp",
		BattleConnID: 4,
		BattleKind:   "kcp",
		UpdatedAt:    123,
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out Route
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != *r {
		t.Fatalf("往返不一致: %+v != %+v", out, *r)
	}
}
