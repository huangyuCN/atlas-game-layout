// store_test.go 是耐久性选源与落盘的 TDD 场景测试（对照设计 §9 场景表）：
// fake cache/persist 可注入，不绑真实 redis/mongo。
package repo

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
)

// errorsSnapshotUnavailable 模拟 Redis 连不上（非 key miss）。
var errorsSnapshotUnavailable = errors.New("fake: redis unavailable")

// fakeCache 是 redis 依赖的测试桩：存取走真实编解码（snapshotEncode/snapshotDecode
// round-trip 全程参与，字段剔除行为被所有场景隐式回归）；记录 noexpire 状态，可注入故障。
type fakeCache struct {
	mu        sync.Mutex
	encoded   map[string][]byte // 已编码的快照字节（Get 时按真实解码路径还原）
	persisted map[string]bool   // PERSIST 过的 key
	failGet   bool              // 注入：Get 模拟 Redis 连不上
	failSet   bool              // 注入：SetEncoded 模拟写失败
}

func newFakeCache() *fakeCache {
	return &fakeCache{encoded: map[string][]byte{}, persisted: map[string]bool{}}
}

func (c *fakeCache) Get(_ context.Context, playerID string) (*models.Player, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failGet {
		return nil, errorsSnapshotUnavailable
	}
	if b, ok := c.encoded[playerID]; ok {
		return snapshotDecode(b)
	}
	return nil, ErrPlayerNotFound
}

func (c *fakeCache) SetEncoded(_ context.Context, playerID string, b []byte, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failSet {
		return errorsSnapshotUnavailable
	}
	c.encoded[playerID] = b
	return nil
}

func (c *fakeCache) Set(_ context.Context, p *models.Player, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failSet {
		return errorsSnapshotUnavailable
	}
	b, _, err := snapshotEncode(p)
	if err != nil {
		return err
	}
	c.encoded[p.PlayerID] = b
	return nil
}

func (c *fakeCache) Persist(_ context.Context, playerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.persisted[playerID] = true
	return nil
}

func (c *fakeCache) TTLOf(_ context.Context, playerID string) (time.Duration, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.encoded[playerID]; !ok {
		return 0, ErrPlayerNotFound
	}
	if c.persisted[playerID] {
		return -1, nil
	}
	return 72 * time.Hour, nil
}

func (c *fakeCache) Expire(_ context.Context, playerID string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.persisted, playerID)
	return nil
}

// fakeMongo 是 mongo 权威库的测试桩（可注入读/写故障）。
type fakeMongo struct {
	mu         sync.Mutex
	players    map[string]*models.Player
	failGet    bool // 注入：FindByID 模拟 mongo 读失败（非 key miss）
	failUpsert bool // 注入：Upsert 模拟写失败
}

func newFakeMongo() *fakeMongo { return &fakeMongo{players: map[string]*models.Player{}} }

func (m *fakeMongo) FindByID(_ context.Context, playerID string) (*models.Player, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failGet {
		return nil, errorsSnapshotUnavailable
	}
	if p, ok := m.players[playerID]; ok {
		cp := *p
		return &cp, nil
	}
	return nil, ErrPlayerNotFound
}

func (m *fakeMongo) Upsert(_ context.Context, p *models.Player) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failUpsert {
		return errorsSnapshotUnavailable
	}
	m.players[p.PlayerID] = p
	return nil
}

func (m *fakeMongo) FindByAccount(_ context.Context, _ string) (*models.Player, error) {
	return nil, ErrPlayerNotFound
}

func (m *fakeMongo) Create(_ context.Context, _ *models.Player) error { return nil }

// ---- 设计 §9 场景对照（fake 注入） ----

// newStore 构造测试仓储组合。
func newStore(c *fakeCache, m *fakeMongo) *PlayerStore { return NewPlayerStore(c, m) }

// testPlayer 构造一个可断言的聚合根。
func testPlayer(id string, level int32, version uint64) *models.Player {
	return &models.Player{PlayerID: id, Nickname: "n-" + id, Level: level, PersistVersion: version}
}

// TestFlushRedisOKSkipsMongo 场景 1：
func TestFlushRedisOKSkipsMongo(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	s := newStore(c, m)
	p := testPlayer("p1", 5, 0)
	result, _, err := s.FlushPlayer(context.Background(), p, 0)
	if err != nil || result != FlushRedisOK {
		t.Fatalf("FlushPlayer: result=%v err=%v", result, err)
	}
	if len(m.players) != 0 {
		t.Fatal("Redis 成功时不应写 Mongo")
	}
}

// TestFlushDegradesToMongo 场景 2：
func TestFlushDegradesToMongo(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	c.failSet = true
	s := newStore(c, m)
	p := testPlayer("p1", 5, 0)
	result, _, err := s.FlushPlayer(context.Background(), p, 0)
	if err != nil || result != FlushMongoOK {
		t.Fatalf("FlushPlayer: result=%v err=%v", result, err)
	}
	if _, ok := m.players["p1"]; !ok {
		t.Fatal("Mongo 应收到降级写入")
	}
}

// TestFlushSkipsUnchanged 场景 8：
func TestFlushSkipsUnchanged(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	s := newStore(c, m)
	p := testPlayer("p1", 5, 0)
	_, firstCRC, err := s.FlushPlayer(context.Background(), p, 0)
	if err != nil {
		t.Fatal(err)
	}
	result, _, err := s.FlushPlayer(context.Background(), p, firstCRC)
	if err != nil || result != FlushSkipped {
		t.Fatalf("未变更应跳过: result=%v err=%v", result, err)
	}
}

// TestLoadSameVersionPrefersRedis 场景 6：
func TestLoadSameVersionPrefersRedis(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	s := newStore(c, m)
	_ = s.SaveSnapshot(context.Background(), testPlayer("p1", 5, 0), 72*1000)
	mp := testPlayer("p1", 9, 0) // Mongo 同版本但 Level 不同：相同用 Redis
	_ = m.Upsert(context.Background(), mp)
	got, err := s.LoadPlayer(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Level != 5 {
		t.Fatalf("相同版本应用 Redis: got level=%d", got.Level)
	}
}

// TestLoadNewerMongoOverwritesRedis 变体：Mongo 版本更高 → 用 Mongo 并覆盖 Redis（禁止回档）。
func TestLoadNewerMongoOverwritesRedis(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	s := newStore(c, m)
	_ = s.SaveSnapshot(context.Background(), testPlayer("p1", 5, 1), 72*1000)
	_ = m.Upsert(context.Background(), testPlayer("p1", 9, 2)) // Mongo 更新
	got, err := s.LoadPlayer(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Level != 9 {
		t.Fatalf("Mongo 版本更高应用 Mongo: got level=%d", got.Level)
	}
	if snap, _ := c.Get(context.Background(), "p1"); snap.PersistVersion != 2 {
		t.Fatalf("Redis 应被 Mongo 覆盖: version=%d", snap.PersistVersion)
	}
}

// TestLoadFailsWhenRedisUnavailable 场景 7：
func TestLoadFailsWhenRedisUnavailable(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	_ = m.Upsert(context.Background(), testPlayer("p1", 5, 0))
	c.failGet = true
	s := newStore(c, m)
	if _, err := s.LoadPlayer(context.Background(), "p1"); err == nil {
		t.Fatal("Redis 连不上应拒绝登录")
	}
}

// TestPersistThenExpireOnLogin 场景 5：
func TestPersistThenExpireOnLogin(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	s := newStore(c, m)
	// 模拟下线 Mongo 失败：写 Redis 后转 PERSIST。
	p := testPlayer("p1", 5, 0)
	_ = s.SaveSnapshot(context.Background(), p, 72*1000)
	_ = s.PersistSnapshot(context.Background(), "p1")
	if d, _ := s.TTLOfSnapshot(context.Background(), "p1"); d != -1 {
		t.Fatalf("PERSIST 后 TTL 应为 -1, got %d", d)
	}
	// Mongo 恢复：登录补写成功 → EXPIRE 72h。
	if err := s.SavePlayer(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	_ = s.ExpireSnapshot(context.Background(), "p1", 72*1000)
	if d, _ := s.TTLOfSnapshot(context.Background(), "p1"); d == -1 {
		t.Fatal("补写后 TTL 不应保持 -1")
	}
}

// TestLoadLegacyZeroVersion 场景 9：
func TestLoadLegacyZeroVersion(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	s := newStore(c, m)
	// mongo 老档 version=0、redis 无档：直接用 mongo。
	_ = m.Upsert(context.Background(), testPlayer("p1", 5, 0))
	got, err := s.LoadPlayer(context.Background(), "p1")
	if err != nil || got.PersistVersion != 0 {
		t.Fatalf("老档应视为 0: got=%+v err=%v", got, err)
	}
}

// TestLoadFailsWhenMongoReadFails 数据安全决策回归：Redis 有档但 Mongo 读失败
// （非 key miss）→ 拒绝登录（Redis 档可能是低版本残留，保守拒绝）。
func TestLoadFailsWhenMongoReadFails(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	m.failGet = true
	s := newStore(c, m)
	_ = s.SaveSnapshot(context.Background(), testPlayer("p1", 5, 1), 72*1000)
	if _, err := s.LoadPlayer(context.Background(), "p1"); err == nil {
		t.Fatal("Mongo 读失败应拒绝登录（Redis 档可能是低版本残留）")
	}
}

// TestFlushBothFailedRollsBackVersion 版本一致性：双库失败 → persistVersion
// 回退（不产生「内存版本已涨但两库都没有」的悬空版本）。
func TestFlushBothFailedRollsBackVersion(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	c.failSet = true
	m.failUpsert = true
	s := newStore(c, m)
	p := testPlayer("p1", 5, 0)
	result, _, err := s.FlushPlayer(context.Background(), p, 0)
	if err == nil || result != FlushBothFailed {
		t.Fatalf("双失败应返回 FlushBothFailed: result=%v err=%v", result, err)
	}
	if p.PersistVersion != 0 {
		t.Fatalf("双失败后版本应回退: got=%d", p.PersistVersion)
	}
}

// TestFlushVersionSingleIncrement 版本语义：一次判脏落盘只自增一次
// （Redis/Mongo 共用同一版本，登录按它选源）。
func TestFlushVersionSingleIncrement(t *testing.T) {
	c, m := newFakeCache(), newFakeMongo()
	s := newStore(c, m)
	p := testPlayer("p1", 5, 0)
	if _, _, err := s.FlushPlayer(context.Background(), p, 0); err != nil {
		t.Fatal(err)
	}
	if p.PersistVersion != 1 {
		t.Fatalf("一次落盘版本应自增一次: got=%d", p.PersistVersion)
	}
}

// TestSnapshotRoundTripStripsAuth 快照编解码 round-trip：认证字段被剔除、
// 数据字段保真、格式版本头可校验（P0 回归：认证字段只存 mongo）。
func TestSnapshotRoundTripStripsAuth(t *testing.T) {
	in := &models.Player{
		PlayerID: "p1", Account: "acc", Salt: "s", Password: "h",
		Nickname: "n", Level: 7, CreatedAt: 123,
		Items: []*models.Item{{ItemID: 1001, Count: 3}},
	}
	b, _, err := snapshotEncode(in)
	if err != nil {
		t.Fatal(err)
	}
	if b[0] != snapFormatVer {
		t.Fatalf("格式版本头不符: got=%d", b[0])
	}
	out, err := snapshotDecode(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Salt != "" || out.Password != "" {
		t.Fatalf("认证字段不得进快照: salt=%q password=%q", out.Salt, out.Password)
	}
	if out.Nickname != "n" || out.Level != 7 || out.CreatedAt != 123 ||
		len(out.Items) != 1 || out.Items[0].ItemID != 1001 || out.Items[0].Count != 3 {
		t.Fatalf("数据字段应保真: %+v", out)
	}
}

// TestUpsertSetDocExcludesCredentials mongo 更新写 $set 文档：认证字段与 _id
// 被剔除（凭据只由 Create 写入，防止无认证字段的内存副本覆盖 mongo 凭据为空）。
func TestUpsertSetDocExcludesCredentials(t *testing.T) {
	p := &models.Player{
		PlayerID: "p1", Account: "acc", Salt: "s", Password: "h",
		Nickname: "n", Level: 7,
	}
	set, full, err := upsertSetDoc(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"_id", "salt", "password"} {
		if _, ok := set[k]; ok {
			t.Fatalf("$set 文档不得包含 %q", k)
		}
	}
	if set["nickname"] != "n" || set["level"] != int32(7) {
		t.Fatalf("$set 应携带数据字段: %v", set)
	}
	if len(full) == 0 {
		t.Fatal("全量 BSON 应返回（文档大小预警复用）")
	}
}

// clonePlayer 拷贝聚合根（测试隔离用）。
func clonePlayer(p *models.Player) *models.Player {
	cp := *p
	cp.Items = make([]*models.Item, len(p.Items))
	for i, it := range p.Items {
		cpi := *it
		cp.Items[i] = &cpi
	}
	return &cp
}
