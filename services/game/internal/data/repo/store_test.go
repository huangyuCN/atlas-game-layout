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

// fakeCache 是 redis 依赖的测试桩（记录 noexpire 状态，可注入故障）。
type fakeCache struct {
	mu        sync.Mutex
	snapshots map[string]*models.Player
	persisted map[string]bool // PERSIST 过的 key
	failGet   bool            // 注入：Get 模拟 Redis 连不上
	failSet   bool            // 注入：SetEncoded 模拟写失败
}

func newFakeCache() *fakeCache {
	return &fakeCache{snapshots: map[string]*models.Player{}, persisted: map[string]bool{}}
}

func (c *fakeCache) Get(_ context.Context, playerID string) (*models.Player, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failGet {
		return nil, errorsSnapshotUnavailable
	}
	if p, ok := c.snapshots[playerID]; ok {
		return clonePlayer(p), nil
	}
	return nil, ErrPlayerNotFound
}

func (c *fakeCache) SetEncoded(_ context.Context, playerID string, _ []byte, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failSet {
		return errorsSnapshotUnavailable
	}
	c.snapshots[playerID] = &models.Player{PlayerID: playerID}
	return nil
}

func (c *fakeCache) Set(_ context.Context, p *models.Player, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshots[p.PlayerID] = clonePlayer(p)
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
	if _, ok := c.snapshots[playerID]; !ok {
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

// fakeMongo 是 mongo 权威库的测试桩。
type fakeMongo struct {
	mu      sync.Mutex
	players map[string]*models.Player
}

func newFakeMongo() *fakeMongo { return &fakeMongo{players: map[string]*models.Player{}} }

func (m *fakeMongo) FindByID(_ context.Context, playerID string) (*models.Player, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.players[playerID]; ok {
		cp := *p
		return &cp, nil
	}
	return nil, ErrPlayerNotFound
}

func (m *fakeMongo) Upsert(_ context.Context, p *models.Player) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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
