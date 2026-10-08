package stream

import (
	"testing"
	"time"
)

// TestEndedBookRecordAndEnded 验证留档后判定为已结束，且不影响其它对局。
func TestEndedBookRecordAndEnded(t *testing.T) {
	book := NewEndedBook(time.Minute)
	if book.Ended("b-1") {
		t.Fatal("未留档的对局被判为已结束")
	}
	book.Record("b-1", "p-a", []string{"p-a", "p-b"})
	if !book.Ended("b-1") {
		t.Fatal("留档后未判为已结束")
	}
	if book.Ended("b-2") {
		t.Fatal("其它对局被误判为已结束")
	}
	if winner, ok := book.Winner("b-1"); !ok || winner != "p-a" {
		t.Fatalf("留档胜者 = %q（ok=%v），期望 p-a", winner, ok)
	}
	if winner, ok := book.Winner("b-2"); ok || winner != "" {
		t.Fatalf("未留档对局返回了胜者 %q", winner)
	}
}

// TestEndedBookRecordIdempotent 验证同局重复留档以首次为准（结算路径可重入）。
func TestEndedBookRecordIdempotent(t *testing.T) {
	book := NewEndedBook(time.Minute)
	book.Record("b-1", "p-a", []string{"p-a", "p-b"})
	book.Record("b-1", "p-b", []string{"p-b"})

	if winner, _ := book.Winner("b-1"); winner != "p-a" {
		t.Fatalf("重复留档覆盖了首次结果: %q", winner)
	}
	if _, ok := book.Claim("b-1", "p-a"); !ok {
		t.Fatal("重复留档后首次名单丢失（p-a 拿不到补投额度）")
	}
	if book.Len() != 1 {
		t.Fatalf("重复留档产生了 %d 条留档，期望 1", book.Len())
	}
}

// TestEndedBookExpires 验证 TTL 到期后墓碑消失（新对局可复用同一 battle_id）。
func TestEndedBookExpires(t *testing.T) {
	now := time.Now()
	book := NewEndedBook(time.Minute)
	book.now = func() time.Time { return now }
	book.Record("b-1", "p-a", []string{"p-a"})
	if !book.Ended("b-1") {
		t.Fatal("留档期内未判为已结束")
	}

	book.now = func() time.Time { return now.Add(time.Minute + time.Second) }
	if book.Ended("b-1") {
		t.Fatal("留档过期后仍判为已结束（墓碑必须随 TTL 消失）")
	}
	if _, ok := book.Winner("b-1"); ok {
		t.Fatal("留档过期后仍返回胜者")
	}
	if _, ok := book.Claim("b-1", "p-a"); ok {
		t.Fatal("留档过期后仍发放补投额度")
	}
	if book.Len() != 0 {
		t.Fatalf("过期留档未清除: %d 条", book.Len())
	}
}

// TestEndedBookClaimBounded 验证补投额度按玩家独立有界，且名单外玩家不发放。
func TestEndedBookClaimBounded(t *testing.T) {
	book := NewEndedBook(time.Minute)
	book.Record("b-1", "p-a", []string{"p-a", "p-b"})

	if _, ok := book.Claim("b-1", "p-x"); ok {
		t.Fatal("名单外玩家获得了补投额度")
	}
	if _, ok := book.Claim("b-2", "p-a"); ok {
		t.Fatal("未留档对局发放了补投额度")
	}
	for i := 0; i < MaxEndReplays; i++ {
		if _, ok := book.Claim("b-1", "p-b"); !ok {
			t.Fatalf("第 %d 次补投被拒（上限 %d 之前都应放行）", i+1, MaxEndReplays)
		}
	}
	if _, ok := book.Claim("b-1", "p-b"); ok {
		t.Fatalf("补投超出上限 %d 仍放行（迟到 op 风暴会无限重发）", MaxEndReplays)
	}
	if _, ok := book.Claim("b-1", "p-a"); !ok {
		t.Fatal("补投上限按对局而非按玩家计量（一个玩家用尽即全体失联）")
	}
}

// TestEndedBookSweepsOnRecord 验证写入路径顺手清理过期留档（表不随对局数无限增长）。
func TestEndedBookSweepsOnRecord(t *testing.T) {
	now := time.Now()
	book := NewEndedBook(time.Minute)
	book.now = func() time.Time { return now }
	for _, id := range []string{"b-1", "b-2", "b-3"} {
		book.Record(id, "p-a", []string{"p-a"})
	}
	book.now = func() time.Time { return now.Add(2 * time.Minute) }
	book.Record("b-4", "p-a", []string{"p-a"})

	if book.Len() != 1 {
		t.Fatalf("写入路径未清理过期留档: %d 条", book.Len())
	}
	if !book.Ended("b-4") {
		t.Fatal("新留档被误清")
	}
}

// TestEndedBookZeroTTLFallsBack 验证非正 TTL 回落默认值（配置缺失不产生「立即过期」的墓碑）。
func TestEndedBookZeroTTLFallsBack(t *testing.T) {
	book := NewEndedBook(0)
	book.Record("b-1", "p-a", []string{"p-a"})
	if !book.Ended("b-1") {
		t.Fatal("TTL 为 0 时留档立即失效（应回落 DefaultEndedTTL）")
	}
	if DefaultEndedTTL <= 0 {
		t.Fatal("DefaultEndedTTL 必须为正")
	}
}
