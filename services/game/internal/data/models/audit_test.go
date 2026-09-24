package models

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// TestAuditResultEnum 校验审计结果状态是有限枚举（杜绝魔法字符串）：
// 取值集合、Valid 判定与 Terminal 判定三者一致（PENDING 之外均为终态）。
func TestAuditResultEnum(t *testing.T) {
	cases := []struct {
		result   AuditResult
		valid    bool
		terminal bool
	}{
		{AuditPending, true, false},
		{AuditSuccess, true, true},
		{AuditFailed, true, true},
		{AuditSkippedDryRun, true, true},
		{AuditResult(""), false, false},
		{AuditResult("success"), false, false}, // 大小写敏感：落库值即对外投影值
		{AuditResult("UNKNOWN"), false, false},
	}
	for _, tc := range cases {
		if got := tc.result.Valid(); got != tc.valid {
			t.Errorf("AuditResult(%q).Valid() = %v, 期望 %v", tc.result, got, tc.valid)
		}
		if got := tc.result.Terminal(); got != tc.terminal {
			t.Errorf("AuditResult(%q).Terminal() = %v, 期望 %v", tc.result, got, tc.terminal)
		}
	}
}

// TestAuditTargetTypeEnum 校验审计目标类型枚举：取值合法、未定义值非法。
func TestAuditTargetTypeEnum(t *testing.T) {
	if !AuditTargetPlayer.Valid() || !AuditTargetItem.Valid() {
		t.Fatal("已定义的目标类型应 Valid() = true")
	}
	if AuditTargetType("").Valid() || AuditTargetType("guild").Valid() {
		t.Fatal("未定义的目标类型应 Valid() = false")
	}
}

// TestAuditRecordBSONKeys 校验审计记录的 bson 键与索引/查询约定一致：
// 幂等键字段必须 omitempty——dry-run 记录不写该字段，稀疏唯一索引才不会被空值互相冲突。
func TestAuditRecordBSONKeys(t *testing.T) {
	dryRun := &AuditRecord{AuditID: "a-1", Operator: "gm", DryRun: true, Result: AuditSkippedDryRun}
	if _, ok := encodeDoc(t, dryRun)["idempotency_key"]; ok {
		t.Fatal("dry-run 记录（无幂等键）不得写入 idempotency_key 字段")
	}
	keyed := &AuditRecord{AuditID: "a-2", Operator: "gm", IdempotencyKey: "k-1", Result: AuditPending}
	doc := encodeDoc(t, keyed)
	if got, ok := doc["idempotency_key"]; !ok || got != "k-1" {
		t.Fatalf("带键记录的 idempotency_key = %v(存在=%v), 期望 k-1", got, ok)
	}
	if got := doc["result"]; got != "PENDING" {
		t.Fatalf("result = %v, 期望 PENDING", got)
	}
}

// encodeDoc 把审计记录编码为 bson 文档，供键存在性断言。
func encodeDoc(t *testing.T, rec *AuditRecord) bson.M {
	t.Helper()
	raw, err := bson.Marshal(rec)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("bson.Unmarshal: %v", err)
	}
	return doc
}
