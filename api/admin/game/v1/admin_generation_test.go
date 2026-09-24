package admingamev1

import (
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// adminAllowedFiles 是管理面包允许出现的非测试文件：消息与标准 gRPC 两类生成物
// （--go_out / --go-grpc_out），加手写的契约与包注释。
var adminAllowedFiles = []string{"admin.proto", "doc.go", "admin.pb.go", "admin_grpc.pb.go"}

// adminBannedFiles 是管理面**绝不**允许出现的产物：actor / http / client / rpc 平面
// 与四传输桩——它们意味着管理面被误接进了客户端通道。
var adminBannedFiles = []string{
	"admin_http.pb.go", "admin_actor.pb.go", "admin_client.pb.go", "admin_rpc_adapter.pb.go",
	"admin_route.pb.go", "admin_tcp.pb.go", "admin_ws.pb.go", "admin_udp.pb.go",
	"admin_kcp.pb.go", "admin_nats.pb.go",
}

// TestGeneratedArtifactsAreAdminOnly 是管理面产物隔离的负面断言（P7 步骤 3）：
// api/admin/game/v1 只允许「消息 + 标准 gRPC」产物（外加手写 admin.proto/doc.go 与测试），
// 不得出现 actor / http / client / rpc 平面产物或 actor/rpc/opclient 子目录——
// 管理面是普通 gRPC 内网面，不经网关通道、不暴露给客户端。
func TestGeneratedArtifactsAreAdminOnly(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("读取包目录: %v", err)
	}
	allowed := make(map[string]bool, len(adminAllowedFiles))
	for _, name := range adminAllowedFiles {
		allowed[name] = true
	}
	present := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() {
			t.Errorf("管理面包不得出现子目录（平面产物隔离）: %s/", e.Name())
			continue
		}
		if strings.HasSuffix(e.Name(), "_test.go") {
			continue // 测试文件（含本断言）不是生成物
		}
		present[e.Name()] = true
		if !allowed[e.Name()] {
			t.Errorf("管理面包出现非预期文件 %s（只允许 %v）", e.Name(), adminAllowedFiles)
		}
	}
	for _, name := range adminAllowedFiles {
		if !present[name] {
			t.Errorf("管理面缺少预期文件 %s（make proto 的 API_ADMIN_PROTOS 未接线？）", name)
		}
	}
	for _, name := range adminBannedFiles {
		if present[name] {
			t.Errorf("管理面不得产出 %s（管理面是普通 gRPC 内网面，不暴露给客户端）", name)
		}
	}
}

// TestAuditEntryResultIsEnum 钉死「协议状态字段一律 enum」：AuditEntry.result 必须是生成的
// AuditResult 枚举（不是裸 string）——防回归（protojson 下发枚举名，客户端不解析魔法字符串）；
// 枚举化不新增产物文件，故 adminAllowedFiles 白名单保持不变。
func TestAuditEntryResultIsEnum(t *testing.T) {
	field := (&AuditEntry{}).ProtoReflect().Descriptor().Fields().ByName("result")
	if field == nil {
		t.Fatal("AuditEntry 缺 result 字段")
	}
	if field.Kind() != protoreflect.EnumKind || field.Enum().FullName() != "admin.game.v1.AuditResult" {
		t.Fatalf("AuditEntry.result 必须是 admin.game.v1.AuditResult 枚举，实际 kind=%v enum=%v",
			field.Kind(), field.Enum().FullName())
	}
	if got := field.Enum().Values().Len(); got != 5 {
		t.Fatalf("AuditResult 枚举值数 = %d，期望 5（UNSPECIFIED/PENDING/SUCCESS/FAILED/SKIPPED_DRY_RUN）", got)
	}
}
