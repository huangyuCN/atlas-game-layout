#!/usr/bin/env bash
# gen-protocol：三端「游戏协议包」的唯一生成入口（幂等：连跑两次产物零 diff）。
#
# 生成范围（api/ 下三个游戏域，共 7 个 proto）：
#   api/battle/v1/{battle,battle_service,migration}.proto   package battle.v1
#   api/game/v1/{player,player_service}.proto               package game.v1
#   api/matcher/v1/{matcher,match_events}.proto             package matcher.v1
#
# 契约域依赖：三个游戏域的消息直接引用契约域的字段类型（SessionMeta / LockstepInput /
# SnapshotMeta 来自 atlas.game.lockstep，PlayerSummary 来自 common.v1），而 route.proto
# 提供 service 上的 atlas.route.v1 注解。协议包要能独立发布（Go 不引框架仓模块、TS/C#
# 不引 SDK），就必须自持这些类型的生成物，故一并生成
# api/{atlas/v1/route,lockstep/lockstep,common/v1/common}.proto。
# 这三个 proto **只是协议包的依赖副本**，权威定义仍在框架仓 / 本仓 api/ 原处，不在此维护。
#
# 三端产物（全部入库；CI 有「重生成无 diff」门禁，见 .github/workflows/ci.yml）：
#   protocol/go/api/**/*.pb.go            独立 module（.../atlas-game-layout/protocol），只 require protobuf 运行时
#   protocol/ts/src/api/**/*_pb.ts        零运行时 DTO + op 常量（protoc-gen-atlas-client lang=ts, emit_stub=false）
#   protocol/ts/src/{index,<域>}.ts        包入口与按域桶文件（本脚本渲染，非手写）
#   protocol/csharp/api/**/*_client.g.cs  零 protobuf 运行时 POCO（同上 lang=csharp）
# TS/C# 另保留 google/protobuf/opclient/timestamp_*（SessionMeta.server_time 的 WKT 类型），
# 其余 route/empty/descriptor 生成物只为解析依赖而入闭包，生成后删除（见 prune_closure）。
#
# 手写入库、本脚本不改写的文件：protocol/README.md、protocol/go/go.{mod,sum}、
# protocol/ts/{package.json,tsconfig.json}、protocol/csharp/*.csproj。
# （protocol/ts/README.md 是 protocol/README.md 的复制件，由本脚本渲染。）
#
# 环境变量（与 make proto 同一套约定）：
#   ATLAS_DIR  框架仓根（默认同级 ../atlas；提供 route/lockstep proto 与插件源码）
#   ATLAS_BIN  框架仓插件目录（默认 $ATLAS_DIR/bin）
#   PROTOC     protoc 可执行文件（默认 protoc）
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

ATLAS_DIR="${ATLAS_DIR:-$(cd .. && pwd)/atlas}"
ATLAS_BIN="${ATLAS_BIN:-$ATLAS_DIR/bin}"
PROTOC="${PROTOC:-protoc}"
BIN_DIR="$REPO_ROOT/bin"

PROTOCOL_DIR="$REPO_ROOT/protocol"
GO_OUT="$PROTOCOL_DIR/go"
TS_OUT="$PROTOCOL_DIR/ts/src"
CS_OUT="$PROTOCOL_DIR/csharp"

# GO_MODULE 是 protocol/go 的 module 路径（生成物的 Go import 前缀）。
GO_MODULE="github.com/huangyuCN/atlas-game-layout/protocol"
# CS_NAMESPACE_PREFIX 是 C# 产物命名空间前缀（与 SDK 的 Atlas.* 命名空间错开，避免同集会撞类型）。
CS_NAMESPACE_PREFIX="HuangyuCN.AtlasGame.Protocol"

# 生成清单：「proto 路径|Go 包名」。前 7 条是三个游戏域；后 3 条是它们依赖的契约域副本。
PROTO_SET=(
	"api/battle/v1/battle.proto|battlev1"
	"api/battle/v1/battle_service.proto|battlev1"
	"api/battle/v1/migration.proto|battlev1"
	"api/game/v1/player.proto|gamev1"
	"api/game/v1/player_service.proto|gamev1"
	"api/matcher/v1/matcher.proto|matcherv1"
	"api/matcher/v1/match_events.proto|matcherv1"
	"api/atlas/v1/route.proto|atlasroutepb"
	"api/lockstep/lockstep.proto|locksteppb"
	"api/common/v1/common.proto|commonv1"
)
# DOMAIN_PROTO_COUNT 是清单里属于「三个游戏域」的条目数（前面的若干条）。
DOMAIN_PROTO_COUNT=7

die() {
	echo "gen-protocol: $*" >&2
	exit 1
}

# require_upstream 校验框架仓与 protoc 可用（缺一样都直接失败，不产出半套产物）。
require_upstream() {
	[ -d "$ATLAS_DIR" ] || die "框架仓不存在：$ATLAS_DIR（用 ATLAS_DIR 指定）"
	[ -f "$ATLAS_DIR/api/lockstep/lockstep.proto" ] ||
		die "$ATLAS_DIR 下找不到 api/lockstep/lockstep.proto（ATLAS_DIR 指错了？）"
	command -v "$PROTOC" >/dev/null 2>&1 || die "找不到 protoc（用 PROTOC 指定，如 PROTOC=/opt/homebrew/bin/protoc）"
}

# build_atlas_plugin 从框架仓源码构建 protoc-gen-atlas-client 到 ./bin。
# 一律源码构建而非复用现成二进制：emit_stub 是生成协议包必需的开关，旧插件会在
# protoc 参数解析阶段直接报「no such flag」，届时错误信息离真因很远（这里先把它挡掉）。
build_atlas_plugin() {
	local src="$ATLAS_DIR/cmd/protoc-gen-atlas-client"
	[ -d "$src" ] || die "框架仓缺插件源码：$src"
	mkdir -p "$BIN_DIR"
	echo "插件：从 $ATLAS_DIR 源码构建 protoc-gen-atlas-client"
	(cd "$ATLAS_DIR" && go build -o "$BIN_DIR/protoc-gen-atlas-client" ./cmd/protoc-gen-atlas-client)
}

# require_go_plugin 找 protoc-gen-go（Go 产物必需）：./bin → $ATLAS_BIN → PATH。
require_go_plugin() {
	if [ -x "$BIN_DIR/protoc-gen-go" ] || [ -x "$ATLAS_BIN/protoc-gen-go" ] ||
		command -v protoc-gen-go >/dev/null 2>&1; then
		return 0
	fi
	die "找不到 protoc-gen-go：先执行 make proto-tools（或在 ATLAS_BIN 下备好该插件）"
}

# go_opt_args 输出 --go_opt=M<proto>=<import>;<pkg> 参数（每行一个），
# 把生成物的 Go import 路径钉在 protocol 模块内（否则会跟 proto 的 go_package 走到框架仓/本仓）。
go_opt_args() {
	local entry proto pkg dir
	for entry in "${PROTO_SET[@]}"; do
		proto="${entry%%|*}"
		pkg="${entry##*|}"
		dir="$(dirname "$proto")"
		printf -- '--go_opt=M%s=%s/%s;%s\n' "$proto" "$GO_MODULE" "$dir" "$pkg"
	done
}

# proto_list 输出清单里的 proto 路径（每行一个）：$1 为条数，缺省全部。
proto_list() {
	local limit="${1:-${#PROTO_SET[@]}}" i entry
	for ((i = 0; i < limit; i++)); do
		entry="${PROTO_SET[$i]}"
		printf '%s\n' "${entry%%|*}"
	done
}

# proto_includes 输出 protoc 的公共 include 根（每行一个）：模板仓（api/common）、
# third_party、框架仓（api/atlas、api/lockstep）；google/protobuf 的 WKT 由 protoc 自带。
proto_includes() {
	printf '%s\n' "-I." "-Ithird_party" "-I$ATLAS_DIR"
}

gen_go() {
	local -a opts=()
	local opt
	while IFS= read -r opt; do opts+=("$opt"); done < <(go_opt_args)
	local -a inc=()
	while IFS= read -r opt; do inc+=("$opt"); done < <(proto_includes)
	local -a protos=()
	while IFS= read -r opt; do protos+=("$opt"); done < <(proto_list)

	rm -rf "$GO_OUT/api"
	mkdir -p "$GO_OUT"
	# paths=source_relative：产物落在 api/<域>/<版本>/ 下，与 proto 源路径一致。
	# 契约域副本（route/lockstep/common）同样按 M 映射生成到本模块内，Go 侧零外部依赖。
	PATH="$BIN_DIR:$ATLAS_BIN:$PATH" "$PROTOC" "${inc[@]}" \
		--go_out="$GO_OUT" --go_opt=paths=source_relative "${opts[@]}" "${protos[@]}"
	echo "Go   产物 → protocol/go/api（${#protos[@]} 个 proto）"
}

# prune_closure 清掉「只为解析依赖而入闭包、但产物无运行期引用」的生成物：
#   - api/atlas/**：atlas.route.v1 注解消息（proto 选项用，emit_stub=false 后无引用）；
#   - google/protobuf/** 里除 timestamp 之外的部分：empty 只是 stub 的返回类型，
#     descriptor 是 route.proto 的 import 闭包。
# timestamp 必须留：lockstep 的 SessionMeta.server_time 字段类型就是它。
# 参数：$1=产物根目录，$2=文件名后缀（_pb.ts / _client.g.cs）。
prune_closure() {
	local root="$1" suffix="$2" f
	rm -rf "$root/api/atlas"
	[ -d "$root/google" ] || return 0
	while IFS= read -r f; do
		case "$f" in
		*/timestamp"$suffix") ;;
		*) rm -f "$f" ;;
		esac
	done < <(find "$root/google" -type f)
	find "$root/google" -type d -empty -delete
}

# gen_ts 生成 TS DTO 与 op 常量（emit_stub=false：不引 SDK 的 Invoker，产物零依赖）。
gen_ts() {
	local -a inc=()
	while IFS= read -r opt; do inc+=("$opt"); done < <(proto_includes)
	local -a protos=()
	while IFS= read -r opt; do protos+=("$opt"); done < <(proto_list "$DOMAIN_PROTO_COUNT")

	rm -rf "$TS_OUT/api" "$TS_OUT/google"
	rm -f "$TS_OUT"/*.ts
	mkdir -p "$TS_OUT"
	PATH="$BIN_DIR:$ATLAS_BIN:$PATH" "$PROTOC" "${inc[@]}" \
		--plugin="protoc-gen-atlas-client=$BIN_DIR/protoc-gen-atlas-client" \
		--atlas-client_out="$TS_OUT" \
		--atlas-client_opt=lang=ts,emit_stub=false,paths=source_relative \
		"${protos[@]}"
	prune_closure "$TS_OUT" _pb.ts
	echo "TS   产物 → protocol/ts/src/api"
}

# gen_csharp 生成 C# POCO 与 op 常量（emit_stub=false：不引 SDK 的 IAtlasInvoker）。
gen_csharp() {
	local -a inc=()
	while IFS= read -r opt; do inc+=("$opt"); done < <(proto_includes)
	local -a protos=()
	while IFS= read -r opt; do protos+=("$opt"); done < <(proto_list "$DOMAIN_PROTO_COUNT")

	rm -rf "$CS_OUT/api" "$CS_OUT/google"
	mkdir -p "$CS_OUT"
	PATH="$BIN_DIR:$ATLAS_BIN:$PATH" "$PROTOC" "${inc[@]}" \
		--plugin="protoc-gen-atlas-client=$BIN_DIR/protoc-gen-atlas-client" \
		--atlas-client_out="$CS_OUT" \
		--atlas-client_opt="lang=csharp,emit_stub=false,csharp_namespace_prefix=$CS_NAMESPACE_PREFIX,paths=source_relative" \
		"${protos[@]}"
	prune_closure "$CS_OUT" _client.g.cs
	echo "C#   产物 → protocol/csharp/api"
}

# render_ts_index 渲染 TS 包入口：每个 proto 包（api/<域>/）一个桶文件，根入口按域
# 命名空间转发。不能把全部生成物塞进一个 export * —— 不同 proto 包允许同名消息
# （game.v1.CancelMatchReply 与 matcher.v1.CancelMatchReply 都叫 CancelMatchReply），
# 扁平化转发会被 TS 判为歧义（TS2308）并静默丢一个。
render_ts_index() {
	local rel bucket
	local -a buckets=()
	while IFS= read -r bucket; do buckets+=("$bucket"); done < <(
		cd "$TS_OUT" && find api -name '*_pb.ts' | sed 's|^api/||; s|/.*||' | sort -u
	)
	for bucket in "${buckets[@]}"; do
		{
			echo "// Code generated by scripts/gen-protocol.sh (lang=ts). DO NOT EDIT."
			echo "// 游戏协议包 ${bucket} 域：转发生成物（DTO 类型 + op 常量 + 推送 op 常量）。"
			echo
			while IFS= read -r rel; do
				printf 'export * from "./%s";\n' "${rel%.ts}.js"
			done < <(cd "$TS_OUT" && find "api/$bucket" -name '*_pb.ts' | sort)
		} >"$TS_OUT/$bucket.ts"
	done
	{
		echo "// Code generated by scripts/gen-protocol.sh (lang=ts). DO NOT EDIT."
		echo "// 游戏协议包入口：按域命名空间转发（跨包同名消息因此不会互相遮蔽）。"
		echo
		for bucket in "${buckets[@]}"; do
			printf 'export * as %s from "./%s.js";\n' "$bucket" "$bucket"
		done
	} >"$TS_OUT/index.ts"
	echo "TS   入口 → protocol/ts/src/{index,${buckets[*]// /,}}.ts"
}

# render_ts_readme 把 protocol/README.md 复制成 TS 包内的 README：npm 只把**包根**的
# README.md 打进 tarball，而 TS 包根在 protocol/ts/。首行加生成标注，避免有人直接改副本。
render_ts_readme() {
	{
		echo "<!-- 由 scripts/gen-protocol.sh 从 protocol/README.md 复制生成，改动请改该源文件。 -->"
		echo
		cat "$PROTOCOL_DIR/README.md"
	} >"$PROTOCOL_DIR/ts/README.md"
	echo "TS   README → protocol/ts/README.md（复制自 protocol/README.md）"
}

main() {
	require_upstream
	require_go_plugin
	build_atlas_plugin

	gen_go
	gen_ts
	gen_csharp
	render_ts_index
	render_ts_readme

	echo "完成：protocol/{go,ts,csharp}（重跑本脚本应无 diff）"
}

main "$@"
