# Atlas Game Layout Makefile
# 游戏单仓模板构建入口：五服务（gateway/game/matcher/battle/edge）构建/运行、proto 生成、中间件环境。

.DEFAULT_GOAL := help

BIN_DIR := bin
SERVICES := gateway game matcher battle edge
GO ?= go
PROTOC ?= protoc

# 版本注入：构建期把版本/提交/构建时间写入 lib/version（-X 只能改 var，不能改 const）。
# 本地 make build 取 git describe；CI 可显式覆盖：make build VERSION=${GITHUB_REF_NAME}。
MODULE := github.com/huangyuCN/atlas-game-layout
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -X $(MODULE)/lib/version.Version=$(VERSION) \
           -X $(MODULE)/lib/version.Commit=$(COMMIT) \
           -X $(MODULE)/lib/version.BuildTime=$(BUILD_TIME)

# 注意：这里**不能**把 `$(addprefix build-,$(SERVICES))` 也列为 .PHONY——GNU make 对 .PHONY
# 目标会跳过隐式规则搜索，声明后 `make build-<服务>` 会命中「无配方伪目标」而空转成
# "Nothing to be done"，真正的 `build-%:` 配方永不执行。改由 FORCE 前置拿到等价的「永远重建」语义。
.PHONY: help build build-gmctl lint comment-lint check-dup check-deps run-all compose proto proto-tools clean FORCE

help: ## 列出所有目标
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

build: ## 构建全部服务到 ./bin
	@mkdir -p $(BIN_DIR)
	@for s in $(SERVICES); do \
		$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$$s ./services/$$s/cmd || exit 1; \
	done

build-%: FORCE ## 构建单个服务（make build-gateway）
	@mkdir -p $(BIN_DIR)
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$* ./services/$*/cmd

# FORCE 是空配方伪目标：作 `build-%` 的前置，使每个 `make build-<服务>` 都必然重建
#（等价于「该目标是 phony」，但不触发 .PHONY 跳过隐式规则搜索的行为）。
FORCE:

build-gmctl: ## 构建 GM 命令行到 ./bin/gmctl（管理面 internal listener 的运维工具，无版本注入）
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/gmctl ./cmd/gmctl

lint: ## 代码规范检查：包名前缀 + Go doc 注释 + 重复代码 + 依赖边界
	$(GO) run ./scripts/check-pkgname . scripts/check-pkgname/allowlist.txt
	$(GO) run ./scripts/go-comment-lint .
	$(GO) run ./scripts/check-dup . scripts/check-dup/allowlist.txt
	$(GO) run ./scripts/check-deps .

check-dup: ## 重复代码检查（结构指纹归一，详见 scripts/check-dup/allowlist.txt；违规退出码 1）
	$(GO) run ./scripts/check-dup . scripts/check-dup/allowlist.txt

check-deps: ## 依赖边界检查：网关/matcher 不得依赖 actor 集群运行时（阶段 2「退出 actor 面」的落地判据）
	$(GO) run ./scripts/check-deps .

comment-lint: ## Go doc 注释规范检查（首词=声明名等，详见 docs/go-comments.md；违规退出码 1）
	$(GO) run ./scripts/go-comment-lint .

run-%: FORCE ## 运行单个服务（make run-gateway）
	$(GO) run ./services/$*/cmd

run-all: ## 一键起全部服务（前台交错输出，Ctrl-C 全部退出并等待子进程结束）
	@trap 'kill 0; wait' INT TERM; \
	for s in $(SERVICES); do $(GO) run ./services/$$s/cmd & done; wait

# e2e 客户端形态：dual（TCP 业务通道 + KCP 直连帧面）/ single（WS 业务通道 + WS 直连帧面）
# / party / fault / kick / freeze / direct（战斗帧一律直连 battle 帧面，阶段 3 批次 5 起不经网关）。
E2E_MODE ?= dual

e2e: ## 运行 e2e 闭环脚本（脚本自起所需服务，仅需先 make compose；-e E2E_MODE=single 切 WS 形态）
	$(GO) run ./scripts/e2e -mode $(E2E_MODE)


compose: ## 起中间件（etcd/redis/nats/mongo）
	@docker compose -f deploy/docker-compose/compose.yaml up -d

clean:
	rm -rf $(BIN_DIR)

# proto 生成：Atlas 插件来自 ATLAS_BIN（本地开发=../atlas/bin，用户项目=atlas upgrade
# 安装目录，如 `make proto ATLAS_BIN="$(go env GOBIN)"`）。proto-tools 优先收集现成
# 插件，缺插件且存在 Atlas 源码时回退到源码构建——用户项目不依赖 Atlas 源码仓库。
# ATLAS_BIN/ATLAS_DIR 必须先于 PROTO_INC 定义：PROTO_INC 用 := 即时展开，
# 引用后置变量会展开成空、留下悬空 -I（protoc 报 Missing value for flag）。
ATLAS_BIN ?= ../atlas/bin
ATLAS_DIR ?= $(dir $(ATLAS_BIN))
PROTO_INC := -I. -Ithird_party -I$(abspath $(ATLAS_DIR))
# 信任边界三层：
#  1) 管理面（api/admin/**）：普通 gRPC 内网面，**只**进 --go_out/--go-grpc_out/--openapi_out。
#     隔离理由：管理面不加 atlas.route.v1 / google.api.http 注解，但「插件自行跳过」只是默认值、
#     不是契约——atlas-http 的 omitempty 默认 true（跳过无注解 service），一旦传
#     omitempty=false 就会为 AdminService 产出兜底 REST 路由（POST /admin.game.v1.AdminService/*），
#     把管理面暴露成 HTTP 接口；atlas-actor / atlas-client / 四传输插件也只按注解或命名约定过滤，
#     有人给 admin.proto 补注解或改 service 名即会产出 actor 桩 / 客户端 SDK。
#     故用 API_ADMIN_PROTOS 把管理面挡在这些调用行之外：隔离由 Makefile 保证，不依赖插件行为。
#  2) 域统一契约（客户端 op + actor 分发共用一份 service）：atlas-actor 生成路由表 +
#     actor/ 分发桩 + rpc/ 双面接入层（自带 grpc.ServiceDesc）、atlas-client 生成 opclient/ SDK stub；
#     透传引擎按注解路由表运行时注册。
#  3) 会话生命周期（gateway.v1.Session，Gateway 自留，四传输生成桩）。
# 注意：域 proto **不进 --go-grpc_out**——rpc/ 平面自持 gRPC 管线，不依赖标准 _grpc.pb.go；
# 只有 API_SERVICE_PROTOS（会话等无 route 注解的 proto）与 API_ADMIN_PROTOS 需要标准 gRPC 产物。
API_SERVICE_PROTOS := api/game/v1/player.proto api/matcher/v1/matcher.proto api/battle/v1/battle.proto
API_DOMAIN_PROTOS := api/game/v1/player_service.proto api/battle/v1/battle_service.proto
API_SESSION_PROTOS := api/gateway/v1/session.proto
# 管理面：只进 go/go-grpc/openapi（理由见上），绝不进 atlas-http/atlas-actor/atlas-client 与四传输插件。
API_ADMIN_PROTOS := api/admin/game/v1/admin.proto
# migration.proto 是战斗迁移控制面的消息-only 契约（阶段 3 批次 7）：不进 route 注解，
# 只产出 --go_out 的 Go 消息（不产生 actor/rpc/opclient/传输插件产物）。
API_MIGRATION_PROTOS := api/battle/v1/migration.proto
API_ALL_PROTOS := api/common/v1/common.proto api/error/v1/errors.proto api/matcher/v1/match_events.proto $(API_SERVICE_PROTOS) $(API_DOMAIN_PROTOS) $(API_SESSION_PROTOS) $(API_MIGRATION_PROTOS)

.PHONY: proto proto-tools

proto-tools: ## 收集/构建 protoc 插件到 ./bin（Atlas 全家桶 + go/go-grpc/openapi）
	@mkdir -p $(BIN_DIR)
	@if [ -f "$(ATLAS_BIN)/protoc-gen-atlas-http" ]; then \
		echo "atlas 插件：从 $(ATLAS_BIN) 收集"; \
		cp $(ATLAS_BIN)/protoc-gen-atlas-* $(BIN_DIR)/ 2>/dev/null || true; \
	elif [ -d "$(abspath $(ATLAS_DIR))" ]; then \
		echo "atlas 插件：从 $(abspath $(ATLAS_DIR)) 源码构建"; \
		$(MAKE) -C $(abspath $(ATLAS_DIR)) proto-tools; \
		cp $(abspath $(ATLAS_DIR))/bin/protoc-gen-atlas-* $(BIN_DIR)/ 2>/dev/null || true; \
	else \
		echo "错误：ATLAS_BIN 缺插件且无 Atlas 源码；请先执行 atlas upgrade 并以 ATLAS_BIN=$$(go env GOPATH)/bin 重试" >&2; \
		exit 1; \
	fi
	@# 外部插件版本钉死：生成物头部记录插件版本（protoc-gen-go v1.36.12 /
	@# protoc-gen-go-grpc v1.6.2），@latest 漂移会让 CI 的「重生成无 diff」门禁假红。
	@GOBIN="$(PWD)/$(BIN_DIR)" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	@GOBIN="$(PWD)/$(BIN_DIR)" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
	@GOBIN="$(PWD)/$(BIN_DIR)" $(GO) install github.com/google/gnostic/cmd/protoc-gen-openapi@v0.7.1

proto: proto-tools ## 生成全部 proto 产物（go/grpc/http/多传输/errors/openapi/配置）
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--go_out=. --go_opt=paths=source_relative \
		protobuf/configs/*.proto services/*/internal/conf/conf.proto $(API_ALL_PROTOS) $(API_ADMIN_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(API_SERVICE_PROTOS) $(API_ADMIN_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-http_out=. --atlas-http_opt=paths=source_relative \
		$(API_SERVICE_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-actor_out=. --atlas-actor_opt=paths=source_relative \
		$(API_DOMAIN_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-client_out=. --atlas-client_opt=paths=source_relative \
		$(API_DOMAIN_PROTOS)
# 会话协议 stub + 协议描述符（R13：会话协议留在模板，按项目生成三语言产物；
# 描述符给出 op 名与 token/playerID/expiresAt 提取器，供 SDK 的 SessionProtocol 接缝消费）。
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-client_out=. --atlas-client_opt=paths=source_relative \
		$(API_SESSION_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-tcp_out=. --atlas-tcp_opt=paths=source_relative \
		--atlas-ws_out=. --atlas-ws_opt=paths=source_relative \
		--atlas-udp_out=. --atlas-udp_opt=paths=source_relative \
		--atlas-kcp_out=. --atlas-kcp_opt=paths=source_relative \
		$(API_SESSION_PROTOS)
	@mkdir -p api/client/ts api/client/csharp
# 多语言 stub 必须分两次独立 protoc 调用：同一次调用里重复 --atlas-client_opt
# 会被 protoc 逗号拼接（lang 键重复，flag 解析后到者赢=csharp 覆盖 ts）、
# 重复 --atlas-client_out 会让 protoc 对每次 out 各调用一次插件——结果两个
# 目录都产出 csharp 代码，ts 产物静默缺失。
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-client_opt=lang=ts,paths=source_relative --atlas-client_out=api/client/ts \
		$(API_DOMAIN_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-client_opt=lang=csharp,paths=source_relative --atlas-client_out=api/client/csharp \
		$(API_DOMAIN_PROTOS)
# 会话协议的 TS/C# 产物：同样分两次独立调用（原因见上）。
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-client_opt=lang=ts,paths=source_relative --atlas-client_out=api/client/ts \
		$(API_SESSION_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-client_opt=lang=csharp,paths=source_relative --atlas-client_out=api/client/csharp \
		$(API_SESSION_PROTOS)

	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-errors_out=. --atlas-errors_opt=paths=source_relative,biz_code_key=biz_code,biz_reason_key=biz_reason \
		api/error/v1/errors.proto
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--openapi_out=paths=source_relative:. $(API_ALL_PROTOS) $(API_ADMIN_PROTOS)
