# Atlas Game Layout Makefile
# 游戏单仓模板构建入口：四服务构建/运行、proto 生成、中间件环境。

.DEFAULT_GOAL := help

BIN_DIR := bin
SERVICES := gateway game matcher battle
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

.PHONY: help build lint comment-lint check-dup run-all compose proto proto-tools clean $(addprefix run-,$(SERVICES)) $(addprefix build-,$(SERVICES))

help: ## 列出所有目标
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

build: ## 构建全部服务到 ./bin
	@mkdir -p $(BIN_DIR)
	@for s in $(SERVICES); do \
		$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$$s ./services/$$s/cmd || exit 1; \
	done

build-%: ## 构建单个服务（make build-gateway）
	@mkdir -p $(BIN_DIR)
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$* ./services/$*/cmd

lint: ## 代码规范检查：包名前缀 + Go doc 注释 + 重复代码
	$(GO) run ./scripts/check-pkgname . scripts/check-pkgname/allowlist.txt
	$(GO) run ./scripts/go-comment-lint .
	$(GO) run ./scripts/check-dup . scripts/check-dup/allowlist.txt

check-dup: ## 重复代码检查（结构指纹归一，详见 scripts/check-dup/allowlist.txt；违规退出码 1）
	$(GO) run ./scripts/check-dup . scripts/check-dup/allowlist.txt

comment-lint: ## Go doc 注释规范检查（首词=声明名等，详见 docs/go-comments.md；违规退出码 1）
	$(GO) run ./scripts/go-comment-lint .

run-%: ## 运行单个服务（make run-gateway）
	$(GO) run ./services/$*/cmd

run-all: ## 一键起四服务（前台交错输出，Ctrl-C 全部退出并等待子进程结束）
	@trap 'kill 0; wait' INT TERM; \
	for s in $(SERVICES); do $(GO) run ./services/$$s/cmd & done; wait

# e2e 客户端形态：dual（TCP+KCP 双通道）/ single（WS 单通道）/ party / fault / kick。
E2E_MODE ?= dual

e2e: ## 运行 e2e 闭环脚本（脚本自起四服务，仅需先 make compose；-e E2E_MODE=single 切 WS 单通道）
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
# 信任边界三层：管理面（google.api.http REST，atlas-http/go-grpc 生成）、
# 域统一契约（客户端 op + actor 分发共用一份 service：atlas-actor 生成分发桩 +
# 路由表 + RPC 接入层、atlas-client 生成 SDK stub；透传引擎按注解路由表运行时注册）、
# 注意：域 proto 必须与 --go-grpc_out 同批生成——接入层产物实现 gRPC 接口，漏生成编译不过。
# 会话生命周期（gateway.v1.Session，Gateway 自留，四传输生成桩）。
API_SERVICE_PROTOS := api/game/v1/player.proto api/matcher/v1/matcher.proto api/battle/v1/battle.proto
API_DOMAIN_PROTOS := api/game/v1/player_service.proto api/battle/v1/battle_service.proto
API_SESSION_PROTOS := api/gateway/v1/session.proto
API_ALL_PROTOS := api/common/v1/common.proto api/error/v1/errors.proto api/matcher/v1/match_events.proto $(API_SERVICE_PROTOS) $(API_DOMAIN_PROTOS) $(API_SESSION_PROTOS)

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
	@GOBIN="$(PWD)/$(BIN_DIR)" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@GOBIN="$(PWD)/$(BIN_DIR)" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@GOBIN="$(PWD)/$(BIN_DIR)" $(GO) install github.com/google/gnostic/cmd/protoc-gen-openapi@latest

proto: proto-tools ## 生成全部 proto 产物（go/grpc/http/多传输/errors/openapi/配置）
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--go_out=. --go_opt=paths=source_relative \
		protobuf/configs/*.proto services/*/internal/conf/conf.proto $(API_ALL_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(API_SERVICE_PROTOS) $(API_DOMAIN_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-http_out=. --atlas-http_opt=paths=source_relative \
		$(API_SERVICE_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-actor_out=. --atlas-actor_opt=paths=source_relative \
		$(API_DOMAIN_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-client_out=. --atlas-client_opt=paths=source_relative \
		$(API_DOMAIN_PROTOS)
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

	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-errors_out=. --atlas-errors_opt=paths=source_relative,biz_code_key=biz_code,biz_reason_key=biz_reason \
		api/error/v1/errors.proto
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--openapi_out=paths=source_relative:. $(API_ALL_PROTOS)
