# Atlas Game Layout Makefile
# 游戏单仓模板构建入口：四服务构建/运行、proto 生成、中间件环境。

.DEFAULT_GOAL := help

BIN_DIR := bin
SERVICES := gateway game matcher battle
GO ?= go
PROTOC ?= protoc

.PHONY: help build lint comment-lint run-all compose proto proto-tools clean $(addprefix run-,$(SERVICES)) $(addprefix build-,$(SERVICES))

help: ## 列出所有目标
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

build: ## 构建全部服务到 ./bin
	@mkdir -p $(BIN_DIR)
	@for s in $(SERVICES); do \
		$(GO) build -o $(BIN_DIR)/$$s ./services/$$s/cmd || exit 1; \
	done

build-%: ## 构建单个服务（make build-gateway）
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/$* ./services/$*/cmd

lint: ## 代码规范检查：包名前缀 + Go doc 注释规范
	$(GO) run ./scripts/check-pkgname . scripts/check-pkgname/allowlist.txt
	$(GO) run ./scripts/go-comment-lint .

comment-lint: ## Go doc 注释规范检查（首词=声明名等，详见 docs/go-comments.md；违规退出码 1）
	$(GO) run ./scripts/go-comment-lint .

run-%: ## 运行单个服务（make run-gateway）
	$(GO) run ./services/$*/cmd

run-all: ## 一键起四服务（前台交错输出，Ctrl-C 全部退出并等待子进程结束）
	@trap 'kill 0; wait' INT TERM; \
	for s in $(SERVICES); do $(GO) run ./services/$$s/cmd & done; wait

# e2e 客户端形态：dual（TCP+KCP 双通道）/ single（WS 单通道）。
E2E_MODE ?= dual

e2e: ## 运行 e2e 闭环脚本（需 make compose + make run-all；-e E2E_MODE=single 切 WS 单通道）
	$(GO) run ./scripts/e2e -mode $(E2E_MODE)


compose: ## 起中间件（etcd/redis/nats/mongo）
	@docker compose -f deploy/docker-compose/compose.yaml up -d

clean:
	rm -rf $(BIN_DIR)

# proto 生成：Atlas 插件来自 ATLAS_BIN（本地开发=../atlas/bin，用户项目=atlas upgrade 安装目录）。
# proto 生成：Atlas 插件来自 ATLAS_BIN（本地开发=../atlas/bin，用户项目=atlas upgrade
# 安装目录，如 `make proto ATLAS_BIN="$(go env GOBIN)"`）。proto-tools 优先收集现成
# 插件，缺插件且存在 Atlas 源码时回退到源码构建——用户项目不依赖 Atlas 源码仓库。
PROTO_INC := -I. -Ithird_party
ATLAS_BIN ?= ../atlas/bin
ATLAS_DIR ?= $(dir $(ATLAS_BIN))
API_SERVICE_PROTOS := api/game/v1/player.proto api/matcher/v1/matcher.proto api/battle/v1/battle.proto
API_GATEWAY_PROTOS := api/gateway/v1/auth.proto api/gateway/v1/battle.proto
API_ALL_PROTOS := api/common/v1/common.proto api/error/v1/errors.proto api/matcher/v1/match_events.proto $(API_SERVICE_PROTOS) $(API_GATEWAY_PROTOS) api/gateway/v1/matcher.proto

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
		protobuf/configs/*.proto services/*/internal/conf/conf.proto $(API_ALL_PROTOS) api/game/v1/player_actor.proto
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(API_SERVICE_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-http_out=. --atlas-http_opt=paths=source_relative \
		$(API_SERVICE_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-actor_out=. --atlas-actor_opt=paths=source_relative \
		api/game/v1/player_actor.proto api/battle/v1/battle_actor.proto
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-tcp_out=. --atlas-tcp_opt=paths=source_relative \
		--atlas-ws_out=. --atlas-ws_opt=paths=source_relative \
		api/gateway/v1/matcher.proto
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-tcp_out=. --atlas-tcp_opt=paths=source_relative \
		--atlas-udp_out=. --atlas-udp_opt=paths=source_relative \
		--atlas-kcp_out=. --atlas-kcp_opt=paths=source_relative \
		--atlas-ws_out=. --atlas-ws_opt=paths=source_relative \
		$(API_GATEWAY_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-errors_out=. --atlas-errors_opt=paths=source_relative,biz_code_key=biz_code,biz_reason_key=biz_reason \
		api/error/v1/errors.proto
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--openapi_out=paths=source_relative:. $(API_ALL_PROTOS)
