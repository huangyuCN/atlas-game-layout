# Atlas Game Layout Makefile
# 游戏单仓模板构建入口：四服务构建/运行、proto 生成、中间件环境。

.DEFAULT_GOAL := help

BIN_DIR := bin
SERVICES := gateway game matcher battle
GO ?= go
PROTOC ?= protoc

.PHONY: help build run-all compose proto proto-tools clean $(addprefix run-,$(SERVICES)) $(addprefix build-,$(SERVICES))

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

run-%: ## 运行单个服务（make run-gateway）
	$(GO) run ./services/$*/cmd

run-all: ## 一键起四服务（前台交错输出，Ctrl-C 全部退出）
	@trap 'kill 0' INT TERM; \
	for s in $(SERVICES); do $(GO) run ./services/$$s/cmd & done; wait


compose: ## 起中间件（etcd/redis/nats/mongo）
	@docker compose -f deploy/docker-compose/compose.yaml up -d

clean:
	rm -rf $(BIN_DIR)

# proto 生成：Atlas 插件来自 ATLAS_BIN（本地开发=../atlas/bin，用户项目=atlas upgrade 安装目录）。
# proto-tools 会把全家桶插件复制到本仓库 ./bin，用户项目生成时不再依赖 Atlas 源码仓库。
PROTO_INC := -I. -Ithird_party
ATLAS_BIN ?= ../atlas/bin
ATLAS_DIR ?= $(dir $(ATLAS_BIN))
API_SERVICE_PROTOS := api/game/v1/player.proto api/matcher/v1/matcher.proto api/battle/v1/battle.proto
API_GATEWAY_PROTOS := api/gateway/v1/auth.proto api/gateway/v1/battle.proto
API_ALL_PROTOS := api/common/v1/common.proto api/error/v1/errors.proto api/matcher/v1/match_events.proto $(API_SERVICE_PROTOS) $(API_GATEWAY_PROTOS)

.PHONY: proto proto-tools

proto-tools: ## 构建/收集 protoc 插件到 ./bin（Atlas 全家桶 + go/go-grpc/openapi）
	@mkdir -p $(BIN_DIR)
	@$(MAKE) -C $(abspath $(ATLAS_DIR)) proto-tools
	@cp $(abspath $(ATLAS_DIR))/bin/protoc-gen-atlas-* $(BIN_DIR)/ 2>/dev/null || true
	@GOBIN="$(PWD)/$(BIN_DIR)" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@GOBIN="$(PWD)/$(BIN_DIR)" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@GOBIN="$(PWD)/$(BIN_DIR)" $(GO) install github.com/google/gnostic/cmd/protoc-gen-openapi@latest

proto: proto-tools ## 生成全部 proto 产物（go/grpc/http/多传输/errors/openapi/配置）
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--go_out=. --go_opt=paths=source_relative \
		protobuf/configs/*.proto services/*/internal/conf/conf.proto $(API_ALL_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(API_SERVICE_PROTOS)
	@PATH="$(PWD)/$(BIN_DIR):$(abspath $(ATLAS_BIN)):$$PATH" $(PROTOC) $(PROTO_INC) \
		--atlas-http_out=. --atlas-http_opt=paths=source_relative \
		$(API_SERVICE_PROTOS)
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
