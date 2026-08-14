# Atlas Game Layout Makefile
# 游戏单仓模板构建入口：四服务构建/运行、proto 生成、中间件环境。

.DEFAULT_GOAL := help

BIN_DIR := bin
SERVICES := gateway game matcher battle
GO ?= go
PROTOC ?= protoc

.PHONY: help build run-all compose proto clean $(addprefix run-,$(SERVICES)) $(addprefix build-,$(SERVICES))

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

proto: ## 生成全部 proto 产物（依赖 protoc 与 Atlas 插件）
	@echo "TODO(M3): 编排 protoc 全家桶"

compose: ## 起中间件（etcd/redis/nats/mongo）
	@docker compose -f deploy/docker-compose/compose.yaml up -d

clean:
	rm -rf $(BIN_DIR)
