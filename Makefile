BASE_IMAGE = golang:1.23-alpine3.20
LINT_IMAGE = golangci/golangci-lint:v1.61.0
NODE_IMAGE = node:20-alpine3.20
ALPINE_IMAGE = alpine:3.20
RPI32_IMAGE = balenalib/raspberry-pi:bullseye-run-20240508
RPI64_IMAGE = balenalib/raspberrypi3-64:bullseye-run-20240429
BIN_NAME=vms
CORE_BIN_NAME=w3stream-core
STREAM_BIN_NAME=w3stream-live
STG_REMOTE_PATH=/mnt/staging_data/w3stream/bin
PROD_REMOTE_PATH=/mnt/w3stream/bin
REMOTE_USER=root
CONTAINER_NAME=w3stream
CORE_CONTAINER_NAME=w3stream-core
STREAM_CONTAINER_NAME=w3stream-live
GRPC_CONTAINER_NAME=w3stream-grpc
GRPC_BIN_NAME=w3stream-grpc


.PHONY: $(shell ls)

help:
	@echo "usage: make [action]"
	@echo ""
	@echo "available actions:"
	@echo ""
	@echo "  mod-tidy         run go mod tidy"
	@echo "  format           format source files"
	@echo "  test             run tests"
	@echo "  test32           run tests on a 32-bit system"
	@echo "  test-highlevel   run high-level tests"
	@echo "  lint             run linters"
	@echo "  run              run app"
	@echo "  apidocs          generate api docs HTML"
	@echo "  binaries         build binaries for all platforms"
	@echo "  dockerhub        build and push images to Docker Hub"
	@echo "  dockerhub-legacy build and push images to Docker Hub (legacy)"
	@echo ""

blank :=
define NL

$(blank)
endef

include scripts/*.mk

build:
	@CGO_ENABLED=0 go build -o bin/$(STREAM_BIN_NAME) .

deploy-prod: build
	@ssh root@w3stream mv $(PROD_REMOTE_PATH)/$(STREAM_BIN_NAME) $(PROD_REMOTE_PATH)/BackUps/$(STREAM_BIN_NAME).bk`date +%Y%m%d%H`
	@scp bin/$(STREAM_BIN_NAME) root@w3stream:$(PROD_REMOTE_PATH)
	@ssh root@w3stream docker restart $(STREAM_CONTAINER_NAME)


