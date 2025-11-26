# Makefile for Clarium (spun off from Timeline)

SHELL := /usr/bin/env bash

# Versions and image naming
REGISTRY ?=
IMAGE_NAME ?= clarium
IMAGE_TAG ?= latest
IMAGE := $(if $(REGISTRY),$(REGISTRY)/,)$(IMAGE_NAME):$(IMAGE_TAG)

# Ports
HTTP_PORT ?= 7878
PG_PORT ?= 5433

# Build
.PHONY: build release test fmt clippy clean

build:
	cargo build

release:
	cargo build --release

test:
	cargo test --all-features

fmt:
	cargo fmt --all

clippy:
	cargo clippy --all-targets -- -D warnings

clean:
	cargo clean

# Run locally
.PHONY: run run-pg

run:
	CLARIUM_HTTP_PORT=$(HTTP_PORT) CLARIUM_PG_PORT=$(PG_PORT) CLARIUM_PGWIRE=true \
		cargo run --release

run-pg:
	CLARIUM_PGWIRE=true cargo run --release

# Docker
.PHONY: docker-build docker-run docker-push

docker-build:
	docker build -t $(IMAGE) .

docker-run:
	docker run --rm -it \
		-p $(HTTP_PORT):7878 -p $(PG_PORT):5433 \
		-v $${PWD}/dbs:/opt/clarium/dbs \
		-e CLARIUM_HTTP_PORT=7878 -e CLARIUM_PG_PORT=5433 -e CLARIUM_PGWIRE=true \
		$(IMAGE)

docker-push:
	docker push $(IMAGE)
