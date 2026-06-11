BINARY_DIR := bin
MODULE     := email-service
GO         := go

.PHONY: all build build-api build-smtp build-worker run-api run-smtp run-worker \
        test lint fmt vet tidy migrate-up migrate-down docker-up docker-down clean

all: build

## ── Build ────────────────────────────────────────────────────────────────────
build: build-api build-smtp build-worker

build-api:
	$(GO) build -o $(BINARY_DIR)/api ./cmd/api

build-smtp:
	$(GO) build -o $(BINARY_DIR)/smtp ./cmd/smtp

build-worker:
	$(GO) build -o $(BINARY_DIR)/worker ./cmd/worker

## ── Run (local, needs .env present) ─────────────────────────────────────────
run-api:
	$(GO) run ./cmd/api

run-smtp:
	$(GO) run ./cmd/smtp

run-worker:
	$(GO) run ./cmd/worker

## ── Code quality ─────────────────────────────────────────────────────────────
test:
	$(GO) test ./... -race -count=1

lint:
	golangci-lint run ./...

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

## ── Database migrations (uses golang-migrate CLI) ────────────────────────────
DB_DSN ?= "mysql://$(MYSQL_USER):$(MYSQL_PASSWORD)@tcp($(MYSQL_HOST):$(MYSQL_PORT))/$(MYSQL_DATABASE)"

migrate-up:
	migrate -path ./migrations -database $(DB_DSN) up

migrate-down:
	migrate -path ./migrations -database $(DB_DSN) down

## ── Docker ───────────────────────────────────────────────────────────────────
docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

## ── Clean ────────────────────────────────────────────────────────────────────
clean:
	rm -rf $(BINARY_DIR)
