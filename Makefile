.PHONY: test test-integration lint build tidy up down

## test: run all tests with race detector enabled (skips Postgres when POSTGRES_URL is unset)
test:
	go test -race ./...

## test-integration: run all tests including Postgres (requires a running Postgres instance)
test-integration:
	POSTGRES_URL="postgres://processor:processor@localhost:5432/scans?sslmode=disable" \
	  go test -race ./...

## lint: run golangci-lint (must be installed: https://golangci-lint.run/usage/install/)
lint:
	golangci-lint run ./...

## build: compile both service binaries
build:
	CGO_ENABLED=0 go build ./cmd/processor
	CGO_ENABLED=0 go build ./cmd/scanner

## tidy: tidy and verify go.mod / go.sum
tidy:
	go mod tidy
	go mod verify

## up: start the full stack in Kubernetes via Tilt (requires Docker Desktop K8s + Tilt)
up:
	tilt up

## down: tear down all Kubernetes resources deployed by Tilt
down:
	tilt down
