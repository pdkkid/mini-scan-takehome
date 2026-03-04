.PHONY: test lint build tidy up down

## test: run all tests with race detector enabled
test:
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
